"""Resident alert device with a persistent store-and-forward outbox.

The device model splits state into two buckets:

- nvram: survives restarts. Holds the monotonic sequence counter and the
  outbox of alerts that have not been acked yet. A button press writes to
  the outbox before anything hits the radio, so a restart can never lose
  an emitted alert.
- volatile: retry timers and the epoch counter. A restart bumps the epoch
  so timers scheduled before the restart become no-ops, then resends
  everything left in the outbox.

Transport is at-least-once: resend on a timer until the gateway acks.
"""

from .events import Alert


class Device:
    def __init__(
        self,
        sched,
        device_id,
        resident_id,
        uplink,
        gateway,
        retry_timeout=4.0,
        hb_interval=30.0,
        hb_stop_after=None,
        truth_log=None,
    ):
        self.sched = sched
        self.device_id = device_id
        self.resident_id = resident_id
        self.uplink = uplink
        self.gateway = gateway
        self.retry_timeout = retry_timeout
        self.hb_interval = hb_interval
        self.hb_stop_after = hb_stop_after
        self.truth_log = truth_log if truth_log is not None else []
        # persistent state (simulated flash/NVRAM)
        self.nvram = {"next_seq": 0, "outbox": {}}
        # volatile state
        self.epoch = 0
        self.offline = False
        self.offline_at = None
        self.restarts = 0
        self.sends = 0

    # ---- alert emission -------------------------------------------------
    def emit(self, kind):
        if self.offline:
            return None
        seq = self.nvram["next_seq"]
        self.nvram["next_seq"] = seq + 1
        alert = Alert(
            alert_id="%s:%d" % (self.device_id, seq),
            device_id=self.device_id,
            resident_id=self.resident_id,
            kind=kind,
            emitted_at=self.sched.now,
        )
        # Persist before transmit. This ordering is the whole guarantee.
        self.nvram["outbox"][seq] = alert
        self.truth_log.append(alert)
        self._send(seq)
        return alert

    def _send(self, seq):
        alert = self.nvram["outbox"].get(seq)
        if alert is None or self.offline:
            return
        self.sends += 1
        self.uplink.send({"type": "alert", "alert": alert}, self.gateway.on_message)
        epoch = self.epoch
        self.sched.after(self.retry_timeout, lambda: self._retry(seq, epoch))

    def _retry(self, seq, epoch):
        if epoch != self.epoch or self.offline:
            return
        if seq in self.nvram["outbox"]:
            self._send(seq)

    def on_ack(self, payload):
        if self.offline:
            return
        alert_id = payload["alert_id"]
        try:
            seq = int(alert_id.rsplit(":", 1)[1])
        except (IndexError, ValueError):
            return
        self.nvram["outbox"].pop(seq, None)

    # ---- lifecycle ------------------------------------------------------
    def restart(self):
        """Power-cycle: volatile state gone, nvram kept, unacked alerts resent."""
        if self.offline:
            return
        self.restarts += 1
        self.epoch += 1
        for seq in sorted(self.nvram["outbox"].keys()):
            self._send(seq)

    def go_offline(self):
        """Device dies (battery pull, hardware failure). Stops all traffic."""
        self.offline = True
        self.offline_at = self.sched.now
        self.epoch += 1

    # ---- heartbeats -----------------------------------------------------
    def start_heartbeats(self, phase=0.0):
        self.sched.after(phase, self._heartbeat)

    def _heartbeat(self):
        if self.offline:
            return
        if self.hb_stop_after is not None and self.sched.now > self.hb_stop_after:
            return
        self.uplink.send(
            {"type": "heartbeat", "device_id": self.device_id, "at": self.sched.now},
            self.gateway.on_message,
        )
        self.sched.after(self.hb_interval, self._heartbeat)
