"""Building gateway: receives device traffic, journals, dedupes, delivers.

Accept path for an alert, in order:
  1. dedupe check against the in-memory seen set
  2. append to the write-ahead journal (flush)
  3. deliver to the triage queue
  4. ack the device

If the gateway crashes anywhere in that window, the device keeps
retrying (it never got the ack), and recovery replays the journal to
rebuild the seen set and re-offer entries to triage. The triage queue's
own idempotency barrier turns the replay into a no-op for anything that
already landed. Net effect: exactly once on the triage queue.

The gateway also owns heartbeat monitoring. It tracks last-heard-from
times per registered device and raises a heartbeat_loss alert (with its
own idempotency key) when a device goes quiet past the timeout. After a
gateway restart every registered device gets a fresh grace window so a
crash does not spray false offline alerts.
"""

import time

from .events import Alert, HEARTBEAT_LOSS
from .journal import Journal


class Gateway:
    def __init__(
        self,
        sched,
        journal_path,
        triage,
        downlink,
        devices=None,
        hb_timeout=90.0,
        hb_check_interval=5.0,
        hb_monitor_stop_after=None,
        truth_log=None,
    ):
        self.sched = sched
        self.journal = Journal(journal_path)
        self.triage = triage
        self.downlink = downlink
        self.hb_timeout = hb_timeout
        self.hb_check_interval = hb_check_interval
        self.hb_monitor_stop_after = hb_monitor_stop_after
        self.truth_log = truth_log if truth_log is not None else []
        self.down = False
        self.seen = set()
        self.registry = {}  # device_id -> Device (for ack routing + monitoring)
        self.last_hb = {}
        self.hb_flagged = set()
        self.hb_detections = []  # (device_id, flagged_at)
        self.epoch = 0
        # fault injection: crash immediately after the next journal append,
        # before triage delivery and before the ack goes out
        self.crash_after_next_journal_append = False
        # stats
        self.alerts_received = 0
        self.dupes_dropped = 0
        self.heartbeats_received = 0
        self.crashes = 0
        self.last_replay_seconds = None
        self.last_replay_entries = 0
        self.last_replay_skipped = 0

    def register(self, device):
        self.registry[device.device_id] = device
        self.last_hb[device.device_id] = self.sched.now

    def start_monitor(self):
        epoch = self.epoch
        self.sched.after(self.hb_check_interval, lambda: self._monitor_tick(epoch))

    # ---- receive path ---------------------------------------------------
    def on_message(self, payload):
        if self.down:
            return
        kind = payload.get("type")
        if kind == "heartbeat":
            self.heartbeats_received += 1
            device_id = payload["device_id"]
            self.last_hb[device_id] = self.sched.now
            if device_id in self.hb_flagged:
                self.hb_flagged.discard(device_id)
            return
        if kind != "alert":
            return
        alert = payload["alert"]
        self.alerts_received += 1
        if alert.alert_id in self.seen:
            self.dupes_dropped += 1
            self._ack(alert)
            return
        self.journal.append(alert.to_record())
        self.seen.add(alert.alert_id)
        if self.crash_after_next_journal_append:
            self.crash_after_next_journal_append = False
            self.crash()
            return
        self.triage.ingest(alert, self.sched.now)
        self._ack(alert)

    def _ack(self, alert):
        device = self.registry.get(alert.device_id)
        if device is None:
            return
        self.downlink.send({"type": "ack", "alert_id": alert.alert_id}, device.on_ack)

    # ---- heartbeat monitor ----------------------------------------------
    def _monitor_tick(self, epoch):
        if self.down or epoch != self.epoch:
            return
        if (
            self.hb_monitor_stop_after is not None
            and self.sched.now > self.hb_monitor_stop_after
        ):
            return
        now = self.sched.now
        for device_id, last in self.last_hb.items():
            if device_id in self.hb_flagged:
                continue
            if now - last > self.hb_timeout:
                self.hb_flagged.add(device_id)
                self.hb_detections.append((device_id, now))
                self._raise_hb_loss(device_id, last)
        self.sched.after(self.hb_check_interval, lambda: self._monitor_tick(epoch))

    def _raise_hb_loss(self, device_id, last_seen):
        device = self.registry.get(device_id)
        resident = device.resident_id if device is not None else "unknown"
        alert = Alert(
            alert_id="hbloss:%s:%d" % (device_id, int(last_seen * 1000)),
            device_id=device_id,
            resident_id=resident,
            kind=HEARTBEAT_LOSS,
            emitted_at=self.sched.now,
        )
        if alert.alert_id in self.seen:
            return
        self.truth_log.append(alert)
        self.journal.append(alert.to_record())
        self.seen.add(alert.alert_id)
        self.triage.ingest(alert, self.sched.now)

    # ---- crash / recovery -----------------------------------------------
    def crash(self):
        """Process death: all volatile state gone, journal file stays on disk."""
        self.down = True
        self.crashes += 1
        self.epoch += 1
        self.seen = set()
        self.last_hb = {}
        self.hb_flagged = set()
        self.journal.close()

    def restart(self):
        """Replay the journal, rebuild dedupe state, re-offer entries to triage."""
        t0 = time.perf_counter()
        records, skipped = self.journal.replay()
        for rec in records:
            alert = Alert.from_record(rec)
            self.seen.add(alert.alert_id)
            # Triage dedupes, so re-offering is safe and covers the case
            # where we crashed after journaling but before delivery.
            self.triage.ingest(alert, self.sched.now)
        self.last_replay_seconds = time.perf_counter() - t0
        self.last_replay_entries = len(records)
        self.last_replay_skipped = skipped
        self.journal.reopen()
        # Fresh grace window for every registered device.
        for device_id in self.registry:
            self.last_hb[device_id] = self.sched.now
        self.down = False
        self.start_monitor()
