import random

from alertmesh.device import Device
from alertmesh.events import HELP_BUTTON, PULL_CORD
from alertmesh.network import LossyLink
from alertmesh.simclock import Scheduler


class FakeGateway:
    def __init__(self):
        self.messages = []
        self.acks_enabled = False
        self.device = None
        self.sched = None
        self.link = None

    def on_message(self, payload):
        self.messages.append(payload)
        if self.acks_enabled and payload.get("type") == "alert":
            alert = payload["alert"]
            self.link.send({"type": "ack", "alert_id": alert.alert_id},
                           self.device.on_ack)


def make_device(drop=0.0, retry=2.0):
    sched = Scheduler()
    gw = FakeGateway()
    uplink = LossyLink(sched, rng=random.Random(11), drop_pct=drop)
    d = Device(sched, "dev-0001", "res-0001", uplink, gw, retry_timeout=retry)
    gw.device = d
    gw.sched = sched
    gw.link = LossyLink(sched, rng=random.Random(12))
    return sched, gw, d


def test_sequence_numbers_are_monotonic():
    _, _, d = make_device()
    a1 = d.emit(HELP_BUTTON)
    a2 = d.emit(PULL_CORD)
    assert a1.alert_id == "dev-0001:0"
    assert a2.alert_id == "dev-0001:1"


def test_emit_persists_to_outbox_before_send():
    _, _, d = make_device()
    d.emit(HELP_BUTTON)
    assert 0 in d.nvram["outbox"]


def test_retries_until_acked():
    sched, gw, d = make_device(drop=0.0, retry=2.0)
    d.emit(HELP_BUTTON)
    sched.run(until=10.0)
    # no acks configured: initial send plus retries every 2s
    alert_frames = [m for m in gw.messages if m["type"] == "alert"]
    assert len(alert_frames) >= 4
    assert 0 in d.nvram["outbox"]


def test_ack_stops_retries_and_clears_outbox():
    sched, gw, d = make_device()
    gw.acks_enabled = True
    d.emit(HELP_BUTTON)
    sched.run()
    assert d.nvram["outbox"] == {}
    alert_frames = [m for m in gw.messages if m["type"] == "alert"]
    assert len(alert_frames) == 1


def test_restart_preserves_outbox_and_resends():
    sched, gw, d = make_device(drop=1.0)  # nothing gets through
    d.emit(HELP_BUTTON)
    sched.run(until=1.0)
    d.uplink.drop_pct = 0.0
    d.restart()
    gw.acks_enabled = True
    sched.run()
    assert d.nvram["outbox"] == {}
    assert d.restarts == 1


def test_restart_invalidates_stale_retry_timers():
    sched, gw, d = make_device(retry=2.0)
    gw.acks_enabled = True
    d.emit(HELP_BUTTON)
    epoch_before = d.epoch
    d.restart()
    assert d.epoch == epoch_before + 1
    sched.run()
    # stale timers no-op, fresh send gets acked, outbox drains
    assert d.nvram["outbox"] == {}


def test_sequence_counter_survives_restart():
    _, _, d = make_device()
    d.emit(HELP_BUTTON)
    d.restart()
    a = d.emit(PULL_CORD)
    assert a.alert_id == "dev-0001:1"


def test_offline_device_stops_everything():
    sched, gw, d = make_device()
    d.start_heartbeats(phase=0.0)
    sched.run(until=0.5)
    d.go_offline()
    n_before = len(gw.messages)
    sched.run(until=200.0)
    assert len(gw.messages) == n_before
    assert d.emit(HELP_BUTTON) is None


def test_truth_log_records_every_emit():
    _, _, d = make_device()
    d.emit(HELP_BUTTON)
    d.emit(PULL_CORD)
    assert [a.alert_id for a in d.truth_log] == ["dev-0001:0", "dev-0001:1"]
