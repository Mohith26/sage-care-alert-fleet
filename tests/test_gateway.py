import random

from alertmesh.device import Device
from alertmesh.events import HEARTBEAT_LOSS, HELP_BUTTON, Alert
from alertmesh.gateway import Gateway
from alertmesh.network import LossyLink
from alertmesh.simclock import Scheduler
from alertmesh.triage import TriageQueue


def build(tmp_path, name="gw.wal", **gw_kwargs):
    sched = Scheduler()
    triage = TriageQueue()
    downlink = LossyLink(sched, rng=random.Random(21))
    gw = Gateway(sched, str(tmp_path / name), triage, downlink, **gw_kwargs)
    return sched, triage, gw


def frame(alert_id, kind=HELP_BUTTON, device="dev-1", resident="res-1", t=0.0):
    return {"type": "alert",
            "alert": Alert(alert_id, device, resident, kind, t)}


def test_new_alert_is_journaled_and_delivered(tmp_path):
    _, triage, gw = build(tmp_path)
    gw.on_message(frame("a:1"))
    assert "a:1" in triage.delivered_ids
    assert gw.journal.appends == 1


def test_duplicate_frame_is_dropped_but_acked(tmp_path):
    sched, triage, gw = build(tmp_path)
    uplink = LossyLink(sched, rng=random.Random(22))
    dev = Device(sched, "dev-1", "res-1", uplink, gw)
    gw.register(dev)
    a = dev.emit(HELP_BUTTON)
    sched.run(until=0.5)
    # replay the same frame as if the link duplicated it
    gw.on_message({"type": "alert", "alert": a})
    sched.run()
    assert gw.dupes_dropped >= 1
    assert len(triage.delivered_ids) == 1
    assert dev.nvram["outbox"] == {}  # ack still arrived


def test_messages_while_down_are_ignored(tmp_path):
    _, triage, gw = build(tmp_path)
    gw.crash()
    gw.on_message(frame("a:1"))
    assert triage.delivered_ids == set()


def test_crash_wipes_volatile_state_restart_rebuilds_from_journal(tmp_path):
    sched, triage, gw = build(tmp_path)
    for i in range(5):
        gw.on_message(frame("a:%d" % i))
    gw.crash()
    assert gw.seen == set()
    gw.restart()
    assert gw.seen == {"a:%d" % i for i in range(5)}
    assert gw.last_replay_entries == 5
    # replay re-offered everything but triage deduped it all
    assert len(triage.delivered_ids) == 5
    assert triage.dup_attempts == 5


def test_crash_between_journal_and_delivery_recovers_without_loss(tmp_path):
    sched, triage, gw = build(tmp_path)
    gw.crash_after_next_journal_append = True
    gw.on_message(frame("a:1"))
    # crashed after the WAL write: triage never saw it, device never acked
    assert "a:1" not in triage.delivered_ids
    assert gw.down
    gw.restart()
    # recovery replay delivers it exactly once
    assert "a:1" in triage.delivered_ids
    assert len(triage.latencies) == 1


def test_retry_after_recovery_does_not_double_deliver(tmp_path):
    sched, triage, gw = build(tmp_path)
    gw.crash_after_next_journal_append = True
    f = frame("a:1")
    gw.on_message(f)
    gw.restart()
    gw.on_message(f)  # the device retries because it never got an ack
    assert len(triage.delivered_ids) == 1
    assert gw.dupes_dropped == 1


def test_heartbeat_loss_detected_after_timeout(tmp_path):
    sched, triage, gw = build(tmp_path, hb_timeout=90.0, hb_check_interval=5.0)
    uplink = LossyLink(sched, rng=random.Random(23))
    dev = Device(sched, "dev-1", "res-9", uplink, gw, hb_interval=30.0)
    gw.register(dev)
    dev.start_heartbeats(phase=0.0)
    gw.start_monitor()
    sched.at(100.0, dev.go_offline)
    sched.run(until=400.0)
    assert len(gw.hb_detections) == 1
    device_id, flagged_at = gw.hb_detections[0]
    assert device_id == "dev-1"
    assert 100.0 < flagged_at < 250.0
    hb_alerts = [a for a in triage.latencies if a[0] == HEARTBEAT_LOSS]
    assert len(hb_alerts) == 1


def test_healthy_device_never_flagged(tmp_path):
    sched, triage, gw = build(tmp_path, hb_timeout=90.0, hb_check_interval=5.0)
    uplink = LossyLink(sched, rng=random.Random(24))
    dev = Device(sched, "dev-1", "res-9", uplink, gw, hb_interval=30.0,
                 hb_stop_after=500.0)
    gw.register(dev)
    dev.start_heartbeats(phase=0.0)
    gw.start_monitor()
    sched.run(until=500.0)
    assert gw.hb_detections == []


def test_restart_grants_grace_window_no_false_offline_spray(tmp_path):
    sched, triage, gw = build(tmp_path, hb_timeout=90.0, hb_check_interval=5.0)
    uplink = LossyLink(sched, rng=random.Random(25))
    dev = Device(sched, "dev-1", "res-9", uplink, gw, hb_interval=30.0)
    gw.register(dev)
    dev.start_heartbeats(phase=0.0)
    gw.start_monitor()
    sched.at(50.0, gw.crash)
    sched.at(60.0, gw.restart)
    sched.run(until=140.0)  # only 80s after restart, inside the grace window
    assert gw.hb_detections == []


def test_hb_loss_alert_is_idempotent_across_restart(tmp_path):
    sched, triage, gw = build(tmp_path, hb_timeout=90.0, hb_check_interval=5.0)
    uplink = LossyLink(sched, rng=random.Random(26))
    dev = Device(sched, "dev-1", "res-9", uplink, gw, hb_interval=30.0)
    gw.register(dev)
    dev.start_heartbeats(phase=0.0)
    gw.start_monitor()
    sched.at(100.0, dev.go_offline)
    sched.run(until=400.0)
    gw.crash()
    gw.restart()
    sched.run(until=800.0)
    hb_ids = [i for i in triage.delivered_ids if i.startswith("hbloss:")]
    # the same offline episode may be re-flagged after restart with a new
    # last-seen baseline, but replay never duplicates an existing alert_id
    assert len(hb_ids) == len(set(hb_ids))
    assert triage.dup_attempts >= 1  # replay re-offer was deduped


def test_replay_skips_torn_tail(tmp_path):
    sched, triage, gw = build(tmp_path)
    gw.on_message(frame("a:1"))
    gw.crash()
    with open(gw.journal.path, "a") as fh:
        fh.write('{"broken')
    gw.restart()
    assert gw.last_replay_entries == 1
    assert gw.last_replay_skipped == 1
    assert gw.seen == {"a:1"}
