from alertmesh.events import (
    BATTERY_LOW,
    HEARTBEAT_LOSS,
    HELP_BUTTON,
    PULL_CORD,
    Alert,
)
from alertmesh.triage import ACKED, NEW, RESOLVED, TriageQueue


def mk(alert_id, kind=HELP_BUTTON, resident="res-1", t=0.0):
    return Alert(alert_id, "dev-1", resident, kind, t)


def test_ingest_is_idempotent():
    q = TriageQueue()
    a = mk("a:1")
    assert q.ingest(a, 1.0) is True
    assert q.ingest(a, 2.0) is False
    assert q.dup_attempts == 1
    assert len(q.delivered_ids) == 1


def test_priority_ordering_help_beats_pull_cord_beats_battery():
    q = TriageQueue()
    q.ingest(mk("b:1", BATTERY_LOW, "res-1"), 1.0)
    q.ingest(mk("p:1", PULL_CORD, "res-2"), 2.0)
    q.ingest(mk("h:1", HELP_BUTTON, "res-3"), 3.0)
    kinds = [q.take_next().kind for _ in range(3)]
    assert kinds == [HELP_BUTTON, PULL_CORD, BATTERY_LOW]


def test_heartbeat_loss_shares_battery_tier_fifo():
    q = TriageQueue()
    q.ingest(mk("hb:1", HEARTBEAT_LOSS, "res-1"), 1.0)
    q.ingest(mk("b:1", BATTERY_LOW, "res-2"), 2.0)
    kinds = [q.take_next().kind for _ in range(2)]
    assert kinds == [HEARTBEAT_LOSS, BATTERY_LOW]


def test_fifo_within_same_priority():
    q = TriageQueue()
    q.ingest(mk("h:1", HELP_BUTTON, "res-1"), 5.0)
    q.ingest(mk("h:2", HELP_BUTTON, "res-2"), 1.0)
    first = q.take_next()
    assert first.resident_id == "res-2"


def test_same_resident_same_kind_coalesces():
    q = TriageQueue()
    q.ingest(mk("h:1", HELP_BUTTON, "res-1"), 1.0)
    q.ingest(mk("h:2", HELP_BUTTON, "res-1"), 2.0)
    q.ingest(mk("h:3", HELP_BUTTON, "res-1"), 3.0)
    assert len(q.entries) == 1
    entry = q.peek_next()
    assert entry.count == 3
    assert entry.alert_ids == ["h:1", "h:2", "h:3"]
    assert entry.first_at == 1.0
    assert entry.last_at == 3.0


def test_different_residents_do_not_coalesce():
    q = TriageQueue()
    q.ingest(mk("h:1", HELP_BUTTON, "res-1"), 1.0)
    q.ingest(mk("h:2", HELP_BUTTON, "res-2"), 2.0)
    assert len(q.entries) == 2


def test_different_kinds_do_not_coalesce():
    q = TriageQueue()
    q.ingest(mk("h:1", HELP_BUTTON, "res-1"), 1.0)
    q.ingest(mk("p:1", PULL_CORD, "res-1"), 2.0)
    assert len(q.entries) == 2


def test_resolved_entry_stops_coalescing_new_alert_opens_fresh_entry():
    q = TriageQueue()
    q.ingest(mk("h:1", HELP_BUTTON, "res-1"), 1.0)
    entry = q.take_next(now=2.0)
    q.resolve(entry.entry_id, now=3.0)
    q.ingest(mk("h:2", HELP_BUTTON, "res-1"), 4.0)
    assert len(q.entries) == 2
    fresh = q.peek_next()
    assert fresh.status == NEW
    assert fresh.alert_ids == ["h:2"]


def test_acked_entry_still_coalesces_until_resolved():
    q = TriageQueue()
    q.ingest(mk("h:1", HELP_BUTTON, "res-1"), 1.0)
    entry = q.take_next(now=2.0)
    assert entry.status == ACKED
    q.ingest(mk("h:2", HELP_BUTTON, "res-1"), 3.0)
    assert len(q.entries) == 1
    assert q.entries[entry.entry_id].count == 2


def test_ack_resolve_lifecycle_timestamps():
    q = TriageQueue()
    q.ingest(mk("h:1"), 1.0)
    entry = q.peek_next()
    q.ack(entry.entry_id, now=2.0)
    assert entry.status == ACKED
    assert entry.acked_at == 2.0
    q.resolve(entry.entry_id, now=3.0)
    assert entry.status == RESOLVED
    assert entry.resolved_at == 3.0


def test_take_next_skips_acked_and_resolved():
    q = TriageQueue()
    q.ingest(mk("h:1", HELP_BUTTON, "res-1"), 1.0)
    q.ingest(mk("h:2", HELP_BUTTON, "res-2"), 2.0)
    e1 = q.take_next()
    assert e1.resident_id == "res-1"
    e2 = q.take_next()
    assert e2.resident_id == "res-2"
    assert q.take_next() is None


def test_latencies_recorded_per_delivery():
    q = TriageQueue()
    q.ingest(mk("h:1", HELP_BUTTON, "res-1", t=1.0), 1.5)
    q.ingest(mk("h:1", HELP_BUTTON, "res-1", t=1.0), 9.9)  # dup, no latency row
    assert q.latencies == [(HELP_BUTTON, 1.0, 1.5)]


def test_open_count_tracks_new_entries_only():
    q = TriageQueue()
    q.ingest(mk("h:1", HELP_BUTTON, "res-1"), 1.0)
    q.ingest(mk("p:1", PULL_CORD, "res-2"), 2.0)
    assert q.open_count() == 2
    q.take_next()
    assert q.open_count() == 1
