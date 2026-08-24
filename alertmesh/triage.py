"""Caregiver triage queue.

Responsibilities:
- final idempotency barrier: an alert_id is only ever ingested once, so
  journal replay after a gateway crash cannot double-page a caregiver
- priority ordering: help button > pull cord > battery/heartbeat, FIFO
  within a priority tier
- per-resident coalescing: repeat alerts of the same kind for the same
  resident while an entry is still open fold into one entry with a count,
  instead of stacking the queue
- ack/resolve lifecycle per entry
"""

import heapq

from .events import PRIORITY

NEW = "new"
ACKED = "acked"
RESOLVED = "resolved"


class TriageEntry:
    __slots__ = (
        "entry_id",
        "resident_id",
        "kind",
        "priority",
        "first_at",
        "last_at",
        "count",
        "alert_ids",
        "status",
        "acked_at",
        "resolved_at",
    )

    def __init__(self, entry_id, resident_id, kind, now, alert_id):
        self.entry_id = entry_id
        self.resident_id = resident_id
        self.kind = kind
        self.priority = PRIORITY[kind]
        self.first_at = now
        self.last_at = now
        self.count = 1
        self.alert_ids = [alert_id]
        self.status = NEW
        self.acked_at = None
        self.resolved_at = None


class TriageQueue:
    def __init__(self, coalesce=True):
        self.coalesce = coalesce
        self.delivered_ids = set()
        self.dup_attempts = 0
        self.entries = {}
        self._open_by_key = {}
        self._heap = []
        self._next_entry_id = 0
        self.latencies = []  # (kind, emitted_at, delivered_at)

    # ---- ingestion ------------------------------------------------------
    def ingest(self, alert, now):
        """Idempotent ingest. Returns True if this alert_id was new."""
        if alert.alert_id in self.delivered_ids:
            self.dup_attempts += 1
            return False
        self.delivered_ids.add(alert.alert_id)
        self.latencies.append((alert.kind, alert.emitted_at, now))
        key = (alert.resident_id, alert.kind)
        if self.coalesce:
            open_id = self._open_by_key.get(key)
            if open_id is not None:
                entry = self.entries[open_id]
                if entry.status != RESOLVED:
                    entry.count += 1
                    entry.last_at = now
                    entry.alert_ids.append(alert.alert_id)
                    return True
        entry_id = self._next_entry_id
        self._next_entry_id += 1
        entry = TriageEntry(entry_id, alert.resident_id, alert.kind, now, alert.alert_id)
        self.entries[entry_id] = entry
        self._open_by_key[key] = entry_id
        heapq.heappush(self._heap, (entry.priority, entry.first_at, entry_id))
        return True

    # ---- caregiver workflow --------------------------------------------
    def peek_next(self):
        """Highest-priority NEW entry without removing it."""
        while self._heap:
            _, _, entry_id = self._heap[0]
            entry = self.entries[entry_id]
            if entry.status == NEW:
                return entry
            heapq.heappop(self._heap)
        return None

    def take_next(self, now=None):
        """Pop the highest-priority NEW entry and mark it acked."""
        entry = self.peek_next()
        if entry is None:
            return None
        heapq.heappop(self._heap)
        entry.status = ACKED
        entry.acked_at = now
        return entry

    def ack(self, entry_id, now=None):
        entry = self.entries[entry_id]
        if entry.status == NEW:
            entry.status = ACKED
            entry.acked_at = now
        return entry

    def resolve(self, entry_id, now=None):
        entry = self.entries[entry_id]
        entry.status = RESOLVED
        entry.resolved_at = now
        key = (entry.resident_id, entry.kind)
        if self._open_by_key.get(key) == entry.entry_id:
            del self._open_by_key[key]
        return entry

    # ---- stats ----------------------------------------------------------
    def open_count(self):
        return sum(1 for e in self.entries.values() if e.status == NEW)
