"""Deterministic discrete-event scheduler.

All simulated components share one Scheduler. Callbacks fire in strict
(time, insertion order) order, so a given seed always produces the exact
same interleaving.
"""

import heapq


class Scheduler:
    def __init__(self):
        self.now = 0.0
        self._heap = []
        self._counter = 0
        self.events_processed = 0

    def at(self, when, fn):
        if when < self.now:
            when = self.now
        heapq.heappush(self._heap, (when, self._counter, fn))
        self._counter += 1

    def after(self, delay, fn):
        self.at(self.now + delay, fn)

    def pending(self):
        return len(self._heap)

    def run(self, until=None):
        while self._heap:
            when, _, fn = self._heap[0]
            if until is not None and when > until:
                break
            heapq.heappop(self._heap)
            self.now = when
            fn()
            self.events_processed += 1
        if until is not None and self.now < until:
            self.now = until
