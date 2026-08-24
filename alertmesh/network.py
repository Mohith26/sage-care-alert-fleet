"""Lossy in-process link between devices and the gateway.

Models the failure modes I care about: random drops, duplicated frames,
reordering (via randomized extra latency), latency jitter, and hard
partitions (windows where nothing gets through in either direction).
"""

import random


class LossyLink:
    def __init__(
        self,
        sched,
        rng=None,
        drop_pct=0.0,
        dup_pct=0.0,
        reorder_pct=0.0,
        base_latency=0.05,
        jitter=0.02,
        reorder_extra=1.0,
        partitions=None,
    ):
        self.sched = sched
        self.rng = rng if rng is not None else random.Random(0)
        self.drop_pct = drop_pct
        self.dup_pct = dup_pct
        self.reorder_pct = reorder_pct
        self.base_latency = base_latency
        self.jitter = jitter
        self.reorder_extra = reorder_extra
        # partitions: list of (start_time, end_time) windows where the link is dead
        self.partitions = list(partitions or [])
        self.sent = 0
        self.dropped = 0
        self.duplicated = 0

    def in_partition(self, t):
        for start, end in self.partitions:
            if start <= t < end:
                return True
        return False

    def send(self, payload, deliver):
        """Attempt to move payload across the link, calling deliver(payload)."""
        self.sent += 1
        now = self.sched.now
        if self.in_partition(now):
            self.dropped += 1
            return
        if self.rng.random() < self.drop_pct:
            self.dropped += 1
            return
        copies = 1
        if self.rng.random() < self.dup_pct:
            copies = 2
            self.duplicated += 1
        for _ in range(copies):
            delay = self.base_latency + self.rng.random() * self.jitter
            if self.rng.random() < self.reorder_pct:
                delay += self.rng.random() * self.reorder_extra
            self.sched.after(delay, self._arrival(payload, deliver))

    def _arrival(self, payload, deliver):
        def cb():
            # A frame already in flight when a partition slams shut gets lost too.
            if self.in_partition(self.sched.now):
                self.dropped += 1
                return
            deliver(payload)
        return cb
