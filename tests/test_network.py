import random

from alertmesh.network import LossyLink
from alertmesh.simclock import Scheduler


def deliver_all(link, n):
    got = []
    for i in range(n):
        link.send(i, got.append)
    link.sched.run()
    return got


def test_lossless_link_delivers_everything_once():
    s = Scheduler()
    link = LossyLink(s, rng=random.Random(1))
    got = deliver_all(link, 500)
    assert sorted(got) == list(range(500))


def test_drop_rate_is_roughly_configured():
    s = Scheduler()
    link = LossyLink(s, rng=random.Random(2), drop_pct=0.3)
    got = deliver_all(link, 5000)
    rate = 1 - len(got) / 5000.0
    assert 0.25 < rate < 0.35


def test_duplicates_happen_at_configured_rate():
    s = Scheduler()
    link = LossyLink(s, rng=random.Random(3), dup_pct=0.2)
    got = deliver_all(link, 5000)
    extra = len(got) - 5000
    assert 800 < extra < 1200


def test_partition_blocks_sends():
    s = Scheduler()
    link = LossyLink(s, rng=random.Random(4), partitions=[(0.0, 10.0)])
    got = []
    link.send("x", got.append)
    s.run()
    assert got == []
    assert link.dropped == 1


def test_frame_in_flight_when_partition_starts_is_lost():
    s = Scheduler()
    link = LossyLink(s, rng=random.Random(5), base_latency=1.0, jitter=0.0,
                     partitions=[(0.5, 5.0)])
    got = []
    link.send("x", got.append)  # sent at t=0, would arrive at t=1.0 inside partition
    s.run()
    assert got == []


def test_delivery_after_partition_ends():
    s = Scheduler()
    link = LossyLink(s, rng=random.Random(6), partitions=[(0.0, 10.0)])
    got = []
    s.at(11.0, lambda: link.send("y", got.append))
    s.run()
    assert got == ["y"]


def test_reorder_can_flip_arrival_order():
    s = Scheduler()
    link = LossyLink(s, rng=random.Random(7), reorder_pct=0.5,
                     base_latency=0.01, jitter=0.0, reorder_extra=5.0)
    got = []
    for i in range(50):
        link.send(i, got.append)
    s.run()
    assert sorted(got) == list(range(50))
    assert got != sorted(got)


def test_same_seed_same_outcome():
    def run(seed):
        s = Scheduler()
        link = LossyLink(s, rng=random.Random(seed), drop_pct=0.2, dup_pct=0.1,
                         reorder_pct=0.2)
        got = []
        for i in range(200):
            link.send(i, got.append)
        s.run()
        return got

    assert run(42) == run(42)
    assert run(42) != run(43)
