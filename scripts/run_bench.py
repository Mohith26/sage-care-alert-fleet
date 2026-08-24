"""Microbenchmarks: pipeline throughput and journal replay time.

- Pipeline throughput: feed pre-built alert frames straight into the
  gateway receive path (journal append + dedupe + triage ingest + ack)
  and measure alerts/sec end to end.
- Journal replay: write a journal with N entries, simulate a crash, and
  time the recovery replay (rebuild dedupe set + re-offer to triage).

Usage: .venv/bin/python scripts/run_bench.py
"""

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from alertmesh.events import Alert, HELP_BUTTON
from alertmesh.gateway import Gateway
from alertmesh.network import LossyLink
from alertmesh.simclock import Scheduler
from alertmesh.triage import TriageQueue

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOURNAL_DIR = os.path.join(ROOT, "tmpwal")
RESULTS = os.path.join(ROOT, "results", "bench.json")

N = 100000
REPEATS = 3


def build_gateway(name):
    sched = Scheduler()
    triage = TriageQueue()
    downlink = LossyLink(sched)  # lossless defaults, acks go nowhere (no devices)
    path = os.path.join(JOURNAL_DIR, "%s.wal" % name)
    if os.path.exists(path):
        os.remove(path)
    gw = Gateway(sched, path, triage, downlink)
    return sched, triage, gw


def bench_throughput():
    rates = []
    for rep in range(REPEATS):
        _, triage, gw = build_gateway("bench-thru-%d" % rep)
        frames = [
            {
                "type": "alert",
                "alert": Alert(
                    alert_id="b:%d" % i,
                    device_id="dev-b",
                    resident_id="res-b",
                    kind=HELP_BUTTON,
                    emitted_at=0.0,
                ),
            }
            for i in range(N)
        ]
        t0 = time.perf_counter()
        for f in frames:
            gw.on_message(f)
        dt = time.perf_counter() - t0
        assert len(triage.delivered_ids) == N
        gw.journal.close()
        rates.append(N / dt)
    return rates


def bench_replay():
    times = []
    for rep in range(REPEATS):
        _, triage, gw = build_gateway("bench-replay-%d" % rep)
        for i in range(N):
            gw.on_message(
                {
                    "type": "alert",
                    "alert": Alert(
                        alert_id="r:%d" % i,
                        device_id="dev-r",
                        resident_id="res-r",
                        kind=HELP_BUTTON,
                        emitted_at=0.0,
                    ),
                }
            )
        gw.crash()
        t0 = time.perf_counter()
        gw.restart()
        dt = time.perf_counter() - t0
        assert gw.last_replay_entries == N
        assert len(gw.seen) == N
        gw.journal.close()
        times.append(dt)
    return times


def main():
    os.makedirs(os.path.dirname(RESULTS), exist_ok=True)
    thru = bench_throughput()
    replay = bench_replay()
    out = {
        "pipeline_alerts": N,
        "repeats": REPEATS,
        "throughput_alerts_per_sec": [round(r, 1) for r in thru],
        "throughput_best": round(max(thru), 1),
        "throughput_median": round(sorted(thru)[len(thru) // 2], 1),
        "replay_entries": N,
        "replay_seconds": [round(t, 4) for t in replay],
        "replay_best_seconds": round(min(replay), 4),
        "replay_median_seconds": round(sorted(replay)[len(replay) // 2], 4),
        "replay_entries_per_sec_median": round(N / sorted(replay)[len(replay) // 2], 1),
        "note": "single thread, journal flushed per append without fsync",
    }
    with open(RESULTS, "w") as fh:
        json.dump(out, fh, indent=2)
    print(json.dumps(out, indent=2))
    print("wrote", RESULTS)


if __name__ == "__main__":
    main()
