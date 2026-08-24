"""Chaos evaluation: >=200k alerts across seeded scenarios.

Runs every scenario, verifies the exactly-once contract on each, and
writes results/eval.json with per-scenario metrics.

Usage: .venv/bin/python scripts/run_eval.py
"""

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from alertmesh.sim import ScenarioConfig, run_scenario

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOURNAL_DIR = os.path.join(ROOT, "tmpwal")
RESULTS = os.path.join(ROOT, "results", "eval.json")


def scenarios():
    dur = 4000.0
    return [
        ScenarioConfig(
            name="baseline_clean",
            seed=101,
            n_devices=400,
            n_alerts=40000,
            duration=dur,
            journal_dir=JOURNAL_DIR,
        ),
        ScenarioConfig(
            name="chaos_moderate_drop10_dup5",
            seed=202,
            n_devices=400,
            n_alerts=40000,
            duration=dur,
            drop_pct=0.10,
            dup_pct=0.05,
            reorder_pct=0.10,
            jitter=0.10,
            n_device_restarts=40,
            n_offline_devices=8,
            journal_dir=JOURNAL_DIR,
        ),
        ScenarioConfig(
            name="chaos_heavy_drop30_dup10",
            seed=303,
            n_devices=400,
            n_alerts=40000,
            duration=dur,
            drop_pct=0.30,
            dup_pct=0.10,
            reorder_pct=0.15,
            jitter=0.15,
            n_device_restarts=60,
            n_offline_devices=8,
            journal_dir=JOURNAL_DIR,
        ),
        ScenarioConfig(
            name="partitions_drop10",
            seed=404,
            n_devices=300,
            n_alerts=30000,
            duration=dur,
            drop_pct=0.10,
            dup_pct=0.05,
            reorder_pct=0.10,
            partitions=[(600.0, 660.0), (1800.0, 1890.0), (3000.0, 3060.0)],
            n_device_restarts=30,
            n_offline_devices=6,
            journal_dir=JOURNAL_DIR,
        ),
        ScenarioConfig(
            name="gateway_crashes_x3_drop10",
            seed=505,
            n_devices=300,
            n_alerts=30000,
            duration=dur,
            drop_pct=0.10,
            dup_pct=0.05,
            reorder_pct=0.10,
            gateway_crashes=[(800.0, 30.0), (2000.0, 45.0), (3200.0, 30.0)],
            n_device_restarts=30,
            n_offline_devices=6,
            journal_dir=JOURNAL_DIR,
        ),
        ScenarioConfig(
            name="combined_worstcase_drop30_crashes_partitions",
            seed=606,
            n_devices=300,
            n_alerts=30000,
            duration=dur,
            drop_pct=0.30,
            dup_pct=0.10,
            reorder_pct=0.15,
            jitter=0.15,
            partitions=[(1200.0, 1290.0), (2600.0, 2660.0)],
            gateway_crashes=[(900.0, 30.0), (3100.0, 45.0)],
            n_device_restarts=60,
            n_offline_devices=8,
            journal_dir=JOURNAL_DIR,
        ),
    ]


def main():
    os.makedirs(os.path.dirname(RESULTS), exist_ok=True)
    out = {"scenarios": [], "totals": {}}
    total_emitted = 0
    total_delivered = 0
    total_lost = 0
    total_dup = 0
    all_ok = True
    t0 = time.perf_counter()
    for cfg in scenarios():
        print("running %s ..." % cfg.name, flush=True)
        r = run_scenario(cfg)
        ok = r.exactly_once()
        all_ok = all_ok and ok
        total_emitted += r.emitted
        total_delivered += r.delivered
        total_lost += r.lost
        total_dup += r.duplicated_ingests
        row = {
            "name": r.name,
            "exactly_once": ok,
            "emitted": r.emitted,
            "delivered": r.delivered,
            "lost": r.lost,
            "duplicated_ingests": r.duplicated_ingests,
            "dupes_dropped_at_gateway": r.dupes_dropped_at_gateway,
            "dup_attempts_at_triage_replay": r.dup_attempts_at_triage,
            "latency_p50_s": r.latency_p50,
            "latency_p99_s": r.latency_p99,
            "latency_max_s": r.latency_max,
            "hb_offline_devices": r.hb_expected,
            "hb_detected": r.hb_detected,
            "hb_false_positives": r.hb_false_positives,
            "hb_detection_mean_s": r.hb_detection_mean,
            "hb_detection_max_s": r.hb_detection_max,
            "gateway_crashes": r.gateway_crashes,
            "device_restarts": r.device_restarts,
            "journal_replay_entries": r.replay_entries,
            "link_frames_sent": r.link_sent,
            "link_frames_dropped": r.link_dropped,
            "link_frames_duplicated": r.link_duplicated,
            "sim_events": r.sim_events,
            "wall_seconds": round(r.wall_seconds, 3),
        }
        out["scenarios"].append(row)
        print(
            "  emitted=%d delivered=%d lost=%d dup=%d p50=%.3fs p99=%.3fs ok=%s"
            % (r.emitted, r.delivered, r.lost, r.duplicated_ingests,
               r.latency_p50 or -1, r.latency_p99 or -1, ok),
            flush=True,
        )
    out["totals"] = {
        "total_alerts_emitted": total_emitted,
        "total_alerts_delivered": total_delivered,
        "total_lost": total_lost,
        "total_duplicated_ingests": total_dup,
        "all_scenarios_exactly_once": all_ok,
        "wall_seconds": round(time.perf_counter() - t0, 2),
    }
    with open(RESULTS, "w") as fh:
        json.dump(out, fh, indent=2)
    print(json.dumps(out["totals"], indent=2))
    print("wrote", RESULTS)


if __name__ == "__main__":
    main()
