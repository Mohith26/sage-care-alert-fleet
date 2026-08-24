# Benchmark and validation notes

All numbers below come from runs I executed on this machine: Apple silicon, macOS, Python 3.9.6, single thread. Raw JSON lives in results/eval.json and results/bench.json.

## Reproduce

```
python3 -m venv .venv && .venv/bin/pip install -U pip pytest pytest-cov
.venv/bin/python scripts/run_eval.py
.venv/bin/python scripts/run_bench.py
.venv/bin/pytest tests/ --color=no -q --cov=alertmesh --cov-report=term
```

Every scenario is seeded, so these commands reproduce the exact counters below (wall-clock times will vary with hardware).

## Exactly-once verification (212,803 alerts)

Six scenarios, roughly 30k to 41k alerts each, 300 to 400 devices, 4,000 simulated seconds per scenario. Verification is set equality between emitted alert IDs and triage-ingested alert IDs, plus a zero double-ingest check.

| scenario | emitted | delivered | lost | dup ingests | p50 | p99 | max |
|---|---|---|---|---|---|---|---|
| baseline_clean | 40,000 | 40,000 | 0 | 0 | 0.075 s | 0.099 s | 0.1 s |
| chaos_moderate_drop10_dup5 | 40,056 | 40,056 | 0 | 0 | 0.110 s | 8.06 s | 20.2 s |
| chaos_heavy_drop30_dup10 | 41,052 | 41,052 | 0 | 0 | 0.172 s | 12.68 s | 32.2 s |
| partitions_drop10 | 30,469 | 30,469 | 0 | 0 | 0.082 s | 64.08 s | 100.1 s |
| gateway_crashes_x3_drop10 | 30,052 | 30,052 | 0 | 0 | 0.081 s | 28.06 s | 52.1 s |
| combined_worstcase | 31,174 | 31,174 | 0 | 0 | 0.179 s | 64.15 s | 100.2 s |

Totals: 212,803 emitted, 212,803 delivered, 0 lost, 0 duplicated. Emitted counts exceed the configured button-alert counts because gateway-minted heartbeat-loss alerts (including false positives) flow through the same exactly-once pipeline.

Chaos actually happened: 885,092 link frames sent, 177,723 dropped by the lossy links across the suite, 52,762 duplicate frames deduped at the gateway, 5 gateway crashes with 89,671 journal entries replayed during in-run recovery, and 220 device restarts.

Latency tails track the failure being injected: the 64 s p99 in the partition scenarios is the 60 to 90 s partition windows plus retry backoff, and the 28 s p99 in the crash scenario is gateway downtime (30 to 45 s outages) landing on in-flight alerts.

## Heartbeat-loss detection

Heartbeat interval 30 s, timeout 90 s, monitor tick 5 s. Every intentionally-killed device was detected in every scenario (8/8, 8/8, 6/6, 6/6, 8/8).

- mean detection: 68.6 to 84.6 s in single-fault scenarios; max 109.9 s when a gateway outage overlapped the silence
- combined worst case degrades hard: mean 297.4 s, max 1,901.7 s, because each gateway restart resets the detection baseline and partitions hide the gap
- false positives from consecutive dropped heartbeats: 48 at 10% drop, 1,044 at 30% drop, 1,158 in the combined scenario. The 90 s timeout is only 3 heartbeat periods, so at 30% drop a false trip has about a 2.7% chance per device per window. I kept the aggressive timeout and reported the cost rather than tuning it away.

## Microbenchmarks (results/bench.json)

100,000 alerts per run, 3 repeats, single thread.

- pipeline throughput (journal append with per-line crc32 and flush, dedupe, triage ingest, ack attempt): 211,166 / 214,565 / 213,276 alerts per sec, median 213,276
- journal crash recovery: replaying a 100,000-entry journal (rebuild dedupe set plus re-offer to triage) took 0.212 / 0.207 / 0.205 s, median 0.207 s, about 483k entries per sec

Caveat: the journal flushes to the OS on every append but does not fsync, so these throughput numbers do not include real durability-to-disk cost. With fsync per append this would be orders of magnitude slower; batched group commit would be the realistic middle ground and I did not measure it.

## Tests

73 pytest tests, all passing, 1.4 s wall. Coverage on the alertmesh package: 97% (563 statements, 15 missed). Includes crash-recovery property tests over multiple seeds (heavy drop/dup, partitions, gateway crashes with replay, device restarts, combined worst case), a fault-injection test that crashes the gateway exactly between the journal append and triage delivery, torn-journal-tail recovery, dedupe, priority ordering, coalescing lifecycle, and a full-determinism test asserting identical counters for identical seeds.

Whole-suite eval wall time: 4.58 s for all six scenarios (1,587,714 discrete events processed).
