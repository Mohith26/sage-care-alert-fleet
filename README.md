# AlertMesh

I wanted to answer a question that sounds simple and is not: if a resident in a senior-living building presses a help button, can you guarantee the alert reaches a caregiver's queue exactly once, even when the radio link is garbage, the device power-cycles mid-send, and the building gateway crashes at the worst possible moment?

This repo is my attempt at that guarantee, built as a fully deterministic in-process simulation so I can throw hundreds of thousands of alerts at it under seeded chaos and check the answer exactly, not statistically.

## The contract

For every alert a device ever emits:

- it lands on the caregiver triage queue (zero loss)
- it lands there once (zero duplicate pages)

The verification is a literal set equality at the end of every run: the set of emitted alert IDs must equal the set of triage-ingested alert IDs, with zero double ingests. Across the full evaluation, 212,803 emitted alerts produced 212,803 triage deliveries, 0 lost and 0 duplicated, including runs with 30% frame drops, 10% duplication, reordering, network partitions, mid-flight device restarts, and gateway crashes with journal recovery.

## How the guarantee is put together

Nothing here is exotic. It is the standard recipe, wired carefully:

1. **Persist before transmit.** A button press writes the alert to the device's simulated NVRAM outbox before anything touches the radio. A device restart reloads the outbox and resends whatever was never acked. Retry timers are volatile and epoch-guarded so stale timers from before a restart become no-ops.
2. **At-least-once transport.** Devices resend on a timer until the gateway acks. Acks travel over the same lossy link, so a lost ack just means another retry.
3. **Write-ahead journal at the gateway.** Accept order is: dedupe check, journal append (with a crc32 per line), deliver to triage, then ack. A crash anywhere in that window is survivable: the device keeps retrying, and recovery replays the journal to rebuild the dedupe set. Torn tail lines from a crash mid-write are detected by checksum and skipped.
4. **Idempotency at the edge of the queue.** The triage queue keeps its own seen-ID barrier, so a journal replay that re-offers already-delivered alerts is a no-op. This is what makes "crashed after journaling but before delivering" safe; there is even a fault-injection hook in the gateway that crashes at exactly that point, and a test that proves recovery delivers the alert once.

The triage queue itself does priority ordering (help button, then pull cord, then battery/heartbeat tier, FIFO within a tier), per-resident coalescing (repeat alerts of the same kind for the same resident fold into one open entry with a count instead of stacking pages), and an ack/resolve lifecycle.

The gateway also watches heartbeats and raises a heartbeat-loss alert, with its own idempotency key, when a registered device goes quiet past the timeout. After a gateway restart, every device gets a fresh grace window so recovery does not spray false offline alerts.

## Numbers

Measured on Apple silicon, single thread, Python 3.9. Full details and reproduce commands in RESULTS.md, raw output in results/.

- 212,803 alerts across 6 seeded chaos scenarios: 0 lost, 0 duplicated, every scenario exactly-once
- delivery latency: p50 75 ms clean; p50 172 ms / p99 12.7 s at 30% drop; p99 64.1 s when 60 to 90 second partitions dominate the tail
- heartbeat loss: every truly-dead device detected; mean detection 68.6 to 84.6 s against a 90 s timeout in single-fault scenarios
- pipeline throughput: about 213k alerts/sec through the full journal + dedupe + triage path
- journal recovery: 100k entries replayed in about 0.21 s

## Running it

```
python3 -m venv .venv && .venv/bin/pip install -U pip pytest pytest-cov
.venv/bin/pytest tests/ -q            # 73 tests
.venv/bin/python scripts/run_eval.py  # the 200k+ alert chaos evaluation
.venv/bin/python scripts/run_bench.py # throughput + replay microbenchmarks
```

Everything is deterministic per seed: same seed, same interleaving, same counters, byte for byte.

## Limitations

- The network is a model, not a network. Drops, dups, reorder, jitter, and partitions are simulated in-process on a discrete-event clock. There are no real sockets, no real BLE, and no real timing effects like buffer bloat or radio contention.
- The journal flushes per append but does not fsync, and NVRAM writes are assumed atomic. Real flash on a battery device can tear writes in ways my crc-per-line scheme only partially models.
- Heartbeat-loss false positives are real and I report them instead of hiding them: at 30% frame drop, three consecutive lost heartbeats (about a 2.7% chance per device per window) trip the 90 s timeout, which produced 1,044 false offline flags in the heavy scenario. A production version needs a longer timeout, heartbeat acks, or link-quality-adaptive thresholds.
- In the combined worst-case scenario (crashes plus partitions plus 30% drop), heartbeat detection degrades badly: mean 297 s, worst case 1,902 s, because every gateway restart resets the grace window and partitions hide the silence. Detection under compound failure is honestly the weakest measured result here.
- Offline devices in the sim die cleanly with an empty outbox. A device that dies holding unsent alerts physically cannot deliver them; the zero-loss contract only covers alerts the hardware survives long enough to hand off.
- Single gateway, single building. No gateway failover or multi-hop mesh.
