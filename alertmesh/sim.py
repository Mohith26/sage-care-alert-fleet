"""Fleet simulation harness.

Wires N devices to one gateway through lossy links, drives a seeded
schedule of alert emissions, device restarts, device deaths, partitions,
and gateway crashes, then verifies the exactly-once contract:

    set(emitted alert_ids) == set(triage delivered alert_ids)

with zero losses and zero duplicate ingests.
"""

import os
import random
import statistics
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .device import Device
from .events import BATTERY_LOW, HEARTBEAT_LOSS, HELP_BUTTON, PULL_CORD
from .gateway import Gateway
from .network import LossyLink
from .simclock import Scheduler
from .triage import TriageQueue

ALERT_MIX = [(HELP_BUTTON, 0.35), (PULL_CORD, 0.35), (BATTERY_LOW, 0.30)]


@dataclass
class ScenarioConfig:
    name: str
    seed: int
    n_devices: int = 200
    n_alerts: int = 10000
    duration: float = 3600.0
    drop_pct: float = 0.0
    dup_pct: float = 0.0
    reorder_pct: float = 0.0
    base_latency: float = 0.05
    jitter: float = 0.05
    partitions: List[Tuple[float, float]] = field(default_factory=list)
    gateway_crashes: List[Tuple[float, float]] = field(default_factory=list)  # (t, down_for)
    n_device_restarts: int = 0
    n_offline_devices: int = 0
    retry_timeout: float = 4.0
    hb_interval: float = 30.0
    hb_timeout: float = 90.0
    hb_check_interval: float = 5.0
    drain_cap: float = 1200.0
    journal_dir: Optional[str] = None


@dataclass
class ScenarioResult:
    name: str
    emitted: int
    delivered: int
    lost: int
    duplicated_ingests: int
    dupes_dropped_at_gateway: int
    dup_attempts_at_triage: int
    latency_p50: Optional[float]
    latency_p99: Optional[float]
    latency_max: Optional[float]
    hb_expected: int
    hb_detected: int
    hb_false_positives: int
    hb_detection_mean: Optional[float]
    hb_detection_max: Optional[float]
    gateway_crashes: int
    device_restarts: int
    replay_entries: int
    replay_skipped: int
    link_sent: int
    link_dropped: int
    link_duplicated: int
    sim_events: int
    wall_seconds: float

    def exactly_once(self):
        return self.lost == 0 and self.duplicated_ingests == 0


def percentile(values, pct):
    if not values:
        return None
    vs = sorted(values)
    k = (len(vs) - 1) * pct / 100.0
    lo = int(k)
    hi = min(lo + 1, len(vs) - 1)
    frac = k - lo
    return vs[lo] * (1 - frac) + vs[hi] * frac


def run_scenario(cfg: ScenarioConfig) -> ScenarioResult:
    import time as _time

    t_wall = _time.perf_counter()
    rng = random.Random(cfg.seed)
    sched = Scheduler()
    truth_log = []

    uplink = LossyLink(
        sched,
        rng=random.Random(cfg.seed + 1),
        drop_pct=cfg.drop_pct,
        dup_pct=cfg.dup_pct,
        reorder_pct=cfg.reorder_pct,
        base_latency=cfg.base_latency,
        jitter=cfg.jitter,
        partitions=cfg.partitions,
    )
    downlink = LossyLink(
        sched,
        rng=random.Random(cfg.seed + 2),
        drop_pct=cfg.drop_pct,
        dup_pct=cfg.dup_pct,
        reorder_pct=cfg.reorder_pct,
        base_latency=cfg.base_latency,
        jitter=cfg.jitter,
        partitions=cfg.partitions,
    )

    triage = TriageQueue()
    journal_dir = cfg.journal_dir or os.path.join("/tmp", "alertmesh-journals")
    journal_path = os.path.join(journal_dir, "%s.wal" % cfg.name)
    if os.path.exists(journal_path):
        os.remove(journal_path)
    gateway = Gateway(
        sched,
        journal_path,
        triage,
        downlink,
        hb_timeout=cfg.hb_timeout,
        hb_check_interval=cfg.hb_check_interval,
        hb_monitor_stop_after=cfg.duration,
        truth_log=truth_log,
    )

    devices = []
    for i in range(cfg.n_devices):
        d = Device(
            sched,
            device_id="dev-%04d" % i,
            resident_id="res-%04d" % i,
            uplink=uplink,
            gateway=gateway,
            retry_timeout=cfg.retry_timeout,
            hb_interval=cfg.hb_interval,
            hb_stop_after=cfg.duration,
            truth_log=truth_log,
        )
        gateway.register(d)
        devices.append(d)
        d.start_heartbeats(phase=rng.random() * cfg.hb_interval)
    gateway.start_monitor()

    # Devices that will die mid-run (heartbeat-loss targets). They emit no
    # button alerts, so the zero-loss contract stays physically satisfiable.
    offline_ids = set(rng.sample(range(cfg.n_devices), cfg.n_offline_devices))
    offline_times = {}
    for i in offline_ids:
        # Die somewhere in the middle so detection fits inside the run.
        t = cfg.duration * (0.2 + 0.5 * rng.random())
        offline_times[i] = t
        sched.at(t, devices[i].go_offline)

    # Alert emission schedule (uniform over the first 85% of the run).
    emit_window = cfg.duration * 0.85
    online_indices = [i for i in range(cfg.n_devices) if i not in offline_ids]
    for _ in range(cfg.n_alerts):
        i = rng.choice(online_indices)
        t = rng.random() * emit_window
        r = rng.random()
        acc = 0.0
        kind = ALERT_MIX[-1][0]
        for k, w in ALERT_MIX:
            acc += w
            if r < acc:
                kind = k
                break
        sched.at(t, (lambda dev, kk: lambda: dev.emit(kk))(devices[i], kind))

    # Device restarts mid-flight.
    for _ in range(cfg.n_device_restarts):
        i = rng.choice(online_indices)
        t = rng.random() * emit_window
        sched.at(t, devices[i].restart)

    # Gateway crashes.
    replay_entries_total = 0
    replay_skipped_total = 0
    crash_replays = []

    def make_crash(down_for):
        def crash():
            gateway.crash()
            sched.after(down_for, restart)

        def restart():
            gateway.restart()
            crash_replays.append(
                (gateway.last_replay_entries, gateway.last_replay_skipped)
            )

        return crash

    for t, down_for in cfg.gateway_crashes:
        sched.at(t, make_crash(down_for))

    # Run to the end of the scenario, then drain retries and in-flight frames.
    sched.run(until=cfg.duration + cfg.drain_cap)
    # Anything still pending past the drain cap would be a liveness bug;
    # surface it by finishing the queue (bounded by retry logic winding down).
    sched.run()

    for entries, skipped in crash_replays:
        replay_entries_total += entries
        replay_skipped_total += skipped

    emitted_ids = set(a.alert_id for a in truth_log)
    delivered_ids = triage.delivered_ids
    lost = len(emitted_ids - delivered_ids)
    phantom = len(delivered_ids - emitted_ids)

    # Latency stats over device-originated alerts only. Heartbeat-loss alerts
    # are minted at the gateway, so their transport latency is trivially zero.
    lat = [d - e for (k, e, d) in triage.latencies if k != HEARTBEAT_LOSS]
    # True detection = first flag at/after the device actually died. Anything
    # else (online device flagged, or flag before death) is a false positive
    # caused by consecutive heartbeat drops on the lossy link.
    hb_true = {}
    hb_false = 0
    for device_id, flagged_at in gateway.hb_detections:
        idx = int(device_id.split("-")[1])
        if idx in offline_times and flagged_at >= offline_times[idx]:
            if idx not in hb_true:
                hb_true[idx] = flagged_at - offline_times[idx]
        else:
            hb_false += 1
    hb_lat = list(hb_true.values())

    gateway.journal.close()

    return ScenarioResult(
        name=cfg.name,
        emitted=len(emitted_ids),
        delivered=len(delivered_ids),
        lost=lost + phantom,
        duplicated_ingests=len(triage.latencies) - len(delivered_ids),
        dupes_dropped_at_gateway=gateway.dupes_dropped,
        dup_attempts_at_triage=triage.dup_attempts,
        latency_p50=percentile(lat, 50),
        latency_p99=percentile(lat, 99),
        latency_max=max(lat) if lat else None,
        hb_expected=cfg.n_offline_devices,
        hb_detected=len(hb_lat),
        hb_false_positives=hb_false,
        hb_detection_mean=statistics.mean(hb_lat) if hb_lat else None,
        hb_detection_max=max(hb_lat) if hb_lat else None,
        gateway_crashes=gateway.crashes,
        device_restarts=sum(d.restarts for d in devices),
        replay_entries=replay_entries_total,
        replay_skipped=replay_skipped_total,
        link_sent=uplink.sent + downlink.sent,
        link_dropped=uplink.dropped + downlink.dropped,
        link_duplicated=uplink.duplicated + downlink.duplicated,
        sim_events=sched.events_processed,
        wall_seconds=_time.perf_counter() - t_wall,
    )
