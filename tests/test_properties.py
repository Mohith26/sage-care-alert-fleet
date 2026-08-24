"""End-to-end property tests: the exactly-once contract must hold for any
seed under any chaos mix the simulator can produce."""

import pytest

from alertmesh.sim import ScenarioConfig, run_scenario


def run(seed, tmp_path, **kw):
    defaults = dict(
        name="prop-%d" % seed,
        seed=seed,
        n_devices=40,
        n_alerts=1500,
        duration=900.0,
        journal_dir=str(tmp_path),
    )
    defaults.update(kw)
    return run_scenario(ScenarioConfig(**defaults))


def assert_exactly_once(r):
    assert r.lost == 0, "%s lost %d" % (r.name, r.lost)
    assert r.duplicated_ingests == 0
    assert r.emitted == r.delivered


@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
def test_exactly_once_under_heavy_drop_and_dup(seed, tmp_path):
    r = run(seed, tmp_path, drop_pct=0.30, dup_pct=0.10, reorder_pct=0.15)
    assert_exactly_once(r)
    assert r.dupes_dropped_at_gateway > 0  # chaos actually happened


@pytest.mark.parametrize("seed", [11, 12, 13])
def test_exactly_once_under_partitions(seed, tmp_path):
    r = run(seed, tmp_path, drop_pct=0.10,
            partitions=[(200.0, 260.0), (500.0, 560.0)])
    assert_exactly_once(r)


@pytest.mark.parametrize("seed", [21, 22, 23])
def test_exactly_once_under_gateway_crashes(seed, tmp_path):
    r = run(seed, tmp_path, drop_pct=0.10, dup_pct=0.05,
            gateway_crashes=[(300.0, 20.0), (600.0, 30.0)])
    assert_exactly_once(r)
    assert r.gateway_crashes == 2
    assert r.replay_entries > 0


@pytest.mark.parametrize("seed", [31, 32, 33])
def test_exactly_once_under_device_restarts(seed, tmp_path):
    r = run(seed, tmp_path, drop_pct=0.20, dup_pct=0.10, n_device_restarts=15)
    assert_exactly_once(r)
    assert r.device_restarts == 15


@pytest.mark.parametrize("seed", [41, 42])
def test_exactly_once_combined_worst_case(seed, tmp_path):
    r = run(seed, tmp_path, drop_pct=0.30, dup_pct=0.10, reorder_pct=0.15,
            partitions=[(250.0, 310.0)], gateway_crashes=[(450.0, 25.0)],
            n_device_restarts=10, n_offline_devices=3)
    assert_exactly_once(r)


def test_offline_devices_all_detected(tmp_path):
    r = run(77, tmp_path, drop_pct=0.05, n_offline_devices=5)
    assert r.hb_detected == r.hb_expected == 5
    assert r.hb_detection_mean is not None
    assert r.hb_detection_mean > 0


def test_same_seed_is_fully_deterministic(tmp_path):
    kw = dict(drop_pct=0.25, dup_pct=0.10, reorder_pct=0.10,
              gateway_crashes=[(300.0, 20.0)], n_device_restarts=8)
    r1 = run(99, tmp_path / "a", **kw)
    r2 = run(99, tmp_path / "b", **kw)
    assert r1.emitted == r2.emitted
    assert r1.delivered == r2.delivered
    assert r1.dupes_dropped_at_gateway == r2.dupes_dropped_at_gateway
    assert r1.latency_p99 == r2.latency_p99
    assert r1.sim_events == r2.sim_events


def test_different_seeds_diverge(tmp_path):
    kw = dict(drop_pct=0.25, dup_pct=0.10)
    r1 = run(101, tmp_path / "a", **kw)
    r2 = run(102, tmp_path / "b", **kw)
    assert (r1.sim_events, r1.dupes_dropped_at_gateway) != (
        r2.sim_events, r2.dupes_dropped_at_gateway)


def test_clean_network_low_latency(tmp_path):
    r = run(55, tmp_path)
    assert_exactly_once(r)
    assert r.latency_p99 < 1.0
