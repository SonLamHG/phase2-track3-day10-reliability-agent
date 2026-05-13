"""Smoke tests for chaos scenario pass/fail criteria."""
from __future__ import annotations

import random

from reliability_lab.chaos import run_simulation
from reliability_lab.config import (
    CacheConfig,
    CircuitBreakerConfig,
    LabConfig,
    LoadTestConfig,
    ProviderConfig,
    ScenarioConfig,
)


def _config(scenario_name: str, primary_fail: float, backup_fail: float = 0.0) -> LabConfig:
    return LabConfig(
        providers=[
            ProviderConfig(name="primary", fail_rate=primary_fail, base_latency_ms=1, cost_per_1k_tokens=0.01),
            ProviderConfig(name="backup", fail_rate=backup_fail, base_latency_ms=1, cost_per_1k_tokens=0.006),
        ],
        circuit_breaker=CircuitBreakerConfig(failure_threshold=3, reset_timeout_seconds=1, success_threshold=1),
        cache=CacheConfig(enabled=False, backend="memory", ttl_seconds=60, similarity_threshold=0.9),
        load_test=LoadTestConfig(requests=30, concurrency=1),
        scenarios=[ScenarioConfig(name=scenario_name)],
    )


def test_primary_timeout_100_passes() -> None:
    random.seed(0)
    cfg = _config("primary_timeout_100", primary_fail=0.0)  # base value; scenario override forces 1.0
    cfg.scenarios[0] = ScenarioConfig(name="primary_timeout_100", provider_overrides={"primary": 1.0})
    metrics = run_simulation(cfg, ["hello"])
    assert metrics.scenarios["primary_timeout_100"] == "pass"
    assert metrics.fallback_successes > 0
    assert metrics.circuit_open_count >= 1


def test_all_healthy_passes() -> None:
    random.seed(0)
    cfg = _config("all_healthy", primary_fail=0.0)
    metrics = run_simulation(cfg, ["hello"])
    assert metrics.scenarios["all_healthy"] == "pass"
    assert metrics.circuit_open_count == 0
