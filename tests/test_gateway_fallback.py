"""Backup must serve once primary's circuit is open."""
from __future__ import annotations

import random

from reliability_lab.cache import ResponseCache
from reliability_lab.circuit_breaker import CircuitBreaker
from reliability_lab.gateway import ReliabilityGateway
from reliability_lab.providers import FakeLLMProvider


def test_fallback_serves_when_primary_circuit_opens() -> None:
    random.seed(0)
    primary = FakeLLMProvider("primary", fail_rate=1.0, base_latency_ms=1, cost_per_1k_tokens=0.01)
    backup = FakeLLMProvider("backup", fail_rate=0.0, base_latency_ms=1, cost_per_1k_tokens=0.006)
    breakers = {
        "primary": CircuitBreaker("primary", failure_threshold=2, reset_timeout_seconds=60),
        "backup": CircuitBreaker("backup", failure_threshold=2, reset_timeout_seconds=60),
    }
    gateway = ReliabilityGateway([primary, backup], breakers, cache=None)

    routes = [gateway.complete(f"q{i}").route for i in range(6)]
    assert "fallback" in routes, routes

    # By call 6, primary breaker should be OPEN.
    assert breakers["primary"].state.value == "open"
    last = gateway.complete("final")
    assert last.route == "fallback"
    assert last.provider == "backup"
    assert "fallback:backup" in last.route_reason
