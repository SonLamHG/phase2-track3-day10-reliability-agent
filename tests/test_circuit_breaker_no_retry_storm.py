"""Verify circuit breaker fails fast when OPEN — no retry storm to a broken provider."""
from __future__ import annotations

from reliability_lab.circuit_breaker import CircuitBreaker, CircuitOpenError, CircuitState


def test_open_circuit_blocks_calls_without_invoking_function() -> None:
    breaker = CircuitBreaker(name="primary", failure_threshold=3, reset_timeout_seconds=60)
    calls = {"count": 0}

    def boom() -> None:
        calls["count"] += 1
        raise RuntimeError("provider down")

    # Drive failures to threshold.
    for _ in range(3):
        try:
            breaker.call(boom)
        except RuntimeError:
            pass

    assert breaker.state == CircuitState.OPEN
    fail_threshold_calls = calls["count"]

    # 10 more attempts should all fail-fast — provider NOT invoked.
    fast_fails = 0
    for _ in range(10):
        try:
            breaker.call(boom)
        except CircuitOpenError:
            fast_fails += 1

    assert fast_fails == 10, f"expected 10 CircuitOpenError, got {fast_fails}"
    assert calls["count"] == fail_threshold_calls, "provider should not be invoked while OPEN"


def test_failure_count_resets_after_halfopen_reopen() -> None:
    """After HALF_OPEN re-opens, the next CLOSED cycle starts a fresh failure count."""
    breaker = CircuitBreaker(name="primary", failure_threshold=3, reset_timeout_seconds=0.0)

    for _ in range(3):
        breaker.record_failure()
    assert breaker.state == CircuitState.OPEN

    # reset_timeout=0 means allow_request flips us to HALF_OPEN immediately.
    assert breaker.allow_request() is True
    assert breaker.state == CircuitState.HALF_OPEN

    # A failure in HALF_OPEN re-opens, and failure_count must be reset to 0 so
    # the NEXT closed-cycle can hit the threshold again cleanly.
    breaker.record_failure()
    assert breaker.state == CircuitState.OPEN
    assert breaker.failure_count == 0

    # Probe success closes the breaker; subsequent CLOSED cycle must accumulate from 0.
    assert breaker.allow_request() is True  # back to HALF_OPEN after timeout=0
    breaker.record_success()
    assert breaker.state == CircuitState.CLOSED

    for _ in range(2):
        breaker.record_failure()
    assert breaker.failure_count == 2
    assert breaker.state == CircuitState.CLOSED  # still under threshold
    breaker.record_failure()
    assert breaker.state == CircuitState.OPEN
