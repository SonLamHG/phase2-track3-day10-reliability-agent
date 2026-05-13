"""Privacy + cross-year guardrails for ResponseCache."""
from __future__ import annotations

from reliability_lab.cache import ResponseCache, _is_uncacheable, _looks_like_false_hit


def test_is_uncacheable_detects_privacy_terms() -> None:
    assert _is_uncacheable("show me the balance for user 42") is True
    assert _is_uncacheable("password reset flow") is True
    assert _is_uncacheable("explain circuit breaker states") is False


def test_looks_like_false_hit_detects_different_years() -> None:
    assert _looks_like_false_hit("refund policy for 2024", "refund policy for 2026") is True
    assert _looks_like_false_hit("refund policy", "refund policy details") is False


def test_privacy_query_is_not_cached() -> None:
    cache = ResponseCache(ttl_seconds=60, similarity_threshold=0.5)
    cache.set("account balance for user 7", "Balance: $42")
    cached, _ = cache.get("account balance for user 7")
    assert cached is None


def test_cross_year_query_is_rejected_and_logged() -> None:
    # Low threshold so the year-variant pair scores a hit; the false-hit
    # guard (not the score) is what this test verifies.
    cache = ResponseCache(ttl_seconds=60, similarity_threshold=0.3)
    cache.set("Summarize refund policy for 2024 deadline", "Old policy")
    cached, _ = cache.get("Summarize refund policy for 2026 deadline")
    assert cached is None
    assert len(cache.false_hit_log) == 1
    entry = cache.false_hit_log[0]
    assert "2026" in entry["query"]
    assert "2024" in entry["matched"]


def test_exact_match_returns_score_1() -> None:
    cache = ResponseCache(ttl_seconds=60, similarity_threshold=0.5)
    cache.set("hello world", "hi there")
    cached, score = cache.get("hello world")
    assert cached == "hi there"
    assert score == 1.0


def test_similar_query_returns_semantic_score() -> None:
    score = ResponseCache.similarity(
        "what is a circuit breaker",
        "what is circuit breaker pattern",
    )
    assert 0.5 < score < 1.0, f"expected semantic score in (0.5, 1.0), got {score}"
