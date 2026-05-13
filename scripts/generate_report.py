"""Generate the final Day 10 reliability report from metrics.json + default.yaml."""
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

import yaml


CONFIG_RATIONALES: dict[str, str] = {
    "failure_threshold": "Low enough to detect a flaky provider fast; high enough to ignore single-shot jitter.",
    "reset_timeout_seconds": "Matches the simulated provider recovery window; short enough to probe quickly without thrashing.",
    "success_threshold": "One successful probe is enough to confirm recovery for a single-region demo.",
    "cache_ttl_seconds": "5-minute freshness window - long enough to amortize hot FAQ queries, short enough to avoid stale policy answers.",
    "cache_similarity_threshold": "0.92 - tested 0.85 (false hits on 2024/2026 policy queries) and 0.92 (zero false hits with guard).",
    "load_test_requests": "200 requests give percentile metrics enough samples to be stable.",
    "load_test_concurrency": "10 mirrors a small production fleet sharing one cache.",
}


def _redis_keys_snapshot() -> str:
    try:
        out = subprocess.check_output(
            ["docker", "compose", "exec", "-T", "redis", "redis-cli", "KEYS", "rl:cache:*"],
            stderr=subprocess.STDOUT,
            timeout=5,
        )
        return out.decode().strip() or "(no keys - populate by running `make run-chaos` with backend: redis)"
    except Exception as exc:
        return f"(redis-cli unavailable: {exc})"


def _slo_table(metrics: dict) -> list[str]:
    targets = metrics.get("slo_targets", {})
    rows = [
        "| SLI | SLO target | Actual | Met? |",
        "|---|---|---:|---|",
    ]
    actuals: dict[str, float | None] = {
        "availability": metrics.get("availability"),
        "latency_p95_ms": metrics.get("latency_p95_ms"),
        "fallback_success_rate": metrics.get("fallback_success_rate"),
        "cache_hit_rate": metrics.get("cache_hit_rate"),
        "recovery_time_ms": metrics.get("recovery_time_ms"),
    }
    comparisons = {
        "availability": (">=", lambda a, t: a >= t),
        "latency_p95_ms": ("<", lambda a, t: a < t),
        "fallback_success_rate": (">=", lambda a, t: a >= t),
        "cache_hit_rate": (">=", lambda a, t: a >= t),
        "recovery_time_ms": ("<", lambda a, t: a < t),
    }
    for key, target in targets.items():
        actual = actuals.get(key)
        op, predicate = comparisons.get(key, (">=", lambda a, t: a >= t))
        if actual is None:
            rendered_actual = "n/a"
            met = "N/A"
        else:
            rendered_actual = str(actual)
            met = "PASS" if predicate(actual, target) else "FAIL"
        rows.append(f"| {key} | {op} {target} | {rendered_actual} | {met} |")
    return rows


def _config_table(config: dict) -> list[str]:
    cb = config["circuit_breaker"]
    cache = config["cache"]
    load = config["load_test"]
    return [
        "| Setting | Value | Reason |",
        "|---|---:|---|",
        f"| failure_threshold | {cb['failure_threshold']} | {CONFIG_RATIONALES['failure_threshold']} |",
        f"| reset_timeout_seconds | {cb['reset_timeout_seconds']} | {CONFIG_RATIONALES['reset_timeout_seconds']} |",
        f"| success_threshold | {cb['success_threshold']} | {CONFIG_RATIONALES['success_threshold']} |",
        f"| cache TTL (s) | {cache['ttl_seconds']} | {CONFIG_RATIONALES['cache_ttl_seconds']} |",
        f"| similarity_threshold | {cache['similarity_threshold']} | {CONFIG_RATIONALES['cache_similarity_threshold']} |",
        f"| load_test.requests | {load['requests']} | {CONFIG_RATIONALES['load_test_requests']} |",
        f"| load_test.concurrency | {load.get('concurrency', 1)} | {CONFIG_RATIONALES['load_test_concurrency']} |",
    ]


def _metrics_table(metrics: dict) -> list[str]:
    keys = [
        "availability",
        "error_rate",
        "latency_p50_ms",
        "latency_p95_ms",
        "latency_p99_ms",
        "fallback_success_rate",
        "cache_hit_rate",
        "estimated_cost",
        "estimated_cost_saved",
        "circuit_open_count",
        "recovery_time_ms",
    ]
    rows = ["| Metric | Value |", "|---|---:|"]
    for k in keys:
        rows.append(f"| {k} | {metrics.get(k)} |")
    cache_hit_rate = metrics.get("cache_hit_rate", 0.0) or 0.0
    if cache_hit_rate >= 0.3:
        rows.append("")
        rows.append(
            f"> Note: latency_p50_ms is sub-millisecond because {cache_hit_rate * 100:.0f}% of requests "
            f"are served from cache (no provider call). See the cache comparison table below for "
            f"without-cache latency."
        )
    return rows


def _cache_comparison_table(metrics: dict) -> list[str]:
    comparison = metrics.get("cache_comparison", {})
    off = comparison.get("without_cache", {})
    on = comparison.get("with_cache", {})
    if not off or not on:
        return ["(cache comparison block not present - rerun `make run-chaos`)"]

    def delta(a: float, b: float) -> str:
        if a == 0:
            return "n/a"
        return f"{((b - a) / a) * 100:+.1f}%"

    rows = ["| Metric | Without cache | With cache | Delta |", "|---|---:|---:|---:|"]
    for k in ["latency_p50_ms", "latency_p95_ms", "estimated_cost", "cache_hit_rate"]:
        a, b = off.get(k, 0), on.get(k, 0)
        rows.append(f"| {k} | {a} | {b} | {delta(float(a or 0), float(b or 0))} |")
    return rows


def _chaos_table(metrics: dict) -> list[str]:
    rows = [
        "| Scenario | Expected | Observed (status) | Recovery (ms) |",
        "|---|---|---|---:|",
    ]
    expected = {
        "primary_timeout_100": "Circuit OPEN on primary; 100% of traffic served by backup.",
        "primary_flaky_50": "Circuit oscillates; mix of primary/backup; non-zero recovery time.",
        "all_healthy": "All requests served by primary; no circuit transitions.",
        "cache_stale_candidate": "Cross-year queries rejected by false-hit guard.",
    }
    per_recovery = metrics.get("per_scenario_recovery_ms", {})
    for name, status in metrics.get("scenarios", {}).items():
        recovery = per_recovery.get(name)
        if recovery is None:
            if name == "primary_timeout_100":
                recovery_str = "n/a (circuit never recovers under 100% failure)"
            elif name == "all_healthy":
                recovery_str = "n/a (circuit never opened)"
            else:
                recovery_str = "n/a"
        else:
            recovery_str = f"{recovery:.1f}"
        rows.append(
            f"| {name} | {expected.get(name, '(custom)')} | {status} | {recovery_str} |"
        )
    return rows


def _architecture() -> list[str]:
    return [
        "```",
        "User request",
        "    |",
        "    v",
        "[Gateway] --> [Cache.get + guardrails]",
        "    | (miss)                    |",
        "    v                            v hit",
        "[CB: primary] --> Provider A   return cache_hit",
        "    | (open -> skip / fail)",
        "    v",
        "[CB: backup]  --> Provider B",
        "    | (all unavailable)",
        "    v",
        "[Static fallback message]",
        "```",
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics", default="reports/metrics.json")
    parser.add_argument("--config", default="configs/default.yaml")
    parser.add_argument("--out", default="reports/final_report.md")
    args = parser.parse_args()

    metrics = json.loads(Path(args.metrics).read_text())
    config = yaml.safe_load(Path(args.config).read_text())

    lines: list[str] = []
    lines += ["# Day 10 Reliability - Final Report", ""]
    lines += ["## 1. Architecture summary", ""]
    lines += [
        "The gateway routes each prompt through a guardrailed cache, a per-provider",
        "circuit breaker chain (primary -> backup), and a static fallback. Cache supports",
        "in-memory or Redis backends; Redis enables shared state across instances.",
        "",
    ]
    lines += _architecture() + [""]

    lines += ["## 2. Configuration", ""]
    lines += _config_table(config) + [""]

    lines += ["## 3. SLO definitions", ""]
    lines += _slo_table(metrics) + [""]

    lines += ["## 4. Metrics", ""]
    lines += _metrics_table(metrics) + [""]

    lines += ["## 5. Cache comparison (without vs with cache)", ""]
    lines += _cache_comparison_table(metrics) + [""]

    lines += ["## 6. Redis shared cache", ""]
    lines += [
        "In-memory cache is per-process; horizontal scaling means each gateway instance",
        "rebuilds its own cache and burns repeat tokens. `SharedRedisCache` puts the",
        "entries in Redis so every instance sees the same hot answers, gets the same TTL,",
        "and shares the privacy/false-hit guardrails. Two cache instances reading the same",
        "key prove the shared state - covered by `tests/test_redis_cache.py::test_shared_state_across_instances`.",
        "",
        "### Redis CLI snapshot",
        "",
        "```",
        _redis_keys_snapshot(),
        "```",
        "",
    ]

    lines += ["## 7. Chaos scenarios", ""]
    lines += _chaos_table(metrics) + [""]

    lines += ["### False-hit examples caught by guardrails", ""]
    if metrics.get("false_hit_examples"):
        for ex in metrics["false_hit_examples"]:
            lines.append(f"- `{ex.get('query')}` matched `{ex.get('matched')}` (score={ex.get('score')}) -- rejected.")
    else:
        lines.append("(no false hits detected in this run)")
    lines.append("")

    lines += ["## 8. Failure analysis", ""]
    lines += [
        "Single weakness: circuit breaker state lives in process memory, so a multi-instance",
        "deployment can re-hammer a broken provider while N replicas independently learn it",
        "is OPEN. Fix: store `failure_count` and `opened_at` per provider in Redis (`INCR` +",
        "`EXPIRE`), so the OPEN signal is shared. The Redis cache plumbing already exists; the",
        "circuit breaker only needs to swap `dataclass` counters for Redis-backed reads.",
        "",
    ]

    lines += ["## 9. Next steps", ""]
    lines += [
        "1. Move circuit-breaker counters into Redis so OPEN state is global across replicas.",
        "2. Add a Prometheus exporter (`agent_requests_total`, `cache_hits_total`, `circuit_state`) so on-call can alert on availability instead of polling JSON.",
        "3. Cost-aware routing: when monthly spend exceeds 80% of budget, downgrade to backup-only; at 100%, serve cache-only or static fallback.",
    ]

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text("\n".join(lines))
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
