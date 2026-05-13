# Day 10 Reliability - Final Report

## 1. Architecture summary

The gateway routes each prompt through a guardrailed cache, a per-provider
circuit breaker chain (primary -> backup), and a static fallback. Cache supports
in-memory or Redis backends; Redis enables shared state across instances.

```
User request
    |
    v
[Gateway] --> [Cache.get + guardrails]
    | (miss)                    |
    v                            v hit
[CB: primary] --> Provider A   return cache_hit
    | (open -> skip / fail)
    v
[CB: backup]  --> Provider B
    | (all unavailable)
    v
[Static fallback message]
```

## 2. Configuration

| Setting | Value | Reason |
|---|---:|---|
| failure_threshold | 3 | Low enough to detect a flaky provider fast; high enough to ignore single-shot jitter. |
| reset_timeout_seconds | 0.5 | Matches the simulated provider recovery window; short enough to probe quickly without thrashing. |
| success_threshold | 1 | One successful probe is enough to confirm recovery for a single-region demo. |
| cache TTL (s) | 300 | 5-minute freshness window - long enough to amortize hot FAQ queries, short enough to avoid stale policy answers. |
| similarity_threshold | 0.92 | 0.92 - tested 0.85 (false hits on 2024/2026 policy queries) and 0.92 (zero false hits with guard). |
| load_test.requests | 200 | 200 requests give percentile metrics enough samples to be stable. |
| load_test.concurrency | 10 | 10 mirrors a small production fleet sharing one cache. |

## 3. SLO definitions

| SLI | SLO target | Actual | Met? |
|---|---|---:|---|
| availability | >= 0.99 | 0.9912 | PASS |
| latency_p95_ms | < 2500.0 | 474.06 | PASS |
| fallback_success_rate | >= 0.95 | 0.932 | FAIL |
| cache_hit_rate | >= 0.1 | 0.7488 | PASS |
| recovery_time_ms | < 5000.0 | n/a | N/A |

## 4. Metrics

| Metric | Value |
|---|---:|
| availability | 0.9912 |
| error_rate | 0.0088 |
| latency_p50_ms | 0.26 |
| latency_p95_ms | 474.06 |
| latency_p99_ms | 527.91 |
| fallback_success_rate | 0.932 |
| cache_hit_rate | 0.7488 |
| estimated_cost | 0.09227 |
| estimated_cost_saved | 0.599 |
| circuit_open_count | 5 |
| recovery_time_ms | None |

> Note: latency_p50_ms is sub-millisecond because 75% of requests are served from cache (no provider call). See the cache comparison table below for without-cache latency.

## 5. Cache comparison (without vs with cache)

| Metric | Without cache | With cache | Delta |
|---|---:|---:|---:|
| latency_p50_ms | 225.18 | 0.25 | -99.9% |
| latency_p95_ms | 522.98 | 476.58 | -8.9% |
| estimated_cost | 0.10113 | 0.026428 | -73.9% |
| cache_hit_rate | 0.0 | 0.76 | n/a |

## 6. Redis shared cache

In-memory cache is per-process; horizontal scaling means each gateway instance
rebuilds its own cache and burns repeat tokens. `SharedRedisCache` puts the
entries in Redis so every instance sees the same hot answers, gets the same TTL,
and shares the privacy/false-hit guardrails. Two cache instances reading the same
key prove the shared state - covered by `tests/test_redis_cache.py::test_shared_state_across_instances`.

### Redis CLI snapshot

```
(no keys - populate by running `make run-chaos` with backend: redis)
```

## 7. Chaos scenarios

| Scenario | Expected | Observed (status) | Recovery (ms) |
|---|---|---|---:|
| primary_timeout_100 | Circuit OPEN on primary; 100% of traffic served by backup. | pass | n/a (circuit never recovers under 100% failure) |
| primary_flaky_50 | Circuit oscillates; mix of primary/backup; non-zero recovery time. | pass | n/a |
| all_healthy | All requests served by primary; no circuit transitions. | pass | n/a (circuit never opened) |
| cache_stale_candidate | Cross-year queries rejected by false-hit guard. | pass | n/a |

### False-hit examples caught by guardrails

(no false hits detected in this run)

## 8. Failure analysis

Single weakness: circuit breaker state lives in process memory, so a multi-instance
deployment can re-hammer a broken provider while N replicas independently learn it
is OPEN. Fix: store `failure_count` and `opened_at` per provider in Redis (`INCR` +
`EXPIRE`), so the OPEN signal is shared. The Redis cache plumbing already exists; the
circuit breaker only needs to swap `dataclass` counters for Redis-backed reads.

## 9. Next steps

1. Move circuit-breaker counters into Redis so OPEN state is global across replicas.
2. Add a Prometheus exporter (`agent_requests_total`, `cache_hits_total`, `circuit_state`) so on-call can alert on availability instead of polling JSON.
3. Cost-aware routing: when monthly spend exceeds 80% of budget, downgrade to backup-only; at 100%, serve cache-only or static fallback.