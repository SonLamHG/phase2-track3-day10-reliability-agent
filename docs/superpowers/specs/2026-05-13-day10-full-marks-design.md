# Day 10 Reliability Lab — Full-Marks Design Spec

Date: 2026-05-13
Target: 100/100 + stretch credit (concurrency, graceful Redis fallback, SLO table).

## 1. Goal

Take the starter skeleton at `phase2-track3-day10-reliability-agent/` and produce a graded submission that scores full marks per the rubric in [README.md](../../../README.md#rubric-overview-100-points-total):

| Category | Points |
|---|---:|
| Circuit breaker & fallback | 25 |
| In-memory cache & cost | 15 |
| Redis shared cache | 15 |
| Observability & metrics | 15 |
| Chaos & load testing | 15 |
| Report & code quality | 15 |

Plus stretch goals: concurrency in load test, graceful Redis degradation, SLO pass/fail table, false-hit log with examples.

## 2. Architecture overview

```
User prompt
   |
   v
ReliabilityGateway.complete(prompt)
   |
   |-- Cache.get(prompt)
   |     guard: _is_uncacheable → skip
   |     exact: hash match → score=1.0
   |     semantic: similarity ≥ threshold
   |     guard: _looks_like_false_hit → reject + log
   |     HIT → GatewayResponse(route="cache_hit", route_reason="cache_hit:exact"|"cache_hit:semantic:0.95")
   |
   v MISS
   for provider in [primary, backup]:
       breaker.allow_request()?
           NO  → route_reason="skip:circuit_open:{provider}" → continue
           YES → provider.complete()
                 success → cache.set() (guarded) → return GatewayResponse(route="primary"|"fallback",
                            route_reason="primary:{name}" | "fallback:{name}:primary_open|primary_error")
                 fail    → breaker.record_failure() → continue
   exhaust → return GatewayResponse(route="static_fallback",
              route_reason="static_fallback:{cause}")
```

`route` stays a small bucketed value (`primary` / `fallback` / `cache_hit` / `static_fallback`) so the existing contract test and `chaos.py` route-matching logic keep working. `route_reason` is a new string field that carries the rubric-required granular reason (provider name + why).

## 3. Component changes

### 3.1 `src/reliability_lab/circuit_breaker.py`

State machine is already 80% correct. Required tightening:

- `record_failure`: after transitioning to OPEN from HALF_OPEN re-failure, reset `failure_count = 0` so subsequent CLOSED-cycle starts clean.
- `record_success`: reset `success_count = 0` after CLOSED transition (already done).
- `_transition`: log retains `from`, `to`, `reason`, `ts` (real wall-clock `time.time()`) so `calculate_recovery_time_ms` produces real numbers.

No public API change.

### 3.2 `src/reliability_lab/gateway.py`

`GatewayResponse` gains a field:

```python
@dataclass(slots=True)
class GatewayResponse:
    text: str
    route: str            # bucket: primary | fallback | cache_hit | static_fallback
    provider: str | None
    cache_hit: bool
    latency_ms: float
    estimated_cost: float
    error: str | None = None
    route_reason: str = ""   # granular: e.g. "primary:llm-primary", "cache_hit:semantic:0.94", "static_fallback:all_circuits_open"
```

`complete()` is restructured to:

1. Start `t0 = time.perf_counter()` so `latency_ms` covers routing overhead.
2. Cache lookup with score; build `route_reason = "cache_hit:exact"` if score == 1.0 else `f"cache_hit:semantic:{score:.2f}"`.
3. Iterate providers; track why each was skipped (circuit OPEN vs. just-failed).
4. Cache `set()` wrapped in try/except — Redis errors must not break gateway.
5. Static fallback: assemble cause from accumulated skip/error reasons.
6. Stamp `latency_ms = (perf_counter() - t0) * 1000` on the returned response (cache hits get this routing overhead too).

### 3.3 `src/reliability_lab/cache.py` — `ResponseCache`

Similarity hybrid:

```python
@staticmethod
def similarity(a: str, b: str) -> float:
    na, nb = _normalize(a), _normalize(b)
    if na == nb:
        return 1.0
    jacc = _jaccard_tokens(na, nb)
    trig = _trigram_overlap(na, nb)
    return 0.6 * jacc + 0.4 * trig
```

Where:
- `_normalize`: lowercase, strip punctuation, collapse whitespace.
- `_jaccard_tokens`: existing set intersection / union over space-split tokens.
- `_trigram_overlap`: Jaccard over character 3-grams.

This makes "refund policy for 2024" vs "refund policy for 2026" lower-scoring than pure token Jaccard (trigrams differ on `024` vs `026`), but the false-hit guard below is the real safety net.

`get()` adds guardrails:

```python
def get(self, query):
    if _is_uncacheable(query):
        return None, 0.0
    # prune TTL, scan entries, pick best
    if best_score >= self.similarity_threshold:
        if _looks_like_false_hit(query, best_entry.key):
            self.false_hit_log.append({"query": query, "matched": best_entry.key, "score": best_score})
            return None, best_score
        return best_value, best_score
    return None, best_score
```

`set()` skips when uncacheable. Adds `self.false_hit_log: list[dict] = []`.

### 3.4 `src/reliability_lab/cache.py` — `SharedRedisCache`

Implement `get()` and `set()`:

```python
def set(self, query, value, metadata=None):
    if _is_uncacheable(query):
        return
    try:
        key = f"{self.prefix}{self._query_hash(query)}"
        self._redis.hset(key, mapping={"query": query, "response": value})
        self._redis.expire(key, self.ttl_seconds)
    except redis_lib.RedisError:
        pass  # graceful: silent set failure

def get(self, query):
    if _is_uncacheable(query):
        return None, 0.0
    try:
        exact_key = f"{self.prefix}{self._query_hash(query)}"
        exact = self._redis.hget(exact_key, "response")
        if exact is not None:
            return exact, 1.0

        best_value, best_score, best_query = None, 0.0, None
        for key in self._redis.scan_iter(f"{self.prefix}*"):
            cached_query = self._redis.hget(key, "query")
            if not cached_query:
                continue
            score = ResponseCache.similarity(query, cached_query)
            if score > best_score:
                best_score = score
                best_value = self._redis.hget(key, "response")
                best_query = cached_query
        if best_score >= self.similarity_threshold and best_query is not None:
            if _looks_like_false_hit(query, best_query):
                self.false_hit_log.append({"query": query, "matched": best_query, "score": best_score})
                return None, best_score
            return best_value, best_score
        return None, best_score
    except redis_lib.RedisError:
        return None, 0.0  # graceful: behave like cache miss
```

This handles all six Redis tests in `tests/test_redis_cache.py` plus the graceful degradation stretch.

### 3.5 `src/reliability_lab/chaos.py`

- Add a fourth scenario `cache_stale_candidate` to `configs/default.yaml` (no provider overrides; runs through low-threshold cache mode handled separately in script).
- Add per-scenario pass/fail predicates:

| Scenario | Pass criterion |
|---|---|
| `primary_timeout_100` | `fallback_success_rate ≥ 0.9` ∧ `circuit_open_count ≥ 1` |
| `primary_flaky_50` | mix of primary and fallback routes ∧ `availability ≥ 0.85` |
| `all_healthy` | `error_rate ≤ 0.1` ∧ `circuit_open_count == 0` |
| `cache_stale_candidate` | `false_hit_log` is non-empty (guardrail caught at least one) OR `false_hit_rate == 0` with explanation |

- Add cache comparison helper:

```python
def run_cache_comparison(config, queries) -> dict[str, dict]:
    cfg_off = config.model_copy(deep=True)
    cfg_off.cache.enabled = False
    cfg_on = config  # cache enabled in default.yaml
    return {
        "without_cache": run_scenario(cfg_off, queries, ScenarioConfig(name="cache_off")).to_report_dict(),
        "with_cache":    run_scenario(cfg_on,  queries, ScenarioConfig(name="cache_on")).to_report_dict(),
    }
```

- **Concurrency (stretch)**: replace `for _ in range(request_count)` with `ThreadPoolExecutor(max_workers=config.load_test.concurrency)` when `concurrency > 1`. Protect `ResponseCache._entries` with `threading.Lock`. Record both sequential and concurrent runs in `cache_comparison`-style block.

### 3.6 `src/reliability_lab/metrics.py`

Additions to `RunMetrics`:

```python
cache_comparison: dict[str, dict[str, object]] = Field(default_factory=dict)
false_hit_examples: list[dict[str, object]] = Field(default_factory=list)
slo_targets: dict[str, float] = Field(default_factory=dict)
```

`to_report_dict()` exposes them. New helper `write_csv(path)` writes per-request latency rows (stretch, for completeness).

### 3.7 `src/reliability_lab/config.py`

`LoadTestConfig` gains:

```python
class LoadTestConfig(BaseModel):
    requests: int = Field(gt=0)
    concurrency: int = Field(default=1, gt=0)
```

Default YAML bumps `requests` to 200 and adds `concurrency: 10`.

### 3.8 `scripts/generate_report.py`

Rewrite to fill the 9-section template using `metrics.json`. Sections:

1. Architecture (ASCII diagram embedded).
2. Configuration table (read from `configs/default.yaml`, supply hand-written `Reason` column from a constants dict inside the script).
3. SLO table (using `slo_targets` block).
4. Metrics table (all required fields).
5. Cache comparison (from `cache_comparison` block).
6. Redis shared cache section (paste subprocess `docker compose exec redis redis-cli KEYS "rl:cache:*"` if available; else placeholder text).
7. Chaos scenarios table (expected vs observed, pass/fail).
8. Failure analysis (static prose committed to script — one weakness + fix).
9. Next steps (static prose, 3 items).

### 3.9 `configs/default.yaml`

```yaml
load_test:
  requests: 200
  concurrency: 10
scenarios:
  - name: primary_timeout_100
    description: "Primary provider fails 100% — all traffic should fallback"
    provider_overrides: { primary: 1.0 }
  - name: primary_flaky_50
    description: "Primary provider fails 50% — circuit should oscillate"
    provider_overrides: { primary: 0.5 }
  - name: all_healthy
    description: "Baseline — both providers healthy"
    provider_overrides: {}
  - name: cache_stale_candidate
    description: "Different-year queries must not produce false cache hits"
    provider_overrides: {}
```

## 4. Tests

### 4.1 Existing tests adjusted

- `tests/test_todo_requirements.py`: remove `@pytest.mark.xfail` so the now-passing test contributes positively to the run.
- `tests/test_gateway_contract.py`: widen the accepted route bucket set to `{"primary", "fallback", "cache_hit", "static_fallback"}` (since cache hits become their own bucket) and assert `result.route_reason` is non-empty.

### 4.2 New tests

- `tests/test_circuit_breaker_no_retry_storm.py`
  - Setup: `FakeLLMProvider(fail_rate=1.0)`, breaker `failure_threshold=3`.
  - Drive `failure_threshold` failures → assert state OPEN.
  - Patch `provider.complete` with a counter; call `gateway.complete` 10 more times → assert counter increments by 0 (no retry).
- `tests/test_gateway_fallback.py`
  - Primary fail_rate=1.0, backup fail_rate=0.0.
  - Drive enough requests to open primary breaker, then assert `result.route == "fallback"` and `result.provider == "backup"`.
- `tests/test_cache_guardrails.py`
  - `_is_uncacheable("account balance for user 123")` → True.
  - `ResponseCache.set("balance for user 123", "x")` then `get(...)` → returns None (skipped).
  - Cross-year false-hit test mirrors xfail (no xfail marker).
- `tests/test_chaos_scenarios.py`
  - Build a minimal `LabConfig` with only the `primary_timeout_100` scenario and call `run_simulation` → assert resulting `metrics.scenarios["primary_timeout_100"] == "pass"` and `metrics.fallback_successes > 0`.
  - Build a minimal `LabConfig` with only the `all_healthy` scenario (both providers fail_rate=0) and call `run_simulation` → assert `metrics.scenarios["all_healthy"] == "pass"` and `metrics.circuit_open_count == 0`.

### 4.3 Lint / typecheck / coverage

- `make lint` clean (ruff).
- `make typecheck` clean under `mypy strict`.
- `make test` zero failures (xfail/xpass acceptable but we eliminate xfail entirely).

## 5. Final deliverables checklist

- [ ] All TODOs in `src/reliability_lab/*.py` removed.
- [ ] `reports/metrics.json` regenerated by `make run-chaos`, contains:
  - `total_requests, availability, error_rate, latency_p50_ms, latency_p95_ms, latency_p99_ms, fallback_success_rate, cache_hit_rate, circuit_open_count, recovery_time_ms, estimated_cost, estimated_cost_saved, scenarios{4 entries pass/fail}, cache_comparison, false_hit_examples, slo_targets`.
- [ ] `reports/final_report.md` filled, all 9 sections populated with real numbers (no `TODO` strings).
- [ ] `tests/` — `make test` shows green (or xpassed if we forget to strip a marker).
- [ ] `docker-compose.yml` unchanged, Redis tests pass.
- [ ] Code: type hints everywhere, ruff clean, mypy strict clean.

## 6. Build sequence (TDD-first)

1. Write the new failing tests (`test_circuit_breaker_no_retry_storm`, `test_gateway_fallback`, `test_cache_guardrails`). Confirm they fail.
2. Tighten `circuit_breaker.py` — first two tests start passing.
3. Implement `ResponseCache` similarity + guardrails + false-hit log — `test_cache_guardrails` and de-xfail `test_todo_requirements.py` pass.
4. Update `gateway.py` (`route_reason`, latency wrap, cache try/except, richer fallback cause). Existing contract test + new fallback test pass.
5. Implement `SharedRedisCache.get/set` with graceful degradation. Start Redis (`make docker-up`). All 6 Redis tests pass.
6. Extend `chaos.py` (4 scenarios with pass/fail predicates, cache comparison helper, concurrency). Add `test_chaos_scenarios.py`.
7. Extend `metrics.py` (new fields, CSV writer) and `config.py` (`concurrency`).
8. Rewrite `scripts/generate_report.py` for 9-section template.
9. `make docker-up && make run-chaos && make report`. Read generated report; fix any placeholders.
10. `make lint && make typecheck && make test`. Iterate until clean.
11. Commit.

## 7. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Redis container not running on grader machine | `make docker-up` documented; `SharedRedisCache` degrades gracefully (returns miss) instead of crashing |
| Concurrency race on `ResponseCache._entries` | Wrap reads/writes in a `threading.Lock`; tests still run sequentially by default |
| Recovery time = `None` when no breaker re-closes | Document in report under failure analysis; `primary_flaky_50` is the scenario that produces a real recovery number. Per-scenario recovery times are also recorded inside `metrics.scenarios` reasoning column or a parallel dict so a `None` from one scenario does not erase a real number from another |
| Trigram similarity not enough to suppress year-mismatch | `_looks_like_false_hit` is the actual gate, not the score |
| `xfail` test marked strict elsewhere | None set in `pyproject.toml`; safe. Still remove marker once test passes for clarity |

## 8. Out of scope

- Real LLM API integration (FakeLLMProvider only, per lab).
- Redis-backed circuit state across instances (stretch goal we explicitly skip to bound scope).
- Prometheus exporter (skipped; not in target rubric).
- Property-based tests (skipped).
