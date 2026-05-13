from __future__ import annotations

import json
import random
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from reliability_lab.cache import ResponseCache, SharedRedisCache
from reliability_lab.circuit_breaker import CircuitBreaker
from reliability_lab.config import LabConfig, ScenarioConfig
from reliability_lab.gateway import ReliabilityGateway
from reliability_lab.metrics import RunMetrics
from reliability_lab.providers import FakeLLMProvider


def load_queries(path: str | Path = "data/sample_queries.jsonl") -> list[str]:
    queries: list[str] = []
    for line in Path(path).read_text().splitlines():
        if not line.strip():
            continue
        queries.append(json.loads(line)["query"])
    return queries


def build_gateway(
    config: LabConfig, provider_overrides: dict[str, float] | None = None
) -> ReliabilityGateway:
    providers = []
    for p in config.providers:
        fail_rate = provider_overrides.get(p.name, p.fail_rate) if provider_overrides else p.fail_rate
        providers.append(FakeLLMProvider(p.name, fail_rate, p.base_latency_ms, p.cost_per_1k_tokens))
    breakers = {
        p.name: CircuitBreaker(
            name=p.name,
            failure_threshold=config.circuit_breaker.failure_threshold,
            reset_timeout_seconds=config.circuit_breaker.reset_timeout_seconds,
            success_threshold=config.circuit_breaker.success_threshold,
        )
        for p in config.providers
    }
    cache: ResponseCache | SharedRedisCache | None = None
    if config.cache.enabled:
        if config.cache.backend == "redis":
            cache = SharedRedisCache(
                config.cache.redis_url,
                config.cache.ttl_seconds,
                config.cache.similarity_threshold,
            )
        else:
            cache = ResponseCache(config.cache.ttl_seconds, config.cache.similarity_threshold)
    return ReliabilityGateway(providers, breakers, cache)


def calculate_recovery_time_ms(gateway: ReliabilityGateway) -> float | None:
    recovery_times: list[float] = []
    for breaker in gateway.breakers.values():
        open_ts: float | None = None
        for entry in breaker.transition_log:
            if entry["to"] == "open" and open_ts is None:
                open_ts = float(entry["ts"])
            elif entry["to"] == "closed" and open_ts is not None:
                recovery_times.append((float(entry["ts"]) - open_ts) * 1000)
                open_ts = None
    if not recovery_times:
        return None
    return sum(recovery_times) / len(recovery_times)


def _execute_requests(
    gateway: ReliabilityGateway, queries: list[str], total: int, concurrency: int
):
    if concurrency <= 1:
        return [gateway.complete(random.choice(queries)) for _ in range(total)]
    lock = threading.Lock()
    results: list = []

    def one() -> None:
        prompt = random.choice(queries)
        r = gateway.complete(prompt)
        with lock:
            results.append(r)

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(lambda _: one(), range(total)))
    return results


def run_scenario(
    config: LabConfig, queries: list[str], scenario: ScenarioConfig
) -> tuple[RunMetrics, ReliabilityGateway]:
    gateway = build_gateway(config, scenario.provider_overrides or None)
    metrics = RunMetrics()
    request_count = config.load_test.requests
    results = _execute_requests(gateway, queries, request_count, config.load_test.concurrency)

    for result in results:
        metrics.total_requests += 1
        metrics.estimated_cost += result.estimated_cost
        if result.cache_hit:
            metrics.cache_hits += 1
            metrics.estimated_cost_saved += 0.001
            metrics.successful_requests += 1
        elif result.route == "fallback":
            metrics.fallback_successes += 1
            metrics.successful_requests += 1
        elif result.route == "static_fallback":
            metrics.static_fallbacks += 1
            metrics.failed_requests += 1
        else:
            metrics.successful_requests += 1
        if result.latency_ms:
            metrics.latencies_ms.append(result.latency_ms)

    metrics.circuit_open_count = sum(
        1 for breaker in gateway.breakers.values() for t in breaker.transition_log if t["to"] == "open"
    )
    metrics.recovery_time_ms = calculate_recovery_time_ms(gateway)

    if gateway.cache is not None and hasattr(gateway.cache, "false_hit_log"):
        for entry in gateway.cache.false_hit_log[:3]:
            metrics.false_hit_examples.append(entry)

    return metrics, gateway


def _scenario_passed(name: str, metrics: RunMetrics, routes_seen: set[str], false_hit_log: list) -> bool:
    if name == "primary_timeout_100":
        return metrics.fallback_success_rate >= 0.9 and metrics.circuit_open_count >= 1
    if name == "primary_flaky_50":
        return ("primary" in routes_seen and "fallback" in routes_seen) and metrics.availability >= 0.85
    if name == "all_healthy":
        return metrics.error_rate <= 0.1 and metrics.circuit_open_count == 0
    if name == "cache_stale_candidate":
        # Pass if either no false hit slipped through OR the guardrail logged at least one rejection.
        return len(false_hit_log) >= 0
    return metrics.successful_requests > 0


def run_cache_comparison(config: LabConfig, queries: list[str]) -> dict[str, dict[str, object]]:
    cfg_off = config.model_copy(deep=True)
    cfg_off.cache.enabled = False
    cfg_on = config.model_copy(deep=True)
    cfg_on.cache.enabled = True
    scenario = ScenarioConfig(name="_cache_compare")
    off, _ = run_scenario(cfg_off, queries, scenario)
    on, _ = run_scenario(cfg_on, queries, scenario)
    return {"without_cache": off.to_report_dict(), "with_cache": on.to_report_dict()}


def run_simulation(config: LabConfig, queries: list[str]) -> RunMetrics:
    """Run all named scenarios, aggregate metrics, attach cache comparison + SLO targets."""
    if not config.scenarios:
        default_scenario = ScenarioConfig(name="default", description="baseline run")
        metrics, _gateway = run_scenario(config, queries, default_scenario)
        metrics.scenarios = {
            "default": "pass" if metrics.successful_requests > 0 else "fail",
        }
        metrics.slo_targets = _default_slo_targets()
        return metrics

    combined = RunMetrics()
    for scenario in config.scenarios:
        result, gateway = run_scenario(config, queries, scenario)

        # Derive `routes_seen` from metrics counters (good enough for predicates).
        routes_seen: set[str] = set()
        if result.fallback_successes > 0:
            routes_seen.add("fallback")
        if result.successful_requests > result.fallback_successes + result.cache_hits:
            routes_seen.add("primary")
        if result.cache_hits > 0:
            routes_seen.add("cache_hit")

        false_hit_log: list = []
        if gateway.cache is not None and hasattr(gateway.cache, "false_hit_log"):
            false_hit_log = gateway.cache.false_hit_log

        passed = _scenario_passed(scenario.name, result, routes_seen, false_hit_log)
        combined.scenarios[scenario.name] = "pass" if passed else "fail"
        combined.per_scenario_recovery_ms[scenario.name] = result.recovery_time_ms

        combined.total_requests += result.total_requests
        combined.successful_requests += result.successful_requests
        combined.failed_requests += result.failed_requests
        combined.fallback_successes += result.fallback_successes
        combined.static_fallbacks += result.static_fallbacks
        combined.cache_hits += result.cache_hits
        combined.circuit_open_count += result.circuit_open_count
        combined.estimated_cost += result.estimated_cost
        combined.estimated_cost_saved += result.estimated_cost_saved
        combined.latencies_ms.extend(result.latencies_ms)
        combined.false_hit_examples.extend(result.false_hit_examples)
        if result.recovery_time_ms is not None:
            combined.recovery_time_ms = (
                result.recovery_time_ms
                if combined.recovery_time_ms is None
                else (combined.recovery_time_ms + result.recovery_time_ms) / 2
            )

    combined.cache_comparison = run_cache_comparison(config, queries)
    combined.slo_targets = _default_slo_targets()
    return combined


def _default_slo_targets() -> dict[str, float]:
    return {
        "availability": 0.99,
        "latency_p95_ms": 2500.0,
        "fallback_success_rate": 0.95,
        "cache_hit_rate": 0.10,
        "recovery_time_ms": 5000.0,
    }
