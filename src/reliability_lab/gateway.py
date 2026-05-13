from __future__ import annotations

import time
from dataclasses import dataclass

from reliability_lab.cache import ResponseCache, SharedRedisCache
from reliability_lab.circuit_breaker import CircuitBreaker
from reliability_lab.providers import FakeLLMProvider, ProviderError, ProviderResponse


@dataclass(slots=True)
class GatewayResponse:
    text: str
    route: str
    provider: str | None
    cache_hit: bool
    latency_ms: float
    estimated_cost: float
    error: str | None = None
    route_reason: str = ""


class ReliabilityGateway:
    """Routes requests through cache, circuit breakers, and a fallback chain."""

    def __init__(
        self,
        providers: list[FakeLLMProvider],
        breakers: dict[str, CircuitBreaker],
        cache: ResponseCache | SharedRedisCache | None = None,
    ):
        self.providers = providers
        self.breakers = breakers
        self.cache = cache

    def complete(self, prompt: str) -> GatewayResponse:
        t0 = time.perf_counter()

        if self.cache is not None:
            try:
                cached, score = self.cache.get(prompt)
            except Exception:
                cached, score = None, 0.0
            if cached is not None:
                reason = "cache_hit:exact" if score >= 0.999 else f"cache_hit:semantic:{score:.2f}"
                return GatewayResponse(
                    text=cached,
                    route="cache_hit",
                    provider=None,
                    cache_hit=True,
                    latency_ms=(time.perf_counter() - t0) * 1000,
                    estimated_cost=0.0,
                    route_reason=reason,
                )

        causes: list[str] = []
        last_error: str | None = None
        for index, provider in enumerate(self.providers):
            breaker = self.breakers[provider.name]
            if not breaker.allow_request():
                causes.append(f"skip:circuit_open:{provider.name}")
                continue
            try:
                response: ProviderResponse = provider.complete(prompt)
                breaker.record_success()
            except ProviderError as exc:
                breaker.record_failure()
                last_error = f"{provider.name}:{exc}"
                causes.append(f"fail:{provider.name}")
                continue

            if self.cache is not None:
                try:
                    self.cache.set(prompt, response.text, {"provider": provider.name})
                except Exception:
                    pass

            if index == 0:
                route = "primary"
                reason = f"primary:{provider.name}"
            else:
                route = "fallback"
                cause = causes[-1] if causes else "primary_unavailable"
                reason = f"fallback:{provider.name}:{cause}"

            return GatewayResponse(
                text=response.text,
                route=route,
                provider=provider.name,
                cache_hit=False,
                latency_ms=(time.perf_counter() - t0) * 1000,
                estimated_cost=response.estimated_cost,
                route_reason=reason,
            )

        cause = ",".join(causes) if causes else "no_providers"
        return GatewayResponse(
            text="The service is temporarily degraded. Please try again soon.",
            route="static_fallback",
            provider=None,
            cache_hit=False,
            latency_ms=(time.perf_counter() - t0) * 1000,
            estimated_cost=0.0,
            error=last_error,
            route_reason=f"static_fallback:{cause}",
        )
