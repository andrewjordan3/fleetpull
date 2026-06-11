"""Tests for fleetpull.network.limits.registry."""

import threading

import pytest

from fleetpull.network.limits.config import RateLimitConfig
from fleetpull.network.limits.limiter import QuotaScopeLimiter
from fleetpull.network.limits.registry import (
    RateLimiterRegistry,
    UnknownQuotaScopeError,
)

__all__: list[str] = []


def build_config() -> RateLimitConfig:
    return RateLimitConfig(
        requests_per_period=100, period_seconds=60.0, burst=20, max_concurrency=5
    )


@pytest.fixture
def registry() -> RateLimiterRegistry:
    return RateLimiterRegistry({'motive': build_config(), 'samsara': build_config()})


class TestRateLimiterRegistry:
    def test_same_scope_returns_identical_instance(
        self, registry: RateLimiterRegistry
    ) -> None:
        assert registry.get('motive') is registry.get('motive')

    def test_distinct_scopes_return_distinct_instances(
        self, registry: RateLimiterRegistry
    ) -> None:
        assert registry.get('motive') is not registry.get('samsara')

    def test_unknown_scope_raises_naming_the_scope(
        self, registry: RateLimiterRegistry
    ) -> None:
        with pytest.raises(UnknownQuotaScopeError, match='geotab'):
            registry.get('geotab')

    def test_concurrent_get_creates_exactly_one_instance(
        self, registry: RateLimiterRegistry
    ) -> None:
        thread_count = 16
        start_barrier = threading.Barrier(thread_count)
        results_lock = threading.Lock()
        fetched_limiters: list[QuotaScopeLimiter] = []

        def fetch_limiter() -> None:
            start_barrier.wait()
            limiter = registry.get('motive')
            with results_lock:
                fetched_limiters.append(limiter)

        worker_threads: list[threading.Thread] = [
            threading.Thread(target=fetch_limiter) for _ in range(thread_count)
        ]
        for worker_thread in worker_threads:
            worker_thread.start()
        for worker_thread in worker_threads:
            worker_thread.join()

        assert len(fetched_limiters) == thread_count
        distinct_instance_ids: set[int] = {id(limiter) for limiter in fetched_limiters}
        assert len(distinct_instance_ids) == 1
