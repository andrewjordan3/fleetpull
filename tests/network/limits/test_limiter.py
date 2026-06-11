"""Tests for fleetpull.network.limits.limiter.

Two strictly separated styles:

- Deterministic tests use FrozenClock on a single thread and only exercise
  paths where ``request_slot()`` succeeds without waiting, advancing the
  clock BETWEEN calls. ``Condition.wait`` sleeps REAL time regardless of the
  injected clock, so a blocking path under FrozenClock deadlocks forever.
- Concurrency tests use the real SystemClock, real threads, and tiny real
  waits with generous upper-bound assertions.
"""

import threading
import time
from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.network.limits.config import RateLimitConfig
from fleetpull.network.limits.limiter import QuotaScopeLimiter
from fleetpull.timing.clock import FrozenClock

__all__: list[str] = []

FROZEN_START_TIME: datetime = datetime(2026, 1, 1, tzinfo=UTC)
FROZEN_START_MONOTONIC: float = 1000.0


def build_frozen_clock() -> FrozenClock:
    return FrozenClock(
        start_time_utc=FROZEN_START_TIME,
        start_monotonic_seconds=FROZEN_START_MONOTONIC,
    )


def build_limiter(
    clock: FrozenClock, *, burst: int = 3, max_concurrency: int = 4
) -> QuotaScopeLimiter:
    # requests_per_period=1 over period_seconds=1.0 gives a refill rate of
    # exactly 1 token/second, keeping the deterministic arithmetic obvious.
    config = RateLimitConfig(
        requests_per_period=1,
        period_seconds=1.0,
        burst=burst,
        max_concurrency=max_concurrency,
    )
    return QuotaScopeLimiter('test-scope', config, clock)


class TestQuotaScopeLimiterDeterministic:
    def test_cold_start_allows_exactly_burst_requests(self) -> None:
        frozen_clock = build_frozen_clock()
        limiter = build_limiter(frozen_clock, burst=3)
        for _attempt in range(3):
            with limiter.request_slot():
                pass
        assert limiter._tokens == pytest.approx(0.0)

    def test_advancing_clock_refills_and_next_request_succeeds(self) -> None:
        frozen_clock = build_frozen_clock()
        limiter = build_limiter(frozen_clock, burst=3)
        for _attempt in range(3):
            with limiter.request_slot():
                pass
        # 2.5 seconds at 1 token/sec refills 2.5 tokens; one request
        # consumes 1, leaving 1.5.
        frozen_clock.advance(timedelta(seconds=2.5))
        with limiter.request_slot():
            pass
        assert limiter._tokens == pytest.approx(1.5)

    def test_refill_never_exceeds_capacity(self) -> None:
        frozen_clock = build_frozen_clock()
        limiter = build_limiter(frozen_clock, burst=3)
        for _attempt in range(3):
            with limiter.request_slot():
                pass
        frozen_clock.advance(timedelta(seconds=10_000))
        with limiter.request_slot():
            pass
        # Capped at burst=3 despite the huge advance, minus the 1 consumed.
        assert limiter._tokens == pytest.approx(2.0)

    def test_penalty_is_max_merged_never_overwritten(self) -> None:
        frozen_clock = build_frozen_clock()
        limiter = build_limiter(frozen_clock)
        limiter.penalize(10.0)
        limiter.penalize(5.0)
        assert limiter._pause_until == pytest.approx(FROZEN_START_MONOTONIC + 10.0)

    def test_request_succeeds_after_penalty_expires(self) -> None:
        frozen_clock = build_frozen_clock()
        limiter = build_limiter(frozen_clock, burst=3)
        limiter.penalize(4.0)
        frozen_clock.advance(timedelta(seconds=4.1))
        with limiter.request_slot():
            pass
        assert limiter._tokens == pytest.approx(2.0)

    @pytest.mark.parametrize('invalid_seconds', [0.0, -1.0])
    def test_penalize_rejects_non_positive_seconds(
        self, invalid_seconds: float
    ) -> None:
        limiter = build_limiter(build_frozen_clock())
        with pytest.raises(ValueError, match='positive'):
            limiter.penalize(invalid_seconds)

    def test_exception_inside_slot_releases_semaphore(self) -> None:
        frozen_clock = build_frozen_clock()
        limiter = build_limiter(frozen_clock, burst=3, max_concurrency=1)
        with pytest.raises(RuntimeError, match='boom'), limiter.request_slot():
            raise RuntimeError('boom')
        # With max_concurrency=1, a leaked slot would block here forever.
        with limiter.request_slot():
            pass
        assert limiter._tokens == pytest.approx(1.0)


class _ConcurrencyTracker:
    """Lock-protected counter of concurrent slot holders and their peak."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.active_count = 0
        self.peak_count = 0

    def enter(self) -> None:
        with self.lock:
            self.active_count += 1
            self.peak_count = max(self.peak_count, self.active_count)

    def leave(self) -> None:
        with self.lock:
            self.active_count -= 1


class TestQuotaScopeLimiterConcurrency:
    def test_in_flight_cap_holds_under_contention(self) -> None:
        config = RateLimitConfig(
            requests_per_period=1000,
            period_seconds=1.0,
            burst=100,
            max_concurrency=2,
        )
        limiter = QuotaScopeLimiter('concurrency-scope', config)
        tracker = _ConcurrencyTracker()

        def hold_slot_briefly() -> None:
            with limiter.request_slot():
                tracker.enter()
                time.sleep(0.05)
                tracker.leave()

        worker_threads: list[threading.Thread] = [
            threading.Thread(target=hold_slot_briefly) for _ in range(6)
        ]
        for worker_thread in worker_threads:
            worker_thread.start()
        for worker_thread in worker_threads:
            worker_thread.join()

        assert tracker.peak_count == 2

    def test_penalty_wakes_waiters_after_expiry(self) -> None:
        config = RateLimitConfig(
            requests_per_period=1000,
            period_seconds=1.0,
            burst=100,
            max_concurrency=4,
        )
        limiter = QuotaScopeLimiter('penalty-scope', config)
        penalty_seconds = 0.3

        def make_request() -> None:
            with limiter.request_slot():
                pass

        start_monotonic: float = time.monotonic()
        limiter.penalize(penalty_seconds)
        worker_threads: list[threading.Thread] = [
            threading.Thread(target=make_request) for _ in range(4)
        ]
        for worker_thread in worker_threads:
            worker_thread.start()
        for worker_thread in worker_threads:
            worker_thread.join()
        elapsed_seconds: float = time.monotonic() - start_monotonic

        # Generous ceiling: tight timing assertions flake under load.
        assert penalty_seconds <= elapsed_seconds < 2.0
