# src/fleetpull/network/limits/limiter.py
"""Per-quota-scope rate limiter: token bucket + in-flight semaphore.

One ``QuotaScopeLimiter`` instance exists per quota scope, shared by every
thread in the process that talks to that scope. All time reads flow through
the injected ``Clock`` — never a direct ``time.*`` call — which is what makes
the deterministic single-threaded tests possible.
"""

import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager

from fleetpull.network.limits.bucket_math import refill_tokens, seconds_until_available
from fleetpull.network.limits.config import RateLimitConfig
from fleetpull.timing.clock import Clock, SystemClock

__all__: list[str] = ['QuotaScopeLimiter']

logger = logging.getLogger(__name__)


class QuotaScopeLimiter:
    """Token-bucket rate limiter plus concurrency cap for one quota scope.

    The bucket starts FULL at ``config.burst`` tokens (burst is the bucket
    capacity), refills lazily at ``config.refill_rate_per_second``, and a
    ``BoundedSemaphore`` caps requests in flight at ``config.max_concurrency``.
    A 429 / Retry-After penalty pauses the whole scope via :meth:`penalize`.

    Caller contracts (this object cannot enforce them):
        - Every HTTP attempt consumes one slot. Retries re-acquire; every
          page is an attempt.
        - ``request_slot()`` wraps exactly one HTTP attempt — never a
          pagination loop, never a retry loop.
    """

    def __init__(
        self,
        quota_scope: str,
        config: RateLimitConfig,
        clock: Clock = SystemClock(),  # noqa: B008 — SystemClock is frozen and stateless; one shared default instance is intentional
    ) -> None:
        """Initialize a limiter with a full bucket and no penalty.

        Args:
            quota_scope: Scope name, used for log context (a penalty WARNING
                that cannot say which scope was penalized is useless).
            config: Token-bucket and concurrency settings for this scope.
            clock: Time source; injected so tests can be deterministic.
        """
        self._quota_scope: str = quota_scope
        self._config: RateLimitConfig = config
        self._clock: Clock = clock
        self._condition: threading.Condition = threading.Condition()
        self._tokens: float = float(config.burst)
        self._last_refill_monotonic: float = clock.monotonic_seconds()
        self._pause_until: float = 0.0
        # Bounded specifically: a double-release bug must raise, not silently
        # inflate the concurrency budget.
        self._in_flight_semaphore: threading.BoundedSemaphore = (
            threading.BoundedSemaphore(config.max_concurrency)
        )

    @contextmanager
    def request_slot(self) -> Iterator[None]:
        """Block until one HTTP attempt may start, then hold its in-flight slot.

        Acquires the concurrency semaphore first, then waits out any scope
        penalty, then consumes one bucket token. Semaphore-first ordering is
        deliberate: taking a token and then blocking on concurrency wastes
        start-rate budget while starting nothing, while holding an idle
        concurrency slot costs the provider nothing.

        Yields:
            None. The caller performs its single HTTP attempt inside the
            ``with`` block.

        Side Effects:
            Blocks the calling thread; consumes one token; holds one
            in-flight slot for the duration of the block. The slot is
            released even if the block raises.
        """
        self._in_flight_semaphore.acquire()
        try:
            self._consume_token()
            yield
        finally:
            self._in_flight_semaphore.release()

    def penalize(self, seconds: float) -> None:
        """Pause the entire quota scope, max-merged with any existing pause.

        Called on 429 / Retry-After. The new pause is
        ``max(pause_until, now + seconds)`` — never overwritten, so concurrent
        429s cannot shrink an existing penalty. All waiters are woken so they
        recompute against the new penalty instead of firing the moment their
        token math says go.

        Args:
            seconds: Pause duration (must be > 0). Clamping a zero or
                negative Retry-After to something sane is the caller's job.

        Raises:
            ValueError: If ``seconds`` is not positive.

        Side Effects:
            Extends the scope-wide pause; wakes all waiting threads; logs a
            WARNING naming the scope and the effective pause duration.
        """
        if seconds <= 0:
            raise ValueError(f'penalty seconds must be positive, got {seconds}.')
        with self._condition:
            now: float = self._clock.monotonic_seconds()
            self._pause_until = max(self._pause_until, now + seconds)
            effective_pause_seconds: float = self._pause_until - now
            self._condition.notify_all()
        logger.warning(
            'Quota scope %r penalized; paused for %.2f seconds.',
            self._quota_scope,
            effective_pause_seconds,
        )

    def _consume_token(self) -> None:
        """Wait out any penalty, refill the bucket, and consume one token.

        The penalty check comes BEFORE the token check on every iteration:
        no token may be consumed while the scope is paused. Every wake
        recomputes from scratch — never assume why you woke.
        """
        with self._condition:
            while True:
                now: float = self._clock.monotonic_seconds()
                if now < self._pause_until:
                    self._condition.wait(timeout=self._pause_until - now)
                    continue
                self._tokens = refill_tokens(
                    current_tokens=self._tokens,
                    elapsed_seconds=now - self._last_refill_monotonic,
                    refill_rate_per_second=self._config.refill_rate_per_second,
                    capacity=float(self._config.burst),
                )
                self._last_refill_monotonic = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                self._condition.wait(
                    timeout=seconds_until_available(
                        current_tokens=self._tokens,
                        refill_rate_per_second=self._config.refill_rate_per_second,
                    )
                )
