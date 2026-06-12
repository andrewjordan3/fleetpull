# src/fleetpull/network/limits/registry.py
"""Process-wide map from quota-scope strings to their limiters."""

import threading
from collections.abc import Mapping

from fleetpull.exceptions import UnknownQuotaScopeError
from fleetpull.network.limits.config import RateLimitConfig
from fleetpull.network.limits.limiter import QuotaScopeLimiter
from fleetpull.timing.clock import Clock, SystemClock

__all__: list[str] = ['RateLimiterRegistry']


class RateLimiterRegistry:
    """Get-or-create registry of one ``QuotaScopeLimiter`` per quota scope.

    The same scope string always returns the SAME limiter instance — two
    limiters for one scope would silently double the request budget.

    This registry is constructed by the application's composition root and
    passed down to whatever needs it. It is NOT a module-level singleton.
    """

    def __init__(
        self,
        rate_limits: Mapping[str, RateLimitConfig],
        clock: Clock = SystemClock(),  # noqa: B008 — SystemClock is frozen and stateless; one shared default instance is intentional
    ) -> None:
        """Initialize the registry with the configured quota scopes.

        Args:
            rate_limits: Map of quota-scope string to its rate-limit config.
            clock: Time source passed to every limiter this registry creates.
        """
        self._rate_limits: dict[str, RateLimitConfig] = dict(rate_limits)
        self._clock: Clock = clock
        self._limiters: dict[str, QuotaScopeLimiter] = {}
        self._lock: threading.Lock = threading.Lock()

    def get(self, quota_scope: str) -> QuotaScopeLimiter:
        """Return the limiter for a scope, creating it on first request.

        The check and the create both happen under the lock — losing the
        check-then-act race would mean two limiters for one scope.

        Args:
            quota_scope: Scope name declared by an endpoint definition.

        Returns:
            The single ``QuotaScopeLimiter`` instance for this scope.

        Raises:
            UnknownQuotaScopeError: If the scope has no configured rate
                limits.
        """
        with self._lock:
            existing_limiter: QuotaScopeLimiter | None = self._limiters.get(quota_scope)
            if existing_limiter is not None:
                return existing_limiter
            scope_config: RateLimitConfig | None = self._rate_limits.get(quota_scope)
            if scope_config is None:
                configured_scopes: str = ', '.join(sorted(self._rate_limits)) or 'none'
                raise UnknownQuotaScopeError(
                    quota_scope,
                    detail=f'configured scopes: {configured_scopes}',
                )
            new_limiter = QuotaScopeLimiter(quota_scope, scope_config, self._clock)
            self._limiters[quota_scope] = new_limiter
            return new_limiter
