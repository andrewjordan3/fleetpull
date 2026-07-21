# src/fleetpull/network/limits/registry.py
"""Process-wide map from quota-scope strings to their limiters, and the
derivation of its per-scope values from provider configs."""

import threading
from collections.abc import Iterable, Mapping

from fleetpull.config import ProviderConfig, RateLimitConfig
from fleetpull.exceptions import UnknownQuotaScopeError
from fleetpull.network.limits.limiter import QuotaScopeLimiter
from fleetpull.timing import Clock, SystemClock

__all__: list[str] = ['RateLimiterRegistry', 'rate_limits_from_configs']


def rate_limits_from_configs(
    provider_configs: Iterable[ProviderConfig],
) -> dict[str, RateLimitConfig]:
    """Derive the registry's per-scope rate limits from provider configs.

    Each provider config emits every scope its budgets govern
    (``ProviderConfig.scope_rate_limits``: the one bound ``quota_scope`` in
    the base, plus a multi-method-class provider's extra scopes -- GeoTab's
    feed and authenticate classes -- in its override), so composition roots
    hand this their resolved configs and never invent rate-limit numbers,
    and no provider is special-cased here.

    Args:
        provider_configs: The resolved provider configs for this run -- the
            same instances handed to ``build_endpoint_registry``.

    Returns:
        The quota-scope-keyed map ``RateLimiterRegistry`` is constructed on.
    """
    rate_limits: dict[str, RateLimitConfig] = {}
    for config in provider_configs:
        rate_limits.update(config.scope_rate_limits())
    return rate_limits


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
