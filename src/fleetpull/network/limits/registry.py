# src/fleetpull/network/limits/registry.py
"""Process-wide map from quota-scope strings to their limiters, and the
derivation of its per-scope values from provider configs."""

import threading
from collections.abc import Iterable, Mapping

from fleetpull.config import GeotabConfig, ProviderConfig, RateLimitConfig
from fleetpull.exceptions import UnknownQuotaScopeError
from fleetpull.network.limits.limiter import QuotaScopeLimiter
from fleetpull.timing import Clock, SystemClock
from fleetpull.vocabulary import QuotaScope

__all__: list[str] = ['RateLimiterRegistry', 'rate_limits_from_configs']


def rate_limits_from_configs(
    provider_configs: Iterable[ProviderConfig],
) -> dict[str, RateLimitConfig]:
    """Derive the registry's per-scope rate limits from provider configs.

    Each provider config carries its scope's budget (``rate_limit``, with a
    documented provider default) and binds the scope it governs
    (``quota_scope``, a ``ClassVar``), so composition roots hand this their
    resolved configs and never invent rate-limit numbers. GeoTab meters per
    method class (DESIGN §8): its ``quota_scope`` ClassVar binds the
    Get-class scope the generic emission covers, and its second budget --
    the dedicated Authenticate class -- is emitted here under
    ``QuotaScope.GEOTAB_AUTHENTICATE`` from the same config, so the
    authenticator's scope is registered wherever a ``GeotabConfig`` is.

    Args:
        provider_configs: The resolved provider configs for this run -- the
            same instances handed to ``build_endpoint_registry``.

    Returns:
        The quota-scope-keyed map ``RateLimiterRegistry`` is constructed on.
    """
    configs = list(provider_configs)  # the Iterable may be one-shot
    rate_limits = {config.quota_scope.value: config.rate_limit for config in configs}
    for config in configs:
        if isinstance(config, GeotabConfig):
            rate_limits[QuotaScope.GEOTAB_AUTHENTICATE.value] = (
                config.authenticate_rate_limit
            )
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
