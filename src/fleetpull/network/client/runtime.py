# src/fleetpull/network/client/runtime.py
"""Process-global transport infrastructure, shared by every provider's client."""

from dataclasses import dataclass

from fleetpull.config import HttpConfig, RetryConfig
from fleetpull.network.limits import RateLimiterRegistry
from fleetpull.network.retry import RandomFractionGenerator
from fleetpull.timing import Sleeper

__all__: list[str] = ['ClientRuntime']


@dataclass(frozen=True, slots=True)
class ClientRuntime:
    """
    The process-global dependencies a transport client runs against.

    Built once at the composition root and shared across every per-provider
    client — only the ``ProviderProfile`` varies per provider. Bundled
    because these always travel together at the one site that constructs
    clients, and no module-level singletons are permitted.

    Attributes:
        http_config: Connect/read timeouts and TLS posture (consumed once to
            build the client's connection pool).
        retry_config: Per-category failure budgets, backoff shape, fallback
            penalty.
        limiter_registry: Shared rate limiters, keyed by quota scope.
        random_source: Injected jitter seam for retry backoff.
        sleeper: Injected sleep seam for TRANSIENT backoff.
    """

    http_config: HttpConfig
    retry_config: RetryConfig
    limiter_registry: RateLimiterRegistry
    random_source: RandomFractionGenerator
    sleeper: Sleeper
