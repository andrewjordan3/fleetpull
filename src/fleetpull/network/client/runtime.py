# src/fleetpull/network/client/runtime.py
"""Process-global transport infrastructure, shared by every provider's client."""

import random
from dataclasses import dataclass, field

from fleetpull.config import HttpConfig, RetryConfig
from fleetpull.network.limits import RateLimiterRegistry
from fleetpull.network.retry import RandomFractionGenerator
from fleetpull.timing import Sleeper, SystemSleeper

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
        random_source: Jitter seam for retry backoff. Defaults to the
            production ``random.Random``; injectable so tests make every
            jittered delay exact arithmetic. No composition root needs to
            know the seam exists.
        sleeper: Sleep seam for TRANSIENT backoff. Defaults to the
            production ``SystemSleeper``; injectable so tests record delays
            instead of waiting. Same stance as ``random_source``.
    """

    http_config: HttpConfig
    retry_config: RetryConfig
    limiter_registry: RateLimiterRegistry
    random_source: RandomFractionGenerator = field(default_factory=random.Random)
    sleeper: Sleeper = field(default_factory=SystemSleeper)
