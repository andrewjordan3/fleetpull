# src/fleetpull/config/provider.py
"""The shared provider-config base: ``ProviderConfig``.

The base every per-provider config (``MotiveConfig`` and the Samsara /
GeoTab configs as they land) inherits. It carries the configuration-model
policy each must follow -- frozen, ``extra='forbid'``, validate-default --
so the policy lives in one place rather than being restated per provider.
It also names the concept the endpoint catalog's builder consumes: a
provider's config. A leaf endpoint factory annotates its concrete subclass
(``MotiveConfig``), and ``build_endpoint_registry`` injects the matching
instance by exact type; the base anchors that contract's type.

The base additionally declares the rate-limit contract every provider
config satisfies: ``quota_scope`` (the scope its budget governs -- a code
fact, so a ``ClassVar`` the subclass binds, never a YAML field) and
``rate_limit`` (the scope's budget, a YAML-facing field each provider
defaults from its published limits). ``rate_limits_from_configs``
(``network/limits/``) derives the limiter registry's per-scope map from
these, so no composition root invents rate-limit numbers.
"""

from typing import ClassVar

from pydantic import BaseModel, ConfigDict

from fleetpull.config.rate_limit import RateLimitConfig
from fleetpull.vocabulary import QuotaScope

__all__: list[str] = ['ProviderConfig']


class ProviderConfig(BaseModel):
    """Base for per-provider configuration models.

    Subclassed once per provider (``MotiveConfig``, ...). Carries the shared
    model policy -- frozen so a loaded config cannot mutate mid-run,
    ``extra='forbid'`` so a misspelled YAML key is rejected rather than
    silently dropped, and ``validate_default`` so defaulted values pass the
    same validators as supplied ones -- plus the rate-limit contract:
    each subclass binds its ``quota_scope`` and defaults its ``rate_limit``.

    Attributes:
        quota_scope: The quota scope this provider's budget governs. A
            ``ClassVar`` bound by each subclass -- provider identity is a
            code fact, not user configuration; a subclass that forgets to
            bind it fails loudly on first attribute access.
        rate_limit: The token-bucket budget for this provider's scope.
            Each subclass supplies its own documented default.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', validate_default=True)

    quota_scope: ClassVar[QuotaScope]

    rate_limit: RateLimitConfig
