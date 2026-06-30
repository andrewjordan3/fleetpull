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
"""

from pydantic import BaseModel, ConfigDict

__all__: list[str] = ['ProviderConfig']


class ProviderConfig(BaseModel):
    """Base for per-provider configuration models.

    Subclassed once per provider (``MotiveConfig``, ...). Carries no fields
    itself -- each provider names its own -- only the shared model policy:
    frozen so a loaded config cannot mutate mid-run, ``extra='forbid'`` so a
    misspelled YAML key is rejected rather than silently dropped, and
    ``validate_default`` so defaulted values pass the same validators as
    supplied ones.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', validate_default=True)
