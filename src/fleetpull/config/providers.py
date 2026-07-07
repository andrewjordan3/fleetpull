# src/fleetpull/config/providers.py
"""Providers-section configuration: one optional entry per provider.

The ``providers:`` YAML section. Each entry is that provider's own
section model (``MotiveConfig`` today; Samsara and GeoTab entries join
as their providers port). An absent entry means the provider is simply
not configured -- no warning, no error; enablement rules apply only to
entries that are present (``config/loader.py``). One module per config
section (house rule).
"""

import logging

from pydantic import BaseModel, ConfigDict

from fleetpull.config.motive import MotiveConfig

__all__: list[str] = ['ProvidersConfig']

logger = logging.getLogger(__name__)


class ProvidersConfig(BaseModel):
    """
    The per-provider configuration entries, one instance per run.

    Attributes:
        motive: The Motive provider section, or ``None`` when the YAML
            does not configure Motive.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', validate_default=True)

    motive: MotiveConfig | None = None
