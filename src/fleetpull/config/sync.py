# src/fleetpull/config/sync.py
"""Sync-level configuration: settings that span the whole sync, not one provider.

The cross-cutting counterpart to the per-provider config modules -- where sync-wide
knobs live. Today that is only the cold-start backfill anchor; backfill chunk sizing
will join here when that layer is built. One module per config section (house rule).
"""

import logging
from datetime import date

from pydantic import BaseModel, ConfigDict

__all__: list[str] = ['SyncConfig']

logger = logging.getLogger(__name__)


class SyncConfig(BaseModel):
    """
    User-facing sync-wide settings, one instance per run.

    Attributes:
        default_start_date: The cold-start backfill anchor -- the UTC calendar date
            a watermark endpoint's history begins from on its very first run, before
            any committed watermark or completed coverage exists (DESIGN section 4/5
            resume precedence arm 3). Required: there is no safe default for "where
            our history begins", so it must be declared. It goes inert the moment
            observed data or completed coverage exists; thereafter the start is
            derived from those, never from this.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', validate_default=True)

    default_start_date: date
