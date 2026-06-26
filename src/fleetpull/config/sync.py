# src/fleetpull/config/sync.py
"""Sync-level configuration: settings that span the whole sync, not one provider.

The cross-cutting counterpart to the per-provider config modules -- where sync-wide
knobs live. Today that is only the cold-start backfill anchor; backfill chunk sizing
will join here when that layer is built. One module per config section (house rule).
"""

import logging
from datetime import UTC, date, datetime, time
from pathlib import Path

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
        dataset_root: The root directory the dataset is written under
            (``{root}/{provider}/{endpoint}/``, DESIGN section 3). Sync-wide -- one
            output location for the whole sync -- so it lives here rather than being
            threaded onto each runner. Required: there is no safe default for where
            output lands. A string path coerces to ``Path``; the storage layer
            normalizes it via ``resolve_path``.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', validate_default=True)

    default_start_date: date
    dataset_root: Path

    @property
    def default_start_datetime(self) -> datetime:
        """The cold-start anchor as a timezone-aware UTC midnight instant.

        ``default_start_date`` is the human-authored calendar date history
        begins from; the resume resolver (``resolve_resume_start``) composes it
        with watermark and frontier datetimes, so it is lifted to the start of
        that UTC day here. Deriving it keeps the stored field a ``date`` (a
        time-of-day on a backfill anchor is meaningless) while handing the
        orchestrator the ``datetime`` it needs, the conversion defined in one
        place.

        Returns:
            ``default_start_date`` at 00:00:00 with ``tzinfo`` ``datetime.UTC``.
        """
        return datetime.combine(self.default_start_date, time.min, tzinfo=UTC)
