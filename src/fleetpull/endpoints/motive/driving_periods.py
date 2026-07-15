# src/fleetpull/endpoints/motive/driving_periods.py
"""The Motive driving_periods watermark binding.

A fleet-wide driving-event endpoint: one date-range request covers every
vehicle, offset-paginated through the shared wrapped-list decoder. Window
matching is START-anchored on UTC days (DESIGN §8, captured 2026-07-15):
retrieval anchor and partition-routing anchor coincide natively, so the
builder maps the resume window to the wire dates with no pad.

The provider enforces a loud 30-day range cap — HTTP 400, ``"Date range
cannot be greater than 30 days"`` — counting the date delta. Steady-state
windows sit far inside it; a backfill configured with chunks wider than
30 days fails loudly on the first request rather than losing data
silently, and the fix is a smaller chunk width.
"""

from datetime import timedelta
from typing import Final

from fleetpull.config import MotiveConfig
from fleetpull.endpoints.motive._spec_builders import MotiveFleetDateRangeSpecBuilder
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    StorageKind,
    WatermarkMode,
)
from fleetpull.models.motive import DrivingPeriod
from fleetpull.network.decoders import MotiveWrappedListPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

_DRIVING_PERIODS_PATH: Final[str] = '/v1/driving_periods'
_DRIVING_PERIODS_LIST_KEY: Final[str] = 'driving_periods'
_DRIVING_PERIODS_ITEM_KEY: Final[str] = 'driving_period'

# The page size sent on every request. Captured honored at 100
# (2026-07-15); larger values are unprobed. A strong candidate for a
# user config knob — promotion is one new MotiveConfig field passed
# where this constant is passed.
_PER_PAGE: Final[int] = 100


def build_endpoint(config: MotiveConfig) -> EndpointDefinition[DrivingPeriod]:
    """Build the Motive driving_periods watermark binding.

    Fleet-wide driving spans fetched incrementally: the run resumes from
    a ``DateWindow`` (watermark with the provider's late-arrival lookback
    from config — which also refetches yesterday's in-progress records
    once they complete), the fetched whole days are written to
    ``date=YYYY-MM-DD`` partitions on ``start_time``, and each refetched
    partition is replaced. Records arrive wrapped and offset-paginated
    (``{"driving_periods": [{"driving_period": {...}}], "pagination":
    {...}}``).

    Args:
        config: The validated Motive configuration; supplies the base URL
            and the lookback and cutoff the watermark mode carries.

    Returns:
        The frozen driving_periods ``EndpointDefinition``. Construction
        validates the ``WatermarkMode`` / ``DATE_PARTITIONED`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='driving_periods',
        spec_builder=MotiveFleetDateRangeSpecBuilder(
            base_url=config.base_url,
            path=_DRIVING_PERIODS_PATH,
        ),
        page_decoder=MotiveWrappedListPageDecoder(
            list_key=_DRIVING_PERIODS_LIST_KEY,
            item_key=_DRIVING_PERIODS_ITEM_KEY,
            per_page=_PER_PAGE,
        ),
        response_model=DrivingPeriod,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(
            lookback=timedelta(days=config.lookback_days),
            cutoff=timedelta(days=config.cutoff_days),
        ),
        event_time_column='start_time',
    )
