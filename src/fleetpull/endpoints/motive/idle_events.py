# src/fleetpull/endpoints/motive/idle_events.py
"""The Motive idle_events watermark binding.

A fleet-wide idle-event endpoint: one date-range request covers every
vehicle, offset-paginated through the shared wrapped-list decoder. Unlike
its driving_periods sibling, window matching here is OVERLAP-anchored on
**company-local** day boundaries, not UTC (DESIGN §8, captured
2026-07-15, prediction-confirmed): the wire dates are interpreted in the
account's configured timezone. The binding therefore pads the wire window
one day on each side — covering any account timezone — and the true UTC
window does the trimming: the post-fetch window filter keeps only records
whose ``start_time`` falls in the resume window, and the writer's
partition tripwire enforces it. Every record lands in exactly one chunk;
the pad widens only what is fetched, never what is written.

The 30-day range cap is NOT enforced here (a 35-day window was honored
end-to-end in capture) — but chunking stays bounded with the sibling's
cap anyway, keeping wide-window latency (observed 12-18 s) inside the
configured HTTP read timeout.
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
from fleetpull.models.motive import IdleEvent
from fleetpull.network.decoders import MotiveWrappedListPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

_IDLE_EVENTS_PATH: Final[str] = '/v1/idle_events'
_IDLE_EVENTS_LIST_KEY: Final[str] = 'idle_events'
_IDLE_EVENTS_ITEM_KEY: Final[str] = 'idle_event'

# The page size sent on every request. Captured honored at 100
# (2026-07-15); larger values are unprobed. A strong candidate for a
# user config knob — promotion is one new MotiveConfig field passed
# where this constant is passed.
_PER_PAGE: Final[int] = 100

# One whole day each side: the timezone-agnostic cover for the
# company-local overlap matching described in the module docstring.
_WINDOW_PAD_DAYS: Final[int] = 1


def build_endpoint(config: MotiveConfig) -> EndpointDefinition[IdleEvent]:
    """Build the Motive idle_events watermark binding.

    Fleet-wide idle intervals fetched incrementally: the run resumes from
    a ``DateWindow`` (watermark with the provider's late-arrival lookback
    from config), the wire window is padded per the module docstring, the
    kept records are written to ``date=YYYY-MM-DD`` partitions on
    ``start_time``, and each refetched partition is replaced. Records
    arrive wrapped and offset-paginated (``{"idle_events":
    [{"idle_event": {...}}], "pagination": {...}}``).

    Args:
        config: The validated Motive configuration; supplies the base URL
            and the lookback and cutoff the watermark mode carries.

    Returns:
        The frozen idle_events ``EndpointDefinition``. Construction
        validates the ``WatermarkMode`` / ``DATE_PARTITIONED`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='idle_events',
        spec_builder=MotiveFleetDateRangeSpecBuilder(
            base_url=config.base_url,
            path=_IDLE_EVENTS_PATH,
            window_pad_days=_WINDOW_PAD_DAYS,
        ),
        page_decoder=MotiveWrappedListPageDecoder(
            list_key=_IDLE_EVENTS_LIST_KEY,
            item_key=_IDLE_EVENTS_ITEM_KEY,
            per_page=_PER_PAGE,
        ),
        response_model=IdleEvent,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(
            lookback=timedelta(days=config.lookback_days),
            cutoff=timedelta(days=config.cutoff_days),
        ),
        event_time_column='start_time',
    )
