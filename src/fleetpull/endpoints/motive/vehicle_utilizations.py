# src/fleetpull/endpoints/motive/vehicle_utilizations.py
"""The Motive vehicle_utilizations binding: the window-grain rollup
surface -- Motive's arm of the fixed-unit-width watermark family
(probe-settled 2026-07-21, DESIGN section 8). The legacy hub's
``vehicle_utilization``, shipped under the wire's own plural envelope
vocabulary (model ``VehicleUtilization``).

``GET /v2/vehicle_utilization`` carries the standard Motive
wrapped-list envelope (wrapper ``vehicle_utilizations`` /
``vehicle_utilization``) and offset pagination at the configured page
size (``per_page`` 50 and 100 both honored live). The population is the
WHOLE vehicle fleet regardless of window (1,466 on the 1-day and the
6-day probe alike -- inactive vehicles ride with zeroed metrics and a
``message`` status string), fleet-wide with per-record vehicle
attribution, so there is NO fan-out -- the default ``SingleFetch``
shape, declared by declaring nothing, and no roster is sourced or
consumed.

**THE ROLLUP GRAIN IS THE REQUEST WINDOW** (a 1-day and a 6-day request
each returned one rollup row per vehicle), so the binding declares
``fixed_unit_days=1`` on its ``WatermarkMode`` -- the unit width is
part of the ROW'S MEANING and never floats with
``sync.backfill_chunk_days`` (the Samsara fuel-energy machinery, its
second consumer; additivity was not probed here, and the family
precedent's do-not-sum posture rides the model docstring). Rows carry
NO date or time identity; the ``MotiveWindowReportPageDecoder`` stamps
each with the sent window and ``event_time_column='window_start'``
routes each day's rollup to its own partition.

**The window mapping and the company-local caveat.** The shared
``MotiveFleetDateRangeSpecBuilder`` maps the half-open unit
``DateWindow`` ``[start, end)`` to the INCLUSIVE date-label pair the
wire takes -- ``start_date`` is ``start``'s date and ``end_date`` the
last covered date (``end`` minus a day) -- proven inclusive on both
ends live (``start_date=end_date`` returned exactly one day's rollup).
With the fixed 1-day unit both labels are the unit's day. The labels
are interpreted in COMPANY-LOCAL days (the account's ``/v1/companies``
zone at a UTC-5 offset -- DESIGN section 8), so each unit's rows are
the provider's company-local-day rollups, mirrored verbatim: no pad, no
trim, the caveat documented on the model, never converted.
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
from fleetpull.models.motive import VehicleUtilization
from fleetpull.network.decoders import MotiveWindowReportPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

_VEHICLE_UTILIZATIONS_PATH: Final[str] = '/v2/vehicle_utilization'

# The wire's own envelope vocabulary -- plural list key, singular item
# key, exactly the sibling wrapped-list convention (captured 2026-07-21).
_VEHICLE_UTILIZATIONS_LIST_KEY: Final[str] = 'vehicle_utilizations'
_VEHICLE_UTILIZATIONS_ITEM_KEY: Final[str] = 'vehicle_utilization'

# The fixed work-unit width, in whole days. The rollup grain is the
# request window (a 1-day and a 6-day request each returned one rollup
# row per vehicle, captured 2026-07-21), so the unit width is part of
# the row's meaning and is pinned here rather than left to
# sync.backfill_chunk_days (the fuel-energy machinery, DESIGN section 5).
_FIXED_UNIT_DAYS: Final[int] = 1


def build_endpoint(config: MotiveConfig) -> EndpointDefinition[VehicleUtilization]:
    """Build the Motive vehicle_utilizations watermark binding.

    Whole-fleet per-vehicle utilization rollups fetched incrementally at
    the fixed 1-day unit width: the run resumes from a ``DateWindow``
    (watermark with the provider's late-arrival lookback from config),
    the planner tiles it into exactly-one-day units (the declared
    ``fixed_unit_days`` -- module docstring for the window-grain proof),
    each unit's rows are stamped with its window by the decoder and
    written to the ``date=YYYY-MM-DD`` partition on ``window_start``,
    and each refetched partition is replaced. Records arrive wrapped
    (``{"vehicle_utilizations": [{"vehicle_utilization": {...}}]}``)
    under page-numbered pagination at the page size the config requests,
    the ``start_date``/``end_date`` labels persisting across the walk.

    Args:
        config: The validated Motive configuration; supplies the base
            URL the spec-builder joins to the utilization path, the page
            size the decoder requests, and the lookback and cutoff the
            watermark mode carries.

    Returns:
        The frozen vehicle_utilizations ``EndpointDefinition``.
        Construction validates the ``WatermarkMode`` /
        ``DATE_PARTITIONED`` / ``event_time_column`` triple against the
        response model.
    """
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='vehicle_utilizations',
        spec_builder=MotiveFleetDateRangeSpecBuilder(
            base_url=config.base_url,
            path=_VEHICLE_UTILIZATIONS_PATH,
        ),
        page_decoder=MotiveWindowReportPageDecoder(
            list_key=_VEHICLE_UTILIZATIONS_LIST_KEY,
            item_key=_VEHICLE_UTILIZATIONS_ITEM_KEY,
            per_page=config.records_per_page,
        ),
        response_model=VehicleUtilization,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(
            lookback=timedelta(days=config.lookback_days),
            cutoff=timedelta(days=config.cutoff_days),
            fixed_unit_days=_FIXED_UNIT_DAYS,
        ),
        event_time_column='window_start',
    )
