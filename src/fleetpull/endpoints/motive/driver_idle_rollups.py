# src/fleetpull/endpoints/motive/driver_idle_rollups.py
"""The Motive driver_idle_rollups binding: the driver arm of the
window-grain utilization rollup pair (probe-settled 2026-07-21, DESIGN
section 8). The legacy hub's ``driver_utilization``, shipped under the
WIRE'S OWN envelope vocabulary -- the wrapper is
``driver_idle_rollups``/``driver_idle_rollup``, a different vocabulary
from its ``/v2/driver_utilization`` path, and the endpoint mirrors the
wire (model ``DriverIdleRollup``; the legacy-name mapping is recorded
in ``ENDPOINTS.md``).

The vehicle arm's binding with the path, wrapper keys, and population
swapped: the standard Motive wrapped-list envelope and offset
pagination at the configured page size, but rows are the DRIVERS WITH
ACTIVITY in the window (13 on a quiet single day, 653 across six days
-- per-driver-per-window grain, unlike the vehicle arm's
whole-fleet-every-window population), each attributed to the shared
8-key ``UserSummary`` ``driver`` ref -- or to NULL on the unattributed
rollup bucket row. Fleet-wide with per-record attribution, so no
fan-out and no roster -- the default ``SingleFetch`` shape, declared by
declaring nothing.

The pair's window-grain facts are shared (vehicle_utilizations carries
the full statement): the rollup grain is the request window, so
``fixed_unit_days=1`` (the fuel-energy machinery's second consumer; the
family precedent's do-not-sum posture rides the model docstring), the
decoder-stamped ``window_start`` routes each day's rollup to its own
partition, and the shared ``MotiveFleetDateRangeSpecBuilder`` maps the
unit ``DateWindow`` to the INCLUSIVE ``start_date``/``end_date`` label
pair -- interpreted in COMPANY-LOCAL days (the account's
``/v1/companies`` zone at a UTC-5 offset), mirrored verbatim, never
converted.
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
from fleetpull.models.motive import DriverIdleRollup
from fleetpull.network.decoders import MotiveWindowReportPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

_DRIVER_IDLE_ROLLUPS_PATH: Final[str] = '/v2/driver_utilization'

# The wire's OWN envelope vocabulary -- NOT the path's: the wrapper keys
# are driver_idle_rollups/driver_idle_rollup (captured 2026-07-21), and
# the endpoint name follows the wire.
_DRIVER_IDLE_ROLLUPS_LIST_KEY: Final[str] = 'driver_idle_rollups'
_DRIVER_IDLE_ROLLUPS_ITEM_KEY: Final[str] = 'driver_idle_rollup'

# The fixed work-unit width, in whole days -- the pair's window-grain
# proof (module docstring; captured 2026-07-21): the unit width is part
# of the row's meaning, pinned here rather than left to
# sync.backfill_chunk_days (the fuel-energy machinery, DESIGN section 5).
_FIXED_UNIT_DAYS: Final[int] = 1


def build_endpoint(config: MotiveConfig) -> EndpointDefinition[DriverIdleRollup]:
    """Build the Motive driver_idle_rollups watermark binding.

    Per-driver idle rollups fetched incrementally at the fixed 1-day
    unit width: the run resumes from a ``DateWindow`` (watermark with
    the provider's late-arrival lookback from config), the planner tiles
    it into exactly-one-day units (the declared ``fixed_unit_days`` --
    module docstring for the window-grain statement), each unit's rows
    are stamped with its window by the decoder and written to the
    ``date=YYYY-MM-DD`` partition on ``window_start``, and each
    refetched partition is replaced. Records arrive wrapped
    (``{"driver_idle_rollups": [{"driver_idle_rollup": {...}}]}``)
    under page-numbered pagination at the page size the config requests,
    the ``start_date``/``end_date`` labels persisting across the walk.

    Args:
        config: The validated Motive configuration; supplies the base
            URL the spec-builder joins to the utilization path, the page
            size the decoder requests, and the lookback and cutoff the
            watermark mode carries.

    Returns:
        The frozen driver_idle_rollups ``EndpointDefinition``.
        Construction validates the ``WatermarkMode`` /
        ``DATE_PARTITIONED`` / ``event_time_column`` triple against the
        response model.
    """
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='driver_idle_rollups',
        spec_builder=MotiveFleetDateRangeSpecBuilder(
            base_url=config.base_url,
            path=_DRIVER_IDLE_ROLLUPS_PATH,
        ),
        page_decoder=MotiveWindowReportPageDecoder(
            list_key=_DRIVER_IDLE_ROLLUPS_LIST_KEY,
            item_key=_DRIVER_IDLE_ROLLUPS_ITEM_KEY,
            per_page=config.records_per_page,
        ),
        response_model=DriverIdleRollup,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(
            lookback=timedelta(days=config.lookback_days),
            cutoff=timedelta(days=config.cutoff_days),
            fixed_unit_days=_FIXED_UNIT_DAYS,
        ),
        event_time_column='window_start',
    )
