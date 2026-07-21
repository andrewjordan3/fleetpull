# src/fleetpull/endpoints/samsara/vehicle_fuel_energy_reports.py
"""The Samsara vehicle_fuel_energy_reports binding: the window-grain
rollup surface -- the first fixed-unit-width watermark endpoint
(probe-settled 2026-07-21, DESIGN section 8). The legacy hub's
``vehicle_fuel_energy``, renamed per the name=snake-plural-of-model
invariant (model ``VehicleFuelEnergyReport``).

``GET /fleet/reports/vehicles/fuel-energy`` carries the standard
``pagination {endCursor, hasNextPage}`` cursor contract, but the record
list is NESTED: ``data`` is an OBJECT whose only key is
``vehicleReports``, a list of report objects -- the
``SamsaraWindowReportPageDecoder``'s shape. Reports are fleet-wide with
per-record vehicle attribution, so there is NO fan-out -- the default
``SingleFetch`` shape, declared by declaring nothing, and no roster is
sourced or consumed.

**THE ROLLUP GRAIN IS THE REQUEST WINDOW, proven twice:** widening a
1-day window to 2 days GREW per-vehicle metrics (36 of 47 vehicles
shared between the 1-day walk and the 2-day window's first page grew),
and summing two adjacent day rollups reproduced the two-day
rollup on only 178 of 267 vehicles (89/267 MISMATCHED across distance,
engine run time, fuel, and energy). Day rows are NOT a lossless
decomposition of wider windows -- each row is the provider's answer for
exactly its window. Consequence: the binding declares
``fixed_unit_days=1`` on its ``WatermarkMode`` -- the unit width is
part of the ROW'S MEANING, so it must never float with
``sync.backfill_chunk_days``. Rows carry NO event-time key of any kind;
the decoder stamps each with the sent window, and
``event_time_column='window_start'`` routes each day's rollup to its
own partition (the vehicle-presence union across day windows holds:
the two day windows' 145- and 242-vehicle sets union to exactly the
two-day walk's 267, so per-day fetches lose no vehicle).

The ``limit`` param is PROVEN IGNORED (the assignments placebo
posture): limit=512, 513, and 10 on the 2-day window all returned
identical paging (3 pages, 267 reports), and 513 was NOT rejected -- no
enforced tier. The declared ``results_limit=100`` is
documentation-by-declaration of the server's OWN observed ~100-report
page size, not a working knob.

The window rides this surface family's own ``startDate``/``endDate``
param NAMES (full RFC3339 datetimes accepted despite the names --
unlike every other probed Samsara vertical's ``startTime``/``endTime``;
the shared ``SamsaraFuelEnergyReportSpecBuilder`` carries the
provenance).
"""

from datetime import timedelta
from typing import Final

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.samsara._spec_builders import (
    SamsaraFuelEnergyReportSpecBuilder,
)
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    StorageKind,
    WatermarkMode,
)
from fleetpull.models.samsara import VehicleFuelEnergyReport
from fleetpull.network.decoders import SamsaraWindowReportPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

_VEHICLE_FUEL_ENERGY_PATH: Final[str] = '/fleet/reports/vehicles/fuel-energy'
_RECORDS_KEY: Final[str] = 'data'

# The nested report key: `data` is an OBJECT whose only key is this
# arm's report list (captured 2026-07-21).
_REPORT_KEY: Final[str] = 'vehicleReports'

# The per-page record count. ~100 is the server's OWN observed page
# size, and the `limit` param is PROVEN IGNORED on this surface:
# limit=512, 513, and 10 on the same 2-day window all returned
# identical paging (3 pages, 267 reports); 513 was NOT rejected (no
# enforced tier, captured 2026-07-21). Declaring 100 documents the
# server's paging; it is not a working knob.
_RESULTS_LIMIT: Final[int] = 100

# The fixed work-unit width, in whole days. The rollup grain is the
# request window (metrics GREW when the window widened) and day rollups
# are NON-ADDITIVE into wider windows (89/267 mismatched, captured
# 2026-07-21), so the unit width is part of the row's meaning and is
# pinned here rather than left to sync.backfill_chunk_days.
_FIXED_UNIT_DAYS: Final[int] = 1


def build_endpoint(
    config: SamsaraConfig,
) -> EndpointDefinition[VehicleFuelEnergyReport]:
    """Build the Samsara vehicle_fuel_energy_reports watermark binding.

    Fleet-wide per-vehicle fuel-energy rollups fetched incrementally at
    the fixed 1-day unit width: the run resumes from a ``DateWindow``
    (watermark with the provider's late-arrival lookback from config),
    the planner tiles it into exactly-one-day units (the declared
    ``fixed_unit_days`` -- module docstring for the non-additivity
    proof), each unit's reports are stamped with its window by the
    decoder and written to the ``date=YYYY-MM-DD`` partition on
    ``window_start``, and each refetched partition is replaced. Reports
    arrive nested under ``data.vehicleReports``, walked by explicit
    cursor pages (``limit`` on page one, ``after`` merged thereafter,
    the ``startDate``/``endDate`` window persisting throughout),
    terminal on ``hasNextPage: false``. No request shape is declared --
    the endpoint is a fleet-wide ``SingleFetch``, the default.

    Args:
        config: The validated Samsara configuration; supplies the base
            URL the spec-builder joins to the report path and the
            lookback and cutoff the watermark mode carries.

    Returns:
        The frozen vehicle_fuel_energy_reports ``EndpointDefinition``.
        Construction validates the ``WatermarkMode`` /
        ``DATE_PARTITIONED`` / ``event_time_column`` triple against the
        response model.
    """
    return EndpointDefinition(
        provider=Provider.SAMSARA,
        name='vehicle_fuel_energy_reports',
        spec_builder=SamsaraFuelEnergyReportSpecBuilder(
            base_url=config.base_url, path=_VEHICLE_FUEL_ENERGY_PATH
        ),
        page_decoder=SamsaraWindowReportPageDecoder(
            records_key=_RECORDS_KEY,
            report_key=_REPORT_KEY,
            results_limit=_RESULTS_LIMIT,
        ),
        response_model=VehicleFuelEnergyReport,
        quota_scope=QuotaScope.SAMSARA,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(
            lookback=timedelta(days=config.lookback_days),
            cutoff=timedelta(days=config.cutoff_days),
            fixed_unit_days=_FIXED_UNIT_DAYS,
        ),
        event_time_column='window_start',
    )
