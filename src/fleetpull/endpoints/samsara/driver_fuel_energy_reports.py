# src/fleetpull/endpoints/samsara/driver_fuel_energy_reports.py
"""The Samsara driver_fuel_energy_reports binding: the driver arm of
the window-grain fuel-energy report pair (probe-settled 2026-07-21,
DESIGN section 8). The legacy hub's ``driver_fuel_energy``, renamed per
the name=snake-plural-of-model invariant (model
``DriverFuelEnergyReport``).

``GET /fleet/reports/drivers/fuel-energy`` is the vehicle arm's wire
family with the entity swapped: the standard cursor contract over the
NESTED record list (``data`` an OBJECT whose only key is
``driverReports``), reports fleet-wide with per-record driver
attribution (``driver {id, name}``; NO ``externalIds`` was ever
observed on this arm), so no fan-out and no roster -- the default
``SingleFetch`` shape, declared by declaring nothing.

The window-grain facts are the pair's, proven on the family
(vehicle_fuel_energy_reports carries the full evidence): the rollup
grain is the request window and day rollups are NON-ADDITIVE into
wider windows (89/267 mismatched, captured 2026-07-21), so the binding
declares ``fixed_unit_days=1`` -- the unit width is part of the ROW'S
MEANING and never floats with ``sync.backfill_chunk_days``. Rows carry
NO event-time key; the decoder stamps each with the sent window and
``event_time_column='window_start'`` routes each day's rollup to its
own partition. Pagination is real on this arm too: a 1-day driver
window showed ``hasNextPage: true`` at 100 reports, the same ~100
server-owned page size the placebo ``limit`` cannot move --
``results_limit=100`` is documentation-by-declaration.

The window rides the family's own ``startDate``/``endDate`` param
NAMES (RFC3339 datetimes accepted despite the names; the shared
``SamsaraFuelEnergyReportSpecBuilder`` carries the provenance).
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
from fleetpull.models.samsara import DriverFuelEnergyReport
from fleetpull.network.decoders import SamsaraWindowReportPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

_DRIVER_FUEL_ENERGY_PATH: Final[str] = '/fleet/reports/drivers/fuel-energy'
_RECORDS_KEY: Final[str] = 'data'

# The nested report key: `data` is an OBJECT whose only key is this
# arm's report list (captured 2026-07-21).
_REPORT_KEY: Final[str] = 'driverReports'

# The per-page record count. ~100 is the server's OWN observed page
# size (a 1-day driver window showed hasNextPage: true at 100 reports)
# and the `limit` param is PROVEN IGNORED on this surface family
# (limit=512/513/10 all paged identically; 513 NOT rejected -- no
# enforced tier, captured 2026-07-21). Declaring 100 documents the
# server's paging; it is not a working knob.
_RESULTS_LIMIT: Final[int] = 100

# The fixed work-unit width, in whole days -- the pair's window-grain
# and non-additivity proofs (module docstring; captured 2026-07-21):
# the unit width is part of the row's meaning, pinned here rather than
# left to sync.backfill_chunk_days.
_FIXED_UNIT_DAYS: Final[int] = 1


def build_endpoint(
    config: SamsaraConfig,
) -> EndpointDefinition[DriverFuelEnergyReport]:
    """Build the Samsara driver_fuel_energy_reports watermark binding.

    Fleet-wide per-driver fuel-energy rollups fetched incrementally at
    the fixed 1-day unit width: the run resumes from a ``DateWindow``
    (watermark with the provider's late-arrival lookback from config),
    the planner tiles it into exactly-one-day units (the declared
    ``fixed_unit_days`` -- module docstring for the non-additivity
    proof), each unit's reports are stamped with its window by the
    decoder and written to the ``date=YYYY-MM-DD`` partition on
    ``window_start``, and each refetched partition is replaced. Reports
    arrive nested under ``data.driverReports``, walked by explicit
    cursor pages (``limit`` on page one, ``after`` merged thereafter,
    the ``startDate``/``endDate`` window persisting throughout),
    terminal on ``hasNextPage: false``. No request shape is declared --
    the endpoint is a fleet-wide ``SingleFetch``, the default.

    Args:
        config: The validated Samsara configuration; supplies the base
            URL the spec-builder joins to the report path and the
            lookback and cutoff the watermark mode carries.

    Returns:
        The frozen driver_fuel_energy_reports ``EndpointDefinition``.
        Construction validates the ``WatermarkMode`` /
        ``DATE_PARTITIONED`` / ``event_time_column`` triple against the
        response model.
    """
    return EndpointDefinition(
        provider=Provider.SAMSARA,
        name='driver_fuel_energy_reports',
        spec_builder=SamsaraFuelEnergyReportSpecBuilder(
            base_url=config.base_url, path=_DRIVER_FUEL_ENERGY_PATH
        ),
        page_decoder=SamsaraWindowReportPageDecoder(
            records_key=_RECORDS_KEY,
            report_key=_REPORT_KEY,
            results_limit=_RESULTS_LIMIT,
        ),
        response_model=DriverFuelEnergyReport,
        quota_scope=QuotaScope.SAMSARA,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(
            lookback=timedelta(days=config.lookback_days),
            cutoff=timedelta(days=config.cutoff_days),
            fixed_unit_days=_FIXED_UNIT_DAYS,
        ),
        event_time_column='window_start',
    )
