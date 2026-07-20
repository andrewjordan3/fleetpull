# src/fleetpull/endpoints/samsara/gps_readings.py
"""The Samsara gps_readings binding: the vehicle-stats windowed cursor
walk for ``types=gps`` -- one of the three endpoints the legacy
``/fleet/vehicles/stats/history`` surface splits into (engine_states,
gps_readings, odometer_readings; the three stat types carry disjoint
schemas, so each is its own entity and dataset -- DESIGN §8).

``GET /fleet/vehicles/stats/history`` is a modern-envelope surface
(``data`` + ``pagination {endCursor, hasNextPage}``), but its cursor
walks the VEHICLE axis within the fixed window: three consecutive live
pages showed zero vehicle-id overlap (captured 2026-07-20). Each
vehicle record nests one reading series under the requested type's key,
so the binding pairs the shared windowed builder (which bakes the FIXED
``types=gps`` selector into every request -- the ``types`` vocabulary
is API-enforced on input, a loud 400 on any unknown value) with
``SamsaraVehicleSeriesPageDecoder``, which unnests each vehicle's
series into flat per-reading records and delegates pagination to the
inner cursor decoder untouched.

Only carrier vehicles are returned per requested type (the 24-hour gps
sample: 569 vehicles, 569 with data -- no empty-array padding
observed), and the endpoint is fleet-wide with per-record vehicle
attribution synthesized by the decoder, so there is NO fan-out -- the
default ``SingleFetch`` shape, declared by declaring nothing, and no
roster is sourced or consumed.

The per-endpoint ``limit`` maximum is 512, probed directly on THIS
surface: limit=512 returned HTTP 200 and limit=513 a loud HTTP 400 --
the vehicles/drivers tier, NOT idling's 200 (the per-endpoint
limit-tier rule, honored by probing rather than assuming).

Retrieval is READING-TIME anchored on the half-open ``[startTime,
endTime)`` window, probe-proven: a 12:00-13:00Z window returned
readings spanning exactly 12:00:03.062Z..12:59:56.881Z. Consequence:
``event_time_column='time'``, the retrieval anchor and the routing
anchor coincide natively, no wire pad exists, and the runner's
post-fetch window filter is pure hygiene.
"""

from datetime import timedelta
from typing import Final

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.samsara._spec_builders import (
    RECORDS_KEY,
    RESULTS_LIMIT,
    STATS_HISTORY_PATH,
    SamsaraVehicleStatsSpecBuilder,
)
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    StorageKind,
    WatermarkMode,
)
from fleetpull.models.samsara import GpsReading
from fleetpull.network.decoders import SamsaraVehicleSeriesPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The requested stat type -- the verbatim `types` wire value AND the
# per-vehicle series key the decoder unnests (one name on the wire for
# both roles, captured 2026-07-20).
_STAT_TYPE: Final[str] = 'gps'


def build_endpoint(config: SamsaraConfig) -> EndpointDefinition[GpsReading]:
    """Build the Samsara gps_readings watermark binding.

    Fleet-wide GPS readings fetched incrementally: the run resumes
    from a ``DateWindow`` (watermark with the provider's late-arrival
    lookback from config), the fetched readings are written to
    ``date=YYYY-MM-DD`` partitions on ``time``, and each refetched
    partition is replaced. Vehicle records arrive as a top-level list
    under ``data``, walked by explicit cursor pages along the VEHICLE
    axis (``limit`` on page one, ``after`` merged thereafter, the
    window and ``types`` parameters persisting throughout), and the
    decoder unnests each vehicle's ``gps`` series into one flat record
    per reading. No request shape is declared -- the endpoint is a
    fleet-wide ``SingleFetch``, the default.

    Args:
        config: The validated Samsara configuration; supplies the base
            URL the spec-builder joins to the stats-history path and
            the lookback and cutoff the watermark mode carries.

    Returns:
        The frozen gps_readings ``EndpointDefinition``. Construction
        validates the ``WatermarkMode`` / ``DATE_PARTITIONED`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.SAMSARA,
        name='gps_readings',
        spec_builder=SamsaraVehicleStatsSpecBuilder(
            base_url=config.base_url,
            path=STATS_HISTORY_PATH,
            stat_type=_STAT_TYPE,
        ),
        page_decoder=SamsaraVehicleSeriesPageDecoder(
            records_key=RECORDS_KEY,
            results_limit=RESULTS_LIMIT,
            series_key=_STAT_TYPE,
        ),
        response_model=GpsReading,
        quota_scope=QuotaScope.SAMSARA,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(
            lookback=timedelta(days=config.lookback_days),
            cutoff=timedelta(days=config.cutoff_days),
        ),
        event_time_column='time',
    )
