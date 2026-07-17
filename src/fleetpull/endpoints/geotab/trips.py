# src/fleetpull/endpoints/geotab/trips.py
"""The GeoTab trips binding: the first windowed (watermark) GeoTab endpoint.

A date-windowed, seek-paged pull of the ``Trip`` entity: the run resumes
from a ``DateWindow`` (watermark with the provider's late-arrival
lookback from config -- for trips, the same margin absorbs GeoTab's Trip
recalculation), the request shape is the shared
``GeotabWindowedGetSpecBuilder`` (``_get_requests``) with
``id_sort=True`` -- the window rides a ``TripSearch``
(``search.fromDate`` / ``search.toDate``) beside the id-ascending
``sort`` of the seek walk -- and the fetched days land in
``date=YYYY-MM-DD`` partitions replaced wholesale. The decoder is the
existing ``GeotabGetPageDecoder`` unchanged: its advance spreads the
sent params when rewriting ``sort.offset``, so ``search`` survives
every page (live-verified 2026-07-13 -- a windowed, sorted, seeked page
pair returned strictly-ascending ids across the boundary with every
record inside the window).

``TripSearch`` matches trips by their STOP time (captured 2026-07-06
via a discriminating window pair; prediction-confirmed 2026-07-15,
DESIGN §8): a trip whose start precedes ``fromDate`` returns when its
stop falls inside, and a trip stopping past ``toDate`` never returns.
The event-time column is therefore ``stop`` — retrieval (stop in
window) and routing (the runner's per-batch window filter over the
event-time column) coincide, so every completed trip belongs to exactly
one chunk: its stop's day. The watermark advances on stop, which
matches record-materialization order — a Trip exists only once it has
stopped. Anchoring on ``start`` instead would drop every
chunk-boundary-crossing trip: the chunk owning its start never receives
it, and the chunk that fetches it filters it out.

Every request here is a JSON-RPC POST whose ``params.credentials`` and
resolved host are the session strategy's injections (the devices-leaf
convention); the host this module writes is a pre-auth placeholder.
"""

from datetime import timedelta
from typing import Final

from fleetpull.config import GeotabConfig
from fleetpull.endpoints.geotab._get_requests import (
    GeotabWindowedGetSpecBuilder,
    server_host,
)
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    StorageKind,
    WatermarkMode,
)
from fleetpull.models.geotab import Trip
from fleetpull.network.decoders import GeotabGetPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The largest sound page under Get's silent 5,000-record cap.
_RESULTS_LIMIT: Final[int] = 5000

_TRIP_TYPE_NAME: Final[str] = 'Trip'


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[Trip]:
    """Build the GeoTab trips watermark binding.

    Movement-interval history fetched incrementally: the run resumes
    from a ``DateWindow``, each window is walked in id-ascending seek
    pages under the silent 5,000-record ``Get`` cap with the window
    filter riding ``search``, and the fetched days are written to
    ``date=YYYY-MM-DD`` partitions replaced wholesale. No
    ``completeness_check``: the guard is snapshot-only by construction
    -- a ``GetCountOf`` compares only against a complete listing, and a
    date window is not one.

    Args:
        config: The validated GeoTab configuration; supplies the
            authentication host the pre-auth spec URLs are built on and
            the lookback and cutoff the watermark mode carries.

    Returns:
        The frozen trips ``EndpointDefinition``. Construction validates
        the ``WatermarkMode`` / ``DATE_PARTITIONED`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='trips',
        spec_builder=GeotabWindowedGetSpecBuilder(
            server=server_host(config),
            type_name=_TRIP_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
            id_sort=True,
        ),
        page_decoder=GeotabGetPageDecoder(),
        response_model=Trip,
        quota_scope=QuotaScope.GEOTAB_GET,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(
            lookback=timedelta(days=config.lookback_days),
            cutoff=timedelta(days=config.cutoff_days),
        ),
        event_time_column='stop',
    )
