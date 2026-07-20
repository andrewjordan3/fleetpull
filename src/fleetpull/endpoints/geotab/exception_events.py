# src/fleetpull/endpoints/geotab/exception_events.py
"""The GeoTab exception_events binding: the bisected windowed endpoint.

A date-windowed pull of the ``ExceptionEvent`` entity — the UNFILTERED
stream, every rule (DESIGN §8's 2026-07-15 decision block: no
server-side rule filter in version one; rule selection is the
consumer's one-expression job on the delivered stream). The seek walk
is structurally unavailable here — id-sort is rejected outright for
this type (captured 2026-07-15: ``ArgumentException``, "Can not sort by
id"), and any sort composed with a search degrades to the deterministic
``-32000 GenericException`` — so the leaf composes the shared
``GeotabWindowedGetSpecBuilder`` (``_get_requests``) with
``id_sort=False`` (no ``sort`` member ever written), declares the
``BisectedWindowFetch`` request shape, and the orchestrator's bisecting
driver fetches each unit window whole, halving on the exactly-full
overflow signal down to the floor.

``ExceptionEventSearch`` window matching is OVERLAP-anchored (captured
2026-07-15): retrieval supersets start-anchored ownership, so
``active_from`` is the event-time column, the driver's leaf filter and
the runner's window filter assign every record exactly one owner, and
no wire-window pad is needed. Events mutate after creation (~1 h
observed envelope), which the provider-level lookback absorbs.

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
    BisectedWindowFetch,
    EndpointDefinition,
    StorageKind,
    WatermarkMode,
)
from fleetpull.models.geotab import ExceptionEvent
from fleetpull.network.decoders import SinglePageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The per-request record limit AND the bisection overflow threshold.
# The silent cap is Captured on this type (2026-07-15: GetCountOf
# 304,716 vs a bare Get returning exactly 5,000); per-type provenance,
# not a global GeoTab fact. A strong candidate for a user config knob.
_RESULTS_LIMIT: Final[int] = 5000

# The bisection floor: a one-minute window still returning a full page
# fails loudly (sustained >5,000 events/minute fleet-wide, or >5,000
# events overlapping one instant -- feed territory either way). A
# strong candidate for a user config knob.
_FLOOR: Final[timedelta] = timedelta(minutes=1)

# The wire key the bisecting driver anchors leaf ownership by.
_EVENT_TIME_WIRE_KEY: Final[str] = 'activeFrom'

# The JSON-RPC envelope key the single-page decoder reads records from
# (the constants-scope precedent: module-private, colocated with the
# decoder composition that consumes it).
_RESULT_KEY: Final[str] = 'result'

_EXCEPTION_EVENT_TYPE_NAME: Final[str] = 'ExceptionEvent'


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[ExceptionEvent]:
    """Build the GeoTab exception_events bisected watermark binding.

    The unfiltered ExceptionEvent stream fetched incrementally: the run
    resumes from a ``DateWindow`` (watermark with the provider's
    late-arrival lookback from config, which also absorbs the observed
    post-creation mutation), the bisecting driver fetches each unit
    window whole (halving on overflow per the declared
    ``BisectedWindowFetch`` shape), and the kept records land in
    ``date=YYYY-MM-DD`` partitions on ``active_from``, each refetched
    partition replaced. Responses are single pages under the JSON-RPC
    ``result`` key — the cap that once disqualified single-page decoding
    is the driver's overflow signal now.

    Args:
        config: The validated GeoTab configuration; supplies the auth
            host and the lookback and cutoff the watermark mode carries.

    Returns:
        The frozen exception_events ``EndpointDefinition``. Construction
        validates the watermark / partitioned / event-time triple and the
        bisection pairing.
    """
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='exception_events',
        spec_builder=GeotabWindowedGetSpecBuilder(
            server=server_host(config),
            type_name=_EXCEPTION_EVENT_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
            id_sort=False,
        ),
        page_decoder=SinglePageDecoder(records_key=_RESULT_KEY),
        response_model=ExceptionEvent,
        quota_scope=QuotaScope.GEOTAB_GET,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(
            lookback=timedelta(days=config.lookback_days),
            cutoff=timedelta(days=config.cutoff_days),
        ),
        event_time_column='active_from',
        request_shape=BisectedWindowFetch(
            results_limit=_RESULTS_LIMIT,
            floor=_FLOOR,
            event_time_wire_key=_EVENT_TIME_WIRE_KEY,
        ),
    )
