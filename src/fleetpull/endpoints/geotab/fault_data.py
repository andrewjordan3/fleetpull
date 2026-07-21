# src/fleetpull/endpoints/geotab/fault_data.py
"""The GeoTab fault_data binding: the engine-fault feed.

A ``GetFeed`` drive of the ``FaultData`` entity — the log_records
asymmetry stance verbatim: FaultData carries NO per-record ``version``
(the 2026-07-21 wave two census, DESIGN §8), so the append-only cell is
trivially complete and the consumer reconciles by ``id`` alone
(DESIGN §4). The name is the wire's own uncountable vocabulary
(``FaultData`` → ``fault_data``, the status_data precedent); there is
no plural to form.

``resultsLimit`` is the 50,000 protocol maximum: the docs list no lower
per-type cap for this type (verified against the GetFeed reference
2026-07-21), and the census pulls used small limits, which prove
nothing about caps (DESIGN §8, the wave two block).

Every request here is a JSON-RPC POST whose ``params.credentials`` and
resolved host are the session strategy's injections (the devices-leaf
convention); the host this module writes is a pre-auth placeholder.
"""

from typing import Final

from fleetpull.config import GeotabConfig
from fleetpull.endpoints.geotab._requests import GeotabGetFeedSpecBuilder, server_host
from fleetpull.endpoints.shared import EndpointDefinition, FeedMode, StorageKind
from fleetpull.models.geotab import FaultData
from fleetpull.network.decoders import GeotabFeedPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The GetFeed protocol maximum; no lower documented per-type cap.
_RESULTS_LIMIT: Final[int] = 50000

_FAULT_DATA_TYPE_NAME: Final[str] = 'FaultData'


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[FaultData]:
    """Build the GeoTab fault_data feed binding.

    The engine-fault stream fetched incrementally as a version-token
    feed: the run resumes from the stored token (seeded via
    ``search.fromDate`` on the tokenless first run only), each page
    appends durably before its ``toVersion`` commits, and the fetched
    records land in ``date=YYYY-MM-DD`` partitions as new numbered part
    files.

    Args:
        config: The validated GeoTab configuration; supplies the
            authentication host the pre-auth spec URLs are built on.

    Returns:
        The frozen fault_data ``EndpointDefinition``. Construction
        validates the ``FeedMode`` / ``APPEND_LOG`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='fault_data',
        spec_builder=GeotabGetFeedSpecBuilder(
            server=server_host(config),
            type_name=_FAULT_DATA_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabFeedPageDecoder(),
        response_model=FaultData,
        quota_scope=QuotaScope.GEOTAB_FEED,
        storage_kind=StorageKind.APPEND_LOG,
        sync_mode=FeedMode(),
        event_time_column='date_time',
    )
