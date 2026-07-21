# src/fleetpull/endpoints/geotab/status_data.py
"""The GeoTab status_data binding: the active diagnostic feed.

A ``GetFeed`` drive of the ``StatusData`` entity — the log_records
binding with the entity swapped: an ACTIVE feed (records emitted once,
reconciled by ``id``) that, unlike LogRecord, carries a per-record
``version`` — mirrored as wire truth (DESIGN §4/§8). The name is the
wire's own uncountable vocabulary (``StatusData`` → ``status_data``);
there is no plural to form.

``resultsLimit`` is the 50,000 protocol maximum: the probed tenant emits
~24,500 StatusData records/hour, so anything smaller only multiplies
pages against the ~60/min feed budget (DESIGN §8, the 2026-07-21 feed
wave block).

Every request here is a JSON-RPC POST whose ``params.credentials`` and
resolved host are the session strategy's injections (the devices-leaf
convention); the host this module writes is a pre-auth placeholder.
"""

from typing import Final

from fleetpull.config import GeotabConfig
from fleetpull.endpoints.geotab._requests import GeotabGetFeedSpecBuilder, server_host
from fleetpull.endpoints.shared import EndpointDefinition, FeedMode, StorageKind
from fleetpull.models.geotab import StatusData
from fleetpull.network.decoders import GeotabFeedPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The GetFeed protocol maximum; the hourly volume makes pages precious.
_RESULTS_LIMIT: Final[int] = 50000

_STATUS_DATA_TYPE_NAME: Final[str] = 'StatusData'


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[StatusData]:
    """Build the GeoTab status_data feed binding.

    The diagnostic stream fetched incrementally as a version-token
    feed: the run resumes from the stored token (seeded via
    ``search.fromDate`` on the tokenless first run only), each page
    appends durably before its ``toVersion`` commits, and the fetched
    records land in ``date=YYYY-MM-DD`` partitions as new numbered part
    files.

    Args:
        config: The validated GeoTab configuration; supplies the
            authentication host the pre-auth spec URLs are built on.

    Returns:
        The frozen status_data ``EndpointDefinition``. Construction
        validates the ``FeedMode`` / ``APPEND_LOG`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='status_data',
        spec_builder=GeotabGetFeedSpecBuilder(
            server=server_host(config),
            type_name=_STATUS_DATA_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabFeedPageDecoder(),
        response_model=StatusData,
        quota_scope=QuotaScope.GEOTAB_FEED,
        storage_kind=StorageKind.APPEND_LOG,
        sync_mode=FeedMode(),
        event_time_column='date_time',
    )
