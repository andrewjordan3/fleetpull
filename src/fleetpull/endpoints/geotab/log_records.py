# src/fleetpull/endpoints/geotab/log_records.py
"""The GeoTab log_records binding: the first GeoTab feed vertical.

A ``GetFeed`` drive of the ``LogRecord`` entity — the ACTIVE GPS stream:
records are emitted once, never re-emitted, and carry no per-record
version, so the append-only cell is trivially complete and the consumer
reconciles by ``id`` (DESIGN §4). The run resumes from the stored
``FeedToken`` (or a ``FeedSeed`` at the sync-wide cold-start anchor on
the tokenless first run), rides the shared ``GeotabGetFeedSpecBuilder``
and ``GeotabFeedPageDecoder``, spends from the ``geotab_feed``
method-class budget, and appends every emitted record into its event
date's partition as numbered part files — stored as emitted, nothing
ever deleted or replaced.

``resultsLimit`` is the 50,000 protocol maximum: the probed tenant emits
>50,000 LogRecords/day (a 50,000-record page did not cover one day), so
anything smaller only multiplies pages against the ~60/min feed budget
(DESIGN §8, the 2026-07-21 feed wave block).

Every request here is a JSON-RPC POST whose ``params.credentials`` and
resolved host are the session strategy's injections (the devices-leaf
convention); the host this module writes is a pre-auth placeholder.
"""

from typing import Final

from fleetpull.config import GeotabConfig
from fleetpull.endpoints.geotab._requests import GeotabGetFeedSpecBuilder, server_host
from fleetpull.endpoints.shared import EndpointDefinition, FeedMode, StorageKind
from fleetpull.models.geotab import LogRecord
from fleetpull.network.decoders import GeotabFeedPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The GetFeed protocol maximum; the daily volume exceeds one page.
_RESULTS_LIMIT: Final[int] = 50000

_LOG_RECORD_TYPE_NAME: Final[str] = 'LogRecord'


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[LogRecord]:
    """Build the GeoTab log_records feed binding.

    The GPS stream fetched incrementally as a version-token feed: the
    run resumes from the stored token (seeded via ``search.fromDate``
    on the tokenless first run only), each page appends durably before
    its ``toVersion`` commits, and the fetched records land in
    ``date=YYYY-MM-DD`` partitions as new numbered part files.

    Args:
        config: The validated GeoTab configuration; supplies the
            authentication host the pre-auth spec URLs are built on.

    Returns:
        The frozen log_records ``EndpointDefinition``. Construction
        validates the ``FeedMode`` / ``APPEND_LOG`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='log_records',
        spec_builder=GeotabGetFeedSpecBuilder(
            server=server_host(config),
            type_name=_LOG_RECORD_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabFeedPageDecoder(),
        response_model=LogRecord,
        quota_scope=QuotaScope.GEOTAB_FEED,
        storage_kind=StorageKind.APPEND_LOG,
        sync_mode=FeedMode(),
        event_time_column='date_time',
    )
