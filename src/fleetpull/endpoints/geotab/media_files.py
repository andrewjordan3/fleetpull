# src/fleetpull/endpoints/geotab/media_files.py
"""The GeoTab media_files binding: the media-attachment feed.

A ``GetFeed`` drive of the ``MediaFile`` entity — media attachments
(images, video, ...) carrying a per-record ``version``; the consumer
reconciles by ``(id, max version)`` (DESIGN §4). MediaFile carries NO
``dateTime`` key: the event-time identity is ``fromDate`` (the media
start, 55/55), so this leaf anchors ``event_time_column='from_date'``
rather than the ``date_time`` its feed siblings use
(``models/geotab/media_file.py``). Thin evidence at the probed tenant —
55 records over 730 days — so the model is conservative.

``resultsLimit`` is the 50,000 protocol maximum: the docs list no lower
per-type cap for this type (the SCALE census cannot probe a cap the
55-record population never reaches — the FillUp dual-provenance lesson's
other arm, DESIGN §8), so the protocol maximum stands.

Every request here is a JSON-RPC POST whose ``params.credentials`` and
resolved host are the session strategy's injections (the devices-leaf
convention); the host this module writes is a pre-auth placeholder.
"""

from typing import Final

from fleetpull.config import GeotabConfig
from fleetpull.endpoints.geotab._requests import GeotabGetFeedSpecBuilder, server_host
from fleetpull.endpoints.shared import EndpointDefinition, FeedMode, StorageKind
from fleetpull.models.geotab import MediaFile
from fleetpull.network.decoders import GeotabFeedPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The GetFeed protocol maximum; no lower documented per-type cap.
_RESULTS_LIMIT: Final[int] = 50000

_MEDIA_FILE_TYPE_NAME: Final[str] = 'MediaFile'


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[MediaFile]:
    """Build the GeoTab media_files feed binding.

    Media attachments fetched incrementally as a version-token feed: the
    run resumes from the stored token (seeded via ``search.fromDate`` on
    the tokenless first run only), each page appends durably before its
    ``toVersion`` commits, and the fetched records land in
    ``date=YYYY-MM-DD`` partitions (by ``fromDate``, the event time) as
    new numbered part files — re-emitted versions accumulate for the
    consumer's ``(id, max version)`` reconcile.

    Args:
        config: The validated GeoTab configuration; supplies the
            authentication host the pre-auth spec URLs are built on.

    Returns:
        The frozen media_files ``EndpointDefinition``. Construction
        validates the ``FeedMode`` / ``APPEND_LOG`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='media_files',
        spec_builder=GeotabGetFeedSpecBuilder(
            server=server_host(config),
            type_name=_MEDIA_FILE_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabFeedPageDecoder(),
        response_model=MediaFile,
        quota_scope=QuotaScope.GEOTAB_FEED,
        storage_kind=StorageKind.APPEND_LOG,
        sync_mode=FeedMode(),
        event_time_column='from_date',
    )
