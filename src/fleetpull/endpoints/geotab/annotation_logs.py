# src/fleetpull/endpoints/geotab/annotation_logs.py
"""The GeoTab annotation_logs binding: the duty-status-log annotation feed.

A ``GetFeed`` drive of the ``AnnotationLog`` entity — free-text
annotations attached to HOS duty-status logs, carrying a per-record
``version``. Annotations are user-editable, so re-emission under newer
versions is expected, every emitted version is stored, and the consumer
reconciles by ``(id, max version)`` (DESIGN §4). The annotation's
``dutyStatusLog`` ref points back to the ``duty_status_logs`` vertical
(``models/geotab/annotation_log.py``), completing the wave-two loop.

``resultsLimit`` is the 50,000 protocol maximum: the docs list no lower
per-type cap for this type (the SCALE census cannot probe a cap the
population never reaches — the FillUp dual-provenance lesson's other
arm, DESIGN §8), so the protocol maximum stands.

Every request here is a JSON-RPC POST whose ``params.credentials`` and
resolved host are the session strategy's injections (the devices-leaf
convention); the host this module writes is a pre-auth placeholder.
"""

from typing import Final

from fleetpull.config import GeotabConfig
from fleetpull.endpoints.geotab._requests import GeotabGetFeedSpecBuilder, server_host
from fleetpull.endpoints.shared import EndpointDefinition, FeedMode, StorageKind
from fleetpull.models.geotab import AnnotationLog
from fleetpull.network.decoders import GeotabFeedPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The GetFeed protocol maximum; no lower documented per-type cap.
_RESULTS_LIMIT: Final[int] = 50000

_ANNOTATION_LOG_TYPE_NAME: Final[str] = 'AnnotationLog'


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[AnnotationLog]:
    """Build the GeoTab annotation_logs feed binding.

    Duty-status-log annotations fetched incrementally as a version-token
    feed: the run resumes from the stored token (seeded via
    ``search.fromDate`` on the tokenless first run only), each page
    appends durably before its ``toVersion`` commits, and the fetched
    records land in ``date=YYYY-MM-DD`` partitions as new numbered part
    files — re-emitted versions accumulate for the consumer's
    ``(id, max version)`` reconcile.

    Args:
        config: The validated GeoTab configuration; supplies the
            authentication host the pre-auth spec URLs are built on.

    Returns:
        The frozen annotation_logs ``EndpointDefinition``. Construction
        validates the ``FeedMode`` / ``APPEND_LOG`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='annotation_logs',
        spec_builder=GeotabGetFeedSpecBuilder(
            server=server_host(config),
            type_name=_ANNOTATION_LOG_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabFeedPageDecoder(),
        response_model=AnnotationLog,
        quota_scope=QuotaScope.GEOTAB_FEED,
        storage_kind=StorageKind.APPEND_LOG,
        sync_mode=FeedMode(),
        event_time_column='date_time',
    )
