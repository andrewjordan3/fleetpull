# src/fleetpull/endpoints/geotab/duty_status_logs.py
"""The GeoTab duty_status_logs binding: the HOS duty-status feed.

A ``GetFeed`` drive of the ``DutyStatusLog`` entity — an EDITABLE log:
``editDateTime`` is the edit trail, past records re-emit under higher
``version`` tokens as they are edited and verified, every emitted
version is stored, and the consumer reconciles by ``(id, max version)``
(DESIGN §4 — the calculated-feed consumer note from the fill_ups
binding applies verbatim). The wave-two census facts (the proven
mixed device/driver refs, the annotations id-list reduction, the
shared nested-location pair) ride the model
(``models/geotab/duty_status_log.py``).

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
from fleetpull.models.geotab import DutyStatusLog
from fleetpull.network.decoders import GeotabFeedPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The GetFeed protocol maximum; no lower documented per-type cap.
_RESULTS_LIMIT: Final[int] = 50000

_DUTY_STATUS_LOG_TYPE_NAME: Final[str] = 'DutyStatusLog'


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[DutyStatusLog]:
    """Build the GeoTab duty_status_logs feed binding.

    HOS duty-status events fetched incrementally as a version-token
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
        The frozen duty_status_logs ``EndpointDefinition``. Construction
        validates the ``FeedMode`` / ``APPEND_LOG`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='duty_status_logs',
        spec_builder=GeotabGetFeedSpecBuilder(
            server=server_host(config),
            type_name=_DUTY_STATUS_LOG_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabFeedPageDecoder(),
        response_model=DutyStatusLog,
        quota_scope=QuotaScope.GEOTAB_FEED,
        storage_kind=StorageKind.APPEND_LOG,
        sync_mode=FeedMode(),
        event_time_column='date_time',
    )
