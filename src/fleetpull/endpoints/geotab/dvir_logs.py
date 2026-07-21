# src/fleetpull/endpoints/geotab/dvir_logs.py
"""The GeoTab dvir_logs binding: the driver vehicle inspection feed.

A ``GetFeed`` drive of the ``DVIRLog`` entity (the wire's own casing —
the model is the house-cased ``DvirLog``). DVIRs are certified and
edited after creation, so past records re-emit under higher ``version``
tokens, every emitted version is stored, and the consumer reconciles by
``(id, max version)`` (DESIGN §4). The wave-two census facts (the
commonly-absent ``device`` trio, the ``defectList`` children exclusion,
the shared nested-location pair) ride the model
(``models/geotab/dvir_log.py``).

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
from fleetpull.models.geotab import DvirLog
from fleetpull.network.decoders import GeotabFeedPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The GetFeed protocol maximum; no lower documented per-type cap.
_RESULTS_LIMIT: Final[int] = 50000

# The wire typeName keeps the provider's own DVIR casing; only the
# model class is house-cased.
_DVIR_LOG_TYPE_NAME: Final[str] = 'DVIRLog'


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[DvirLog]:
    """Build the GeoTab dvir_logs feed binding.

    Inspection reports fetched incrementally as a version-token feed:
    the run resumes from the stored token (seeded via
    ``search.fromDate`` on the tokenless first run only), each page
    appends durably before its ``toVersion`` commits, and the fetched
    records land in ``date=YYYY-MM-DD`` partitions as new numbered part
    files — re-emitted versions accumulate for the consumer's
    ``(id, max version)`` reconcile.

    Args:
        config: The validated GeoTab configuration; supplies the
            authentication host the pre-auth spec URLs are built on.

    Returns:
        The frozen dvir_logs ``EndpointDefinition``. Construction
        validates the ``FeedMode`` / ``APPEND_LOG`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='dvir_logs',
        spec_builder=GeotabGetFeedSpecBuilder(
            server=server_host(config),
            type_name=_DVIR_LOG_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabFeedPageDecoder(),
        response_model=DvirLog,
        quota_scope=QuotaScope.GEOTAB_FEED,
        storage_kind=StorageKind.APPEND_LOG,
        sync_mode=FeedMode(),
        event_time_column='date_time',
    )
