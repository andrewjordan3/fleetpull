# src/fleetpull/endpoints/geotab/text_messages.py
"""The GeoTab text_messages binding: the dispatch-message feed.

A ``GetFeed`` drive of the ``TextMessage`` entity — office↔vehicle
dispatch messages. TextMessage carries NO per-record ``version`` (the
FaultData/LogRecord asymmetry, DESIGN §8): the append-only cell is
trivially complete and the consumer reconciles by ``id`` alone
(DESIGN §4). Delivered/read receipts re-emit a message under newer FEED
``toVersion`` tokens and are stored-as-emitted — the feed's own
versioning, not a per-record ``version`` key.

TextMessage also carries NO ``dateTime`` key: the event-time identity is
``sent`` (the send instant, 25,000/25,000), so this leaf anchors
``event_time_column='sent'`` rather than the ``date_time`` its feed
siblings use (``models/geotab/text_message.py``).

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
from fleetpull.models.geotab import TextMessage
from fleetpull.network.decoders import GeotabFeedPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The GetFeed protocol maximum; no lower documented per-type cap.
_RESULTS_LIMIT: Final[int] = 50000

_TEXT_MESSAGE_TYPE_NAME: Final[str] = 'TextMessage'


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[TextMessage]:
    """Build the GeoTab text_messages feed binding.

    Dispatch messages fetched incrementally as a version-token feed: the
    run resumes from the stored token (seeded via ``search.fromDate`` on
    the tokenless first run only), each page appends durably before its
    ``toVersion`` commits, and the fetched records land in
    ``date=YYYY-MM-DD`` partitions (by ``sent``, the event time) as new
    numbered part files.

    Args:
        config: The validated GeoTab configuration; supplies the
            authentication host the pre-auth spec URLs are built on.

    Returns:
        The frozen text_messages ``EndpointDefinition``. Construction
        validates the ``FeedMode`` / ``APPEND_LOG`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='text_messages',
        spec_builder=GeotabGetFeedSpecBuilder(
            server=server_host(config),
            type_name=_TEXT_MESSAGE_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabFeedPageDecoder(),
        response_model=TextMessage,
        quota_scope=QuotaScope.GEOTAB_FEED,
        storage_kind=StorageKind.APPEND_LOG,
        sync_mode=FeedMode(),
        event_time_column='sent',
    )
