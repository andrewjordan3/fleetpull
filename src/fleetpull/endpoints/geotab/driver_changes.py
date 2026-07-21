# src/fleetpull/endpoints/geotab/driver_changes.py
"""The GeoTab driver_changes binding: the driver-assignment feed.

A ``GetFeed`` drive of the ``DriverChange`` entity — driver-to-device
assignment events carrying a per-record ``version``. DriverChange
records are user-editable through the provider, so re-emission under
newer versions is expected, every emitted version is stored, and the
consumer reconciles by ``(id, max version)`` (DESIGN §4). The proven
object-or-string driver ref (with its ``isDriver`` object-arm sibling)
rides the model (``models/geotab/driver_change.py``).

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
from fleetpull.models.geotab import DriverChange
from fleetpull.network.decoders import GeotabFeedPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The GetFeed protocol maximum; no lower documented per-type cap.
_RESULTS_LIMIT: Final[int] = 50000

_DRIVER_CHANGE_TYPE_NAME: Final[str] = 'DriverChange'


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[DriverChange]:
    """Build the GeoTab driver_changes feed binding.

    Driver-assignment events fetched incrementally as a version-token
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
        The frozen driver_changes ``EndpointDefinition``. Construction
        validates the ``FeedMode`` / ``APPEND_LOG`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='driver_changes',
        spec_builder=GeotabGetFeedSpecBuilder(
            server=server_host(config),
            type_name=_DRIVER_CHANGE_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabFeedPageDecoder(),
        response_model=DriverChange,
        quota_scope=QuotaScope.GEOTAB_FEED,
        storage_kind=StorageKind.APPEND_LOG,
        sync_mode=FeedMode(),
        event_time_column='date_time',
    )
