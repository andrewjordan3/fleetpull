# src/fleetpull/endpoints/geotab/fuel_tax_details.py
"""The GeoTab fuel_tax_details binding: IFTA jurisdiction segments.

A ``GetFeed`` drive of the ``FuelTaxDetail`` entity — a CALCULATED feed
of per-jurisdiction travel segments, stored as emitted and reconciled
by ``(id, max version)``; this type's version identity is the
``versions`` LIST of component tokens rather than a scalar ``version``
(the model docstring, DESIGN §8). The estimates-only-tenant caveat
rides the model (``models/geotab/fuel_tax_detail.py``).

The event-time column is ``enter_time``: a segment materializes where
the vehicle enters the jurisdiction, so its enter instant is the row's
one event-time identity (``exit_time`` merely closes the interval —
the routing choice mirrors the interval-endpoint reasoning the trips
binding records for ``stop``, at the opposite end because THIS
entity's identity anchors on entry).

Every request here is a JSON-RPC POST whose ``params.credentials`` and
resolved host are the session strategy's injections (the devices-leaf
convention); the host this module writes is a pre-auth placeholder.
"""

from typing import Final

from fleetpull.config import GeotabConfig
from fleetpull.endpoints.geotab._requests import GeotabGetFeedSpecBuilder, server_host
from fleetpull.endpoints.shared import EndpointDefinition, FeedMode, StorageKind
from fleetpull.models.geotab import FuelTaxDetail
from fleetpull.network.decoders import GeotabFeedPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The GetFeed protocol maximum (no lower per-type cap documented or
# observed for this type).
_RESULTS_LIMIT: Final[int] = 50000

_FUEL_TAX_DETAIL_TYPE_NAME: Final[str] = 'FuelTaxDetail'


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[FuelTaxDetail]:
    """Build the GeoTab fuel_tax_details feed binding.

    IFTA jurisdiction segments fetched incrementally as a version-token
    feed: the run resumes from the stored token (seeded via
    ``search.fromDate`` on the tokenless first run only), each page
    appends durably before its ``toVersion`` commits, and the fetched
    records land in ``date=YYYY-MM-DD`` partitions (by ``enter_time``)
    as new numbered part files — re-emitted versions accumulate for the
    consumer's ``(id, max version)`` reconcile.

    Args:
        config: The validated GeoTab configuration; supplies the
            authentication host the pre-auth spec URLs are built on.

    Returns:
        The frozen fuel_tax_details ``EndpointDefinition``. Construction
        validates the ``FeedMode`` / ``APPEND_LOG`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='fuel_tax_details',
        spec_builder=GeotabGetFeedSpecBuilder(
            server=server_host(config),
            type_name=_FUEL_TAX_DETAIL_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabFeedPageDecoder(),
        response_model=FuelTaxDetail,
        quota_scope=QuotaScope.GEOTAB_FEED,
        storage_kind=StorageKind.APPEND_LOG,
        sync_mode=FeedMode(),
        event_time_column='enter_time',
    )
