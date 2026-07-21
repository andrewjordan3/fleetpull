# src/fleetpull/endpoints/geotab/fill_ups.py
"""The GeoTab fill_ups binding: the first calculated feed vertical.

A ``GetFeed`` drive of the ``FillUp`` entity — provider-detected
fuel-stop events on a CALCULATED feed: past records re-emit under
higher versions on reprocessing, every emitted version is stored, and
the consumer reconciles by ``(id, max version)`` (DESIGN §4). The
estimates-only-tenant caveat rides the model
(``models/geotab/fill_up.py``): the probed tenant has no
fuel-transaction integration, so every fuel value is provider-derived
and the census cannot speak for integrated tenants.

``resultsLimit`` is 10,000 with DUAL PROVENANCE (DESIGN §8, the
2026-07-21 feed wave block): 10,000 is the DOCUMENTED per-type cap for
FillUp, and the probe could not falsify it — a 50,000 request was
ACCEPTED at this tenant's whole 380-record population, which proves
nothing about the cap (the population never reaches it). Encoding the
documented figure is the conservative arm of encode-probed-behavior:
the probe was structurally unable to test the limit, so the documented
cap stands until a tenant with >10,000 records probes it.

Every request here is a JSON-RPC POST whose ``params.credentials`` and
resolved host are the session strategy's injections (the devices-leaf
convention); the host this module writes is a pre-auth placeholder.
"""

from typing import Final

from fleetpull.config import GeotabConfig
from fleetpull.endpoints.geotab._requests import GeotabGetFeedSpecBuilder, server_host
from fleetpull.endpoints.shared import EndpointDefinition, FeedMode, StorageKind
from fleetpull.models.geotab import FillUp
from fleetpull.network.decoders import GeotabFeedPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The DOCUMENTED FillUp cap — unprobeable at this tenant's population
# (module docstring: the dual-provenance record).
_RESULTS_LIMIT: Final[int] = 10000

_FILL_UP_TYPE_NAME: Final[str] = 'FillUp'


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[FillUp]:
    """Build the GeoTab fill_ups feed binding.

    Fuel-stop detections fetched incrementally as a version-token feed:
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
        The frozen fill_ups ``EndpointDefinition``. Construction
        validates the ``FeedMode`` / ``APPEND_LOG`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='fill_ups',
        spec_builder=GeotabGetFeedSpecBuilder(
            server=server_host(config),
            type_name=_FILL_UP_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabFeedPageDecoder(),
        response_model=FillUp,
        quota_scope=QuotaScope.GEOTAB_FEED,
        storage_kind=StorageKind.APPEND_LOG,
        sync_mode=FeedMode(),
        event_time_column='date_time',
    )
