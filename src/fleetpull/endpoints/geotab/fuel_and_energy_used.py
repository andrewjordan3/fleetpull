# src/fleetpull/endpoints/geotab/fuel_and_energy_used.py
"""The GeoTab fuel_and_energy_used binding: per-trip fuel/energy totals.

A ``GetFeed`` drive of the ``FuelAndEnergyUsed`` entity — a CALCULATED
feed reconciled by ``(id, max version)`` (DESIGN §4). The name is the
WIRE'S OWN VOCABULARY, not a plural (the driver_idle_rollups
precedent): ``FuelAndEnergyUsed`` names a quantity, not a countable
entity, so no snake-plural exists to form — the endpoint mirrors the
type name verbatim (DESIGN §8, the 2026-07-21 feed wave block).

``FuelUsed`` is NOT ported: observed identical to this surface on the
probed tenant (same ids, same values, week-wide) and
provider-documented as this type's predecessor — the model docstring
carries the record. The estimates-only-tenant caveat rides the model
as well (``models/geotab/fuel_and_energy_used.py``).

Every request here is a JSON-RPC POST whose ``params.credentials`` and
resolved host are the session strategy's injections (the devices-leaf
convention); the host this module writes is a pre-auth placeholder.
"""

from typing import Final

from fleetpull.config import GeotabConfig
from fleetpull.endpoints.geotab._requests import GeotabGetFeedSpecBuilder, server_host
from fleetpull.endpoints.shared import EndpointDefinition, FeedMode, StorageKind
from fleetpull.models.geotab import FuelAndEnergyUsed
from fleetpull.network.decoders import GeotabFeedPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The GetFeed protocol maximum (no lower per-type cap documented or
# observed for this type).
_RESULTS_LIMIT: Final[int] = 50000

_FUEL_AND_ENERGY_USED_TYPE_NAME: Final[str] = 'FuelAndEnergyUsed'


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[FuelAndEnergyUsed]:
    """Build the GeoTab fuel_and_energy_used feed binding.

    Per-trip fuel/energy totals fetched incrementally as a
    version-token feed: the run resumes from the stored token (seeded
    via ``search.fromDate`` on the tokenless first run only), each page
    appends durably before its ``toVersion`` commits, and the fetched
    records land in ``date=YYYY-MM-DD`` partitions as new numbered part
    files — re-emitted versions accumulate for the consumer's
    ``(id, max version)`` reconcile.

    Args:
        config: The validated GeoTab configuration; supplies the
            authentication host the pre-auth spec URLs are built on.

    Returns:
        The frozen fuel_and_energy_used ``EndpointDefinition``.
        Construction validates the ``FeedMode`` / ``APPEND_LOG`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='fuel_and_energy_used',
        spec_builder=GeotabGetFeedSpecBuilder(
            server=server_host(config),
            type_name=_FUEL_AND_ENERGY_USED_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabFeedPageDecoder(),
        response_model=FuelAndEnergyUsed,
        quota_scope=QuotaScope.GEOTAB_FEED,
        storage_kind=StorageKind.APPEND_LOG,
        sync_mode=FeedMode(),
        event_time_column='date_time',
    )
