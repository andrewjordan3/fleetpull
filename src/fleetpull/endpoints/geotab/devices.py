# src/fleetpull/endpoints/geotab/devices.py
"""The GeoTab devices binding: a factory composing the devices snapshot
EndpointDefinition from resolved GeoTab configuration.

A binding cannot be a module-level constant because its spec-builder and
completeness check need the run's configured authentication host, known
only after the YAML config loads; so the endpoint is a factory taking a
validated ``GeotabConfig`` and returning the frozen
``EndpointDefinition`` the composition root hands to the client (the
Motive leaf convention).

Every request here is a JSON-RPC POST to ``https://{server}/apiv1``
whose ``params.credentials`` are injected by the session auth strategy,
never built here; the strategy also retargets each prepared request to
the session's resolved host, so the host this module writes is a
pre-auth placeholder that never reaches the wire on its own (DESIGN
section 8). The seek walk and the ``GetCountOfCheck`` truth instrument
live in the shared ``_get_requests`` module (promoted when the users
leaf became their second consumer); this leaf binds them to ``Device``.
"""

from typing import Final

from fleetpull.config import GeotabConfig
from fleetpull.endpoints.geotab._get_requests import (
    GeotabGetSpecBuilder,
    GetCountOfCheck,
    server_host,
)
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SnapshotMode,
    StorageKind,
)
from fleetpull.models.geotab import Device
from fleetpull.network.decoders import GeotabGetPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The largest sound page under Get's silent 5,000-record cap.
_RESULTS_LIMIT: Final[int] = 5000

_DEVICE_TYPE_NAME: Final[str] = 'Device'


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[Device]:
    """Build the GeoTab devices snapshot binding.

    A full-listing snapshot of the account's Device entities (tracked
    vehicles and trailer entries alike): no resume, a single parquet
    file, full replacement each run. Records arrive as a plain list
    under ``result``, walked by id-ascending seek pages under the
    silent 5,000-record ``Get`` cap, and every harvest is verified
    against ``GetCountOf`` before anything flows downstream.

    Args:
        config: The validated GeoTab configuration; supplies the
            authentication host the pre-auth spec URLs are built on.

    Returns:
        The frozen devices ``EndpointDefinition``.
    """
    server = server_host(config)
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='devices',
        spec_builder=GeotabGetSpecBuilder(
            server=server,
            type_name=_DEVICE_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabGetPageDecoder(),
        response_model=Device,
        quota_scope=QuotaScope.GEOTAB_GET,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
        completeness_check=GetCountOfCheck(server=server, type_name=_DEVICE_TYPE_NAME),
    )
