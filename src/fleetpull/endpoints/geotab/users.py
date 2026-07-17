# src/fleetpull/endpoints/geotab/users.py
"""The GeoTab users binding: a factory composing the users snapshot
EndpointDefinition from resolved GeoTab configuration.

The devices pattern verbatim, bound to ``User``: id-sort seek paging is
supported for this type (proven live 2026-07-16 -- first page ascending,
the offset advance continuing past the boundary with no overlap; never
assumed from Device, since ExceptionEvent rejects id-sort outright), so
the leaf composes the shared ``_get_requests`` machinery with a
``GetCountOfCheck`` on ``User``. The captured population (157 accounts,
count == sweep) sits far under the 5,000 silent cap, but the walk is
cap-agnostic by construction: the seek advance terminates on the empty
page whatever the per-type cap turns out to be, and the completeness
check proves the harvest whole either way.

Every request here is a JSON-RPC POST whose ``params.credentials`` and
resolved host are the session strategy's injections (the devices-leaf
convention); the host this module writes is a pre-auth placeholder.
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
from fleetpull.models.geotab import User
from fleetpull.network.decoders import GeotabGetPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The seek-walk page size. The 5,000 silent Get cap is per-type
# provenance and was never grazed for User (157 captured accounts);
# the walk is correct under any per-type cap (see module docstring),
# so 5000 is a page-size choice here, not a proven ceiling. A strong
# candidate for a user config knob.
_RESULTS_LIMIT: Final[int] = 5000

_USER_TYPE_NAME: Final[str] = 'User'


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[User]:
    """Build the GeoTab users snapshot binding.

    A full-listing snapshot of the account's User entities (drivers and
    console accounts alike): no resume, a single parquet file, full
    replacement each run. Records arrive as a plain list under
    ``result``, walked by id-ascending seek pages, and every harvest is
    verified against ``GetCountOf`` before anything flows downstream.

    Args:
        config: The validated GeoTab configuration; supplies the
            authentication host the pre-auth spec URLs are built on.

    Returns:
        The frozen users ``EndpointDefinition``.
    """
    server = server_host(config)
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='users',
        spec_builder=GeotabGetSpecBuilder(
            server=server,
            type_name=_USER_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabGetPageDecoder(),
        response_model=User,
        quota_scope=QuotaScope.GEOTAB_GET,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
        completeness_check=GetCountOfCheck(server=server, type_name=_USER_TYPE_NAME),
    )
