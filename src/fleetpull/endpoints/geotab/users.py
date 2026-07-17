# src/fleetpull/endpoints/geotab/users.py
"""The GeoTab users binding: a factory composing the users snapshot
EndpointDefinition from resolved GeoTab configuration.

The devices pattern verbatim, bound to ``User``: id-sort seek paging is
supported for this type (proven live 2026-07-16 -- first page ascending,
the offset advance continuing past the boundary with no overlap; never
assumed from Device, since ExceptionEvent rejects id-sort outright), so
the leaf composes the shared ``_seek_walk`` machinery with a
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
from fleetpull.endpoints.geotab._seek_walk import GeotabGetSpecBuilder, GetCountOfCheck
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SnapshotMode,
    StorageKind,
)
from fleetpull.models.geotab import User
from fleetpull.network.decoders import GeotabGetPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# Pre-auth placeholder host for a default-constructed (credential-less)
# config -- mirrors GeotabAuthConfig's server default; the session
# strategy retargets every prepared request, so no request ever leaves
# for this host un-retargeted. Duplicated from the devices leaf per its
# stated colocation policy (module-private constants, deliberately
# unshared).
_DEFAULT_SERVER: Final[str] = 'my.geotab.com'

# The seek-walk page size. The 5,000 silent Get cap is per-type
# provenance and was never grazed for User (157 captured accounts);
# the walk is correct under any per-type cap (see module docstring),
# so 5000 is a page-size choice here, not a proven ceiling. A strong
# candidate for a user config knob.
_RESULTS_LIMIT: Final[int] = 5000

_USER_TYPE_NAME: Final[str] = 'User'


def _server_host(config: GeotabConfig) -> str:
    """The authentication host the spec URLs are built on.

    Args:
        config: The validated GeoTab configuration.

    Returns:
        ``auth.server`` when a credential is configured; the placeholder
        default otherwise (a credential-less config still builds every
        discovered leaf -- the registry walk requires it -- but can never
        fetch, so the placeholder never reaches the wire).
    """
    if config.auth is not None:
        return config.auth.server
    return _DEFAULT_SERVER


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
    server = _server_host(config)
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
