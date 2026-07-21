# src/fleetpull/endpoints/motive/users.py
"""The Motive users binding: a factory composing the users snapshot
EndpointDefinition from resolved Motive configuration.

The vehicles template verbatim, bound to ``/v1/users`` (probed
2026-07-21): a full-population wrapped-list snapshot on the shared
static-GET builder and the existing Motive offset-pagination decoder, at
the configured page size (``per_page`` 50 and 100 both honored live).
Only the pagination parameters are sent — the unfiltered listing is the
complete population, one dataset despite the role-partitioned record
shape: the ``role`` column carries the split (DESIGN section 8). No
roster is sourced or consumed.
"""

from typing import Final

from fleetpull.config import MotiveConfig
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.models.motive import User
from fleetpull.network.decoders import MotiveWrappedListPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

_USERS_PATH: Final[str] = '/v1/users'
_USERS_LIST_KEY: Final[str] = 'users'
_USERS_ITEM_KEY: Final[str] = 'user'


def build_endpoint(config: MotiveConfig) -> EndpointDefinition[User]:
    """Build the Motive users snapshot binding.

    A full-dataset snapshot of every account — drivers, admins, and
    fleet users: no resume, a single parquet file, full replacement each
    run. Records arrive wrapped (``{"users": [{"user": {...}}]}``) under
    page-numbered pagination, at the page size the config requests.

    Args:
        config: The validated Motive configuration; supplies the base URL
            the spec-builder joins to the users path and the page size
            the decoder requests.

    Returns:
        The frozen users ``EndpointDefinition``.
    """
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='users',
        spec_builder=StaticGetSpecBuilder(base_url=config.base_url, path=_USERS_PATH),
        page_decoder=MotiveWrappedListPageDecoder(
            list_key=_USERS_LIST_KEY,
            item_key=_USERS_ITEM_KEY,
            per_page=config.records_per_page,
        ),
        response_model=User,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
    )
