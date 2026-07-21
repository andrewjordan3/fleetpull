# src/fleetpull/endpoints/motive/groups.py
"""The Motive groups binding: a factory composing the groups snapshot
EndpointDefinition from resolved Motive configuration.

The vehicles template verbatim, bound to ``/v1/groups`` (probed
2026-07-21): a full-population wrapped-list snapshot on the shared
static-GET builder and the existing Motive offset-pagination decoder, at
the configured page size (``per_page`` 50 and 100 both honored live). No
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
from fleetpull.models.motive import Group
from fleetpull.network.decoders import MotiveWrappedListPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

_GROUPS_PATH: Final[str] = '/v1/groups'
_GROUPS_LIST_KEY: Final[str] = 'groups'
_GROUPS_ITEM_KEY: Final[str] = 'group'


def build_endpoint(config: MotiveConfig) -> EndpointDefinition[Group]:
    """Build the Motive groups snapshot binding.

    A full-dataset snapshot of the company's group tree: no resume, a
    single parquet file, full replacement each run. Records arrive
    wrapped (``{"groups": [{"group": {...}}]}``) under page-numbered
    pagination, at the page size the config requests.

    Args:
        config: The validated Motive configuration; supplies the base URL
            the spec-builder joins to the groups path and the page size
            the decoder requests.

    Returns:
        The frozen groups ``EndpointDefinition``.
    """
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='groups',
        spec_builder=StaticGetSpecBuilder(base_url=config.base_url, path=_GROUPS_PATH),
        page_decoder=MotiveWrappedListPageDecoder(
            list_key=_GROUPS_LIST_KEY,
            item_key=_GROUPS_ITEM_KEY,
            per_page=config.records_per_page,
        ),
        response_model=Group,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
    )
