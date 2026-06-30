# src/fleetpull/endpoints/motive/vehicles.py
"""The Motive vehicles binding: a factory composing the vehicles snapshot
EndpointDefinition from resolved Motive configuration.

A binding cannot be a module-level constant because its spec-builder
needs the run's configured base URL and page size, known only after the
YAML config loads; capturing config at import time would freeze a
default, and module-level mutable state is forbidden. So the endpoint is
a factory taking a validated ``MotiveConfig`` and returning the frozen
``EndpointDefinition`` the composition root hands to the client.
"""

import logging

from fleetpull.config import MotiveConfig
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.models.motive import Vehicle
from fleetpull.network.decoders import MotiveWrappedListPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

logger = logging.getLogger(__name__)

_VEHICLES_PATH: str = '/v1/vehicles'
_VEHICLES_LIST_KEY: str = 'vehicles'
_VEHICLES_ITEM_KEY: str = 'vehicle'


def build_endpoint(config: MotiveConfig) -> EndpointDefinition[Vehicle]:
    """Build the Motive vehicles snapshot binding.

    A full-dataset snapshot of the fleet's vehicles: no resume, a single
    parquet file, full replacement each run. Records arrive wrapped
    (``{"vehicles": [{"vehicle": {...}}]}``) under page-numbered
    pagination, at the page size the config requests.

    Only the pagination parameters are sent. The endpoint's optional
    filters -- ``driver_ids[]``, ``fuel_type``, ``updated_after``, and the
    ``X-Time-Zone`` / ``X-Metric-Units`` / ``X-User-Id`` header params --
    are deliberately not wired yet.

    Args:
        config: The validated Motive configuration; supplies the base URL
            the spec-builder joins to the vehicles path and the page size
            the decoder requests.

    Returns:
        The frozen vehicles ``EndpointDefinition``.
    """
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='vehicles',
        spec_builder=StaticGetSpecBuilder(
            base_url=config.base_url, path=_VEHICLES_PATH
        ),
        page_decoder=MotiveWrappedListPageDecoder(
            list_key=_VEHICLES_LIST_KEY,
            item_key=_VEHICLES_ITEM_KEY,
            per_page=config.records_per_page,
        ),
        response_model=Vehicle,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
    )
