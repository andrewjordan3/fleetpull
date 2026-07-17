# src/fleetpull/endpoints/motive/vehicles.py
"""The Motive vehicles binding: a factory composing the vehicles snapshot
EndpointDefinition from resolved Motive configuration, plus the
``vehicle_ids`` roster the listing feeds.

A binding cannot be a module-level constant because its spec-builder
needs the run's configured base URL and page size, known only after the
YAML config loads; capturing config at import time would freeze a
default, and module-level mutable state is forbidden. So the endpoint is
a factory taking a validated ``MotiveConfig`` and returning the frozen
``EndpointDefinition`` the composition root hands to the client.

``VEHICLE_IDS_ROSTER`` is declared here, beside the feeder it describes:
the roster names this module's endpoint and its frame column, which is
provider-specific knowledge that belongs in the provider leaf. Unlike the
endpoint factory it needs no config, so it is a frozen constant -- and a
public one deliberately: ``build_roster_registry`` discovers public
module-level ``RosterDefinition`` constants in the same walk that finds
``build_endpoint``, so declaring the constant IS the registration
(no hand-maintained list exists to drift).
The include-inactive guarantee binds at the feeder population, not at
eviction policy: ``/v1/vehicles`` lists inactive and retired vehicles, so
a fan-out over this roster covers vehicles that were active during a
historical window even if they are inactive today.
"""

from datetime import timedelta
from typing import Final

from fleetpull.config import MotiveConfig
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.models.motive import Vehicle
from fleetpull.network.decoders import MotiveWrappedListPageDecoder
from fleetpull.roster import RosterDefinition, RosterKey
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['VEHICLE_IDS_ROSTER', 'build_endpoint']

_VEHICLES_PATH: Final[str] = '/v1/vehicles'
_VEHICLES_LIST_KEY: Final[str] = 'vehicles'
_VEHICLES_ITEM_KEY: Final[str] = 'vehicle'

# The fleet's membership changes on the order of days, so a daily re-list
# keeps the roster current without spending a full vehicles listing on
# every sync.
_VEHICLE_IDS_MAX_AGE: Final[timedelta] = timedelta(days=1)

# Eviction hysteresis (DESIGN §3): vehicle ids are permanent, absent-means-
# empty keys, so eviction is an efficiency lever (stop fanning over
# long-retired vehicles), not a correctness one. Three consecutive absent
# listings tolerate a transient provider omission before dropping a member.
_VEHICLE_IDS_EVICTION_THRESHOLD: Final[int] = 3

# The Motive vehicle_ids roster: fed by this module's vehicles listing, read
# by the vehicle_locations fan-out (which carries only the RosterKey).
VEHICLE_IDS_ROSTER: RosterDefinition = RosterDefinition(
    key=RosterKey(Provider.MOTIVE, 'vehicle_ids'),
    source_endpoint='vehicles',
    source_column='vehicle_id',
    max_age=_VEHICLE_IDS_MAX_AGE,
    eviction_threshold=_VEHICLE_IDS_EVICTION_THRESHOLD,
)


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
