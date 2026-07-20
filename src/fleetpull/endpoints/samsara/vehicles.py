# src/fleetpull/endpoints/samsara/vehicles.py
"""The Samsara vehicles binding: a factory composing the vehicles snapshot
EndpointDefinition from resolved Samsara configuration, plus the
``vehicle_ids`` roster the listing feeds.

A binding cannot be a module-level constant because its spec-builder
needs the run's configured base URL, known only after the YAML config
loads; so the endpoint is a factory taking a validated ``SamsaraConfig``
and returning the frozen ``EndpointDefinition`` the composition root
hands to the client (the Motive leaf convention).

The cursor walk was proven live before this binding shipped (2026-07-17
probe session): the advance continued across a real page boundary with
no overlap or loss, and the terminal page carries ``hasNextPage: false``
beside an empty-string ``endCursor``. The walk is complete by
construction -- continuation is explicit on every page and a promised
continuation without a cursor fails loudly in the decoder -- so no
completeness check is declared (unlike GeoTab's silently capped ``Get``,
there is nothing here to silently lose). Success responses carry no
rate-limit headers (captured 2026-07-17), so the config's self-limiting
default is the only budget signal.

``VEHICLE_IDS_ROSTER`` is declared here, beside the feeder it describes:
the roster names this module's endpoint and its frame column, which is
provider-specific knowledge that belongs in the provider leaf (the
Motive vehicles precedent). Unlike the endpoint factory it needs no
config, so it is a frozen constant -- and a public one deliberately:
``build_roster_registry`` discovers public module-level
``RosterDefinition`` constants in the same walk that finds
``build_endpoint``, so declaring the constant IS the registration (no
hand-maintained list exists to drift). On inactive coverage, honestly:
the 2026-07-17 capture proves ``/fleet/vehicles`` lists unplugged units
(the minimal 7-key shape is a gateway-less unit carrying a
serial-shaped default name), so present-but-inactive vehicles stay
fanned over; whether Samsara ever delists a removed vehicle was not
probed, and the eviction hysteresis below is what retires a member the
listing stops returning.
"""

from datetime import timedelta
from typing import Final

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.models.samsara import Vehicle
from fleetpull.network.decoders import SamsaraCursorPageDecoder
from fleetpull.roster import RosterDefinition, RosterKey
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['VEHICLE_IDS_ROSTER', 'build_endpoint']

_VEHICLES_PATH: Final[str] = '/fleet/vehicles'
_RECORDS_KEY: Final[str] = 'data'

# The per-page record count. 512 is Samsara's documented list-endpoint
# maximum, and the live sweep confirmed it honored exactly (a 608-vehicle
# fleet returned 512 + 96). A strong candidate for a user config knob.
_RESULTS_LIMIT: Final[int] = 512

# The fleet's membership changes on the order of days, so a daily re-list
# keeps the roster current without spending a full vehicles listing on
# every sync (the Motive vehicle_ids policy, re-declared per provider --
# intentional cross-provider duplication, blast-radius over DRY).
_VEHICLE_IDS_MAX_AGE: Final[timedelta] = timedelta(days=1)

# Eviction hysteresis (DESIGN §3): vehicle ids are permanent, absent-
# means-empty keys, so eviction is an efficiency lever (stop fanning over
# vehicles the listing no longer returns), not a correctness one. Three
# consecutive absent listings tolerate a transient provider omission
# before dropping a member.
_VEHICLE_IDS_EVICTION_THRESHOLD: Final[int] = 3

# The Samsara vehicle_ids roster: fed by this module's vehicles listing,
# read by the trips fan-out (which carries only the RosterKey). The
# source column is the vehicles frame's 'id' (the top-level model field,
# flattened verbatim) -- a numeric string, mirrored as string.
VEHICLE_IDS_ROSTER: RosterDefinition = RosterDefinition(
    key=RosterKey(Provider.SAMSARA, 'vehicle_ids'),
    source_endpoint='vehicles',
    source_column='id',
    max_age=_VEHICLE_IDS_MAX_AGE,
    eviction_threshold=_VEHICLE_IDS_EVICTION_THRESHOLD,
)


def build_endpoint(config: SamsaraConfig) -> EndpointDefinition[Vehicle]:
    """Build the Samsara vehicles snapshot binding.

    A full-listing snapshot of the fleet's vehicles: no resume, a single
    parquet file, full replacement each run. Records arrive as a
    top-level list under ``data``, walked by explicit cursor pages
    (``limit`` on page one, ``after`` merged thereafter), terminal on
    ``hasNextPage: false``.

    Args:
        config: The validated Samsara configuration; supplies the base
            URL the spec-builder joins to the vehicles path.

    Returns:
        The frozen vehicles ``EndpointDefinition``.
    """
    return EndpointDefinition(
        provider=Provider.SAMSARA,
        name='vehicles',
        spec_builder=StaticGetSpecBuilder(
            base_url=config.base_url, path=_VEHICLES_PATH
        ),
        page_decoder=SamsaraCursorPageDecoder(
            records_key=_RECORDS_KEY, results_limit=_RESULTS_LIMIT
        ),
        response_model=Vehicle,
        quota_scope=QuotaScope.SAMSARA,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
    )
