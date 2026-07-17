# src/fleetpull/endpoints/samsara/vehicles.py
"""The Samsara vehicles binding: a factory composing the vehicles snapshot
EndpointDefinition from resolved Samsara configuration.

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
"""

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
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

_VEHICLES_PATH: Final[str] = '/fleet/vehicles'
_RECORDS_KEY: Final[str] = 'data'

# The per-page record count. 512 is Samsara's documented list-endpoint
# maximum, and the live sweep confirmed it honored exactly (a 608-vehicle
# fleet returned 512 + 96). A strong candidate for a user config knob.
_RESULTS_LIMIT: Final[int] = 512


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
