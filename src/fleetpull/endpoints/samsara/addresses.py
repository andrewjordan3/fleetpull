# src/fleetpull/endpoints/samsara/addresses.py
"""The Samsara addresses binding: a factory composing the addresses
snapshot EndpointDefinition from resolved Samsara configuration.

A binding cannot be a module-level constant because its spec-builder
needs the run's configured base URL, known only after the YAML config
loads; so the endpoint is a factory taking a validated ``SamsaraConfig``
and returning the frozen ``EndpointDefinition`` the composition root
hands to the client (the Motive leaf convention).

The vehicles template verbatim (2026-07-20 probe session): a plain
snapshot on the standard Samsara cursor contract -- ``data`` beside
``pagination {endCursor, hasNextPage}`` -- with no roster sourced, no
roster consumed, and no window. The full walk was the whole population
in one page (25 records), and the walk is complete by construction:
continuation is explicit on every page and a promised continuation
without a cursor fails loudly in the decoder, so no completeness check
is declared.
"""

from typing import Final

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.models.samsara import Address
from fleetpull.network.decoders import SamsaraCursorPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

_ADDRESSES_PATH: Final[str] = '/addresses'
_RECORDS_KEY: Final[str] = 'data'

# The per-page record count. The limit tier was probed directly on THIS
# endpoint (2026-07-20): limit=512 returned HTTP 200 and limit=513 a
# loud HTTP 400, so /addresses sits in the vehicles/drivers 512 tier --
# NOT the 200 tier of /idling/events. Samsara limit maxima are
# per-endpoint (the idling_events capture's rule): never assume a
# sibling's tier. A strong candidate for a user config knob.
_RESULTS_LIMIT: Final[int] = 512


def build_endpoint(config: SamsaraConfig) -> EndpointDefinition[Address]:
    """Build the Samsara addresses snapshot binding.

    A full-listing snapshot of the fleet's defined addresses (named
    locations with geofences): no resume, a single parquet file, full
    replacement each run. Records arrive as a top-level list under
    ``data``, walked by explicit cursor pages (``limit`` on page one,
    ``after`` merged thereafter), terminal on ``hasNextPage: false``.

    Args:
        config: The validated Samsara configuration; supplies the base
            URL the spec-builder joins to the addresses path.

    Returns:
        The frozen addresses ``EndpointDefinition``.
    """
    return EndpointDefinition(
        provider=Provider.SAMSARA,
        name='addresses',
        spec_builder=StaticGetSpecBuilder(
            base_url=config.base_url, path=_ADDRESSES_PATH
        ),
        page_decoder=SamsaraCursorPageDecoder(
            records_key=_RECORDS_KEY, results_limit=_RESULTS_LIMIT
        ),
        response_model=Address,
        quota_scope=QuotaScope.SAMSARA,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
    )
