"""The GeoTab feed drive-through harness the five vertical test modules share.

Runs one REAL feed leaf end-to-end over synthetic envelopes: the leaf's
own spec builder composes the seeded first request, the real
``TransportClient.fetch_pages`` loop drives the real
``GeotabFeedPageDecoder`` (its token advance builds every next request),
the runner's feed arm appends each page through the real
``FeedAppendWriter`` into a temp dataset root, and the page tokens
commit through a real migrated ``CursorStore`` — only the HTTP hop is
scripted (``_fetch_single_page`` serves the canned envelopes in order,
recording every sent body so the wire shapes are assertable).

The one knob: ``page_size`` replaces the leaf's declared
``results_limit`` for the drive, because the decoder's short-page
terminal rule reads the SENT limit — synthetic fixtures page at 2 where
production declares tens of thousands (the trips-capture
``resultsLimit: 3`` precedent: the limit is the walk's parameter, not
the mechanism). The endpoint tests pin the declared production limit
separately.

Not a test module (no ``test_`` prefix); the vertical test modules
import it (the ``tests/orchestrator/doubles`` precedent).
"""

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path

from fleetpull.config import (
    FleetpullConfig,
    ProvidersConfig,
    StorageConfig,
    SyncConfig,
)
from fleetpull.endpoints.geotab._requests import GeotabGetFeedSpecBuilder
from fleetpull.endpoints.shared import EndpointDefinition
from fleetpull.incremental import IncrementalCursor
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import TransportClient
from fleetpull.network.contract import RequestSpec
from fleetpull.orchestrator.drivers import SingleRequestDriver
from fleetpull.orchestrator.outcome import RunOutcome
from fleetpull.orchestrator.runner import EndpointRunner
from fleetpull.orchestrator.spine import RunStateAccess
from fleetpull.state import (
    CursorStore,
    RunLedger,
    StateDatabase,
    WorkUnitStore,
    migrate_to_head,
)
from fleetpull.timing import FrozenClock
from fleetpull.vocabulary import JsonValue, Provider

_CLOCK_NOW = datetime(2026, 7, 21, tzinfo=UTC)

# The sync-wide cold-start anchor: the tokenless first run seeds
# search.fromDate at this date's midnight.
SEED_START_DATE = date(2024, 1, 1)


class ScriptedFeedTransport(TransportClient):
    """Serves canned envelopes through the REAL ``fetch_pages`` page loop.

    Deliberately no ``super().__init__`` (the StubClient precedent): no
    pool, no profile — the ``_fetch_single_page`` override below is the
    only network touchpoint, so the real page loop and the real decoder
    run over scripted wire bodies. Every sent JSON-RPC body is recorded
    for wire-shape assertions.
    """

    def __init__(self, envelopes: list[JsonValue]) -> None:
        self._envelopes = list(envelopes)
        self.sent_bodies: list[Mapping[str, JsonValue]] = []

    def _fetch_single_page(self, sent: RequestSpec, quota_scope: str) -> JsonValue:
        assert sent.json_body is not None, 'GeoTab requests always carry a body'
        self.sent_bodies.append(sent.json_body)
        assert self._envelopes, 'the drive requested more pages than were scripted'
        return self._envelopes.pop(0)


class _ScriptedClientSource:
    """A ClientSource handing the one scripted transport for any provider."""

    def __init__(self, transport: ScriptedFeedTransport) -> None:
        self._transport = transport

    def client_for(self, provider: Provider) -> TransportClient:
        return self._transport


@dataclass(frozen=True, slots=True)
class FeedDriveResult:
    """What one drive-through produced, ready for the vertical's asserts.

    Attributes:
        outcome: The runner's ``RunOutcome`` (an ``Executed`` on success).
        cursor: The endpoint's stored cursor after the drive — the
            committed ``FeedToken``.
        sent_bodies: Every JSON-RPC body the drive sent, in order (the
            seeded first request, then each decoder-built advance).
        endpoint_dir: The endpoint's dataset directory under the temp
            root.
    """

    outcome: RunOutcome
    cursor: IncrementalCursor | None
    sent_bodies: list[Mapping[str, JsonValue]]
    endpoint_dir: Path


def drive_feed_endpoint(
    definition: EndpointDefinition[ResponseModel],
    envelopes: list[JsonValue],
    root: Path,
    page_size: int,
) -> FeedDriveResult:
    """Drive one feed leaf through the real runner over scripted envelopes.

    Args:
        definition: The REAL leaf definition (from its ``build_endpoint``);
            only its spec builder's ``results_limit`` is replaced with
            ``page_size`` (the module docstring's one knob).
        envelopes: The canned GetFeed envelopes, in page order; the last
            must be short at ``page_size`` (the decoder's terminal rule).
        root: The temp directory holding both the dataset root and the
            state database.
        page_size: The drive's ``resultsLimit`` — the fixtures' page size.

    Returns:
        The drive's ``FeedDriveResult``.

    Raises:
        TypeError: The definition's spec builder is not the shared
            ``GeotabGetFeedSpecBuilder`` — the harness only serves feed
            leaves.
    """
    spec_builder = definition.spec_builder
    if not isinstance(spec_builder, GeotabGetFeedSpecBuilder):
        raise TypeError(
            f'drive_feed_endpoint serves GeotabGetFeedSpecBuilder leaves, '
            f'got {type(spec_builder).__name__}'
        )
    driven = replace(
        definition, spec_builder=replace(spec_builder, results_limit=page_size)
    )
    clock = FrozenClock(start_time_utc=_CLOCK_NOW)
    database = StateDatabase(root / 'state.sqlite3')
    database.initialize()
    migrate_to_head(database)
    cursors = CursorStore(database, clock)
    transport = ScriptedFeedTransport(envelopes)
    runner = EndpointRunner(
        _ScriptedClientSource(transport),
        RunStateAccess(
            recorder=RunLedger(database, clock),
            cursors=cursors,
            units=WorkUnitStore(database, clock),
        ),
        clock,
        FleetpullConfig(
            sync=SyncConfig(default_start_date=SEED_START_DATE),
            storage=StorageConfig(dataset_root=root),
            providers=ProvidersConfig(),
        ),
    )
    outcome = runner.run(driven, SingleRequestDriver())
    return FeedDriveResult(
        outcome=outcome,
        cursor=cursors.get_cursor(definition.provider, definition.name),
        sent_bodies=transport.sent_bodies,
        endpoint_dir=root / definition.provider.value / definition.name,
    )
