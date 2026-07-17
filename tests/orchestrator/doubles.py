"""Shared test doubles for the orchestrator suite.

The stubs and helpers the orchestrator test modules would otherwise each
carry a private copy of: hollow transport pieces the canned drivers never
call, canned and failing request drivers, the migrated work-unit-store
builder, and the determinism helpers for byte-stable parquet comparisons.
Doubles that genuinely differ per module (page-serving clients, recording
recorders) stay local to their tests.

Not a test module (no ``test_`` prefix); orchestrator test modules import it.
"""

import itertools
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest

from fleetpull.endpoints.shared import EndpointDefinition, ResumeValue
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.network.contract import DecodedPage, PageAdvance, RequestSpec
from fleetpull.state import StateDatabase, WorkUnitStore, migrate_to_head
from fleetpull.timing import Clock
from fleetpull.vocabulary import JsonObject, JsonValue, Provider


class StubPageDecoder:
    """A PageDecoder double; the canned drivers and clients bypass it, so it
    is never called.
    """

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        return spec

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        return DecodedPage(
            records=[], advance=PageAdvance(next_spec=None, durable_progress=None)
        )


class StubClient(TransportClient):
    """A hollow client; the canned driver never calls it (no ``super().__init__``)."""

    def __init__(self) -> None:
        pass


class StubClientSource:
    """A ClientSource handing a hollow client for any provider."""

    def __init__(self) -> None:
        self._client = StubClient()

    def client_for(self, provider: Provider) -> TransportClient:
        return self._client


class CannedDriver:
    """A RequestDriver yielding pre-set record pages, ignoring the client."""

    def __init__(self, batches: list[list[JsonObject]]) -> None:
        self._batches = batches

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[FetchedPage]:
        for batch in self._batches:
            yield FetchedPage(records=batch, durable_progress=None)


class FailingDriver:
    """A RequestDriver that raises as soon as it is driven."""

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[FetchedPage]:
        raise RuntimeError('fetch blew up')


def open_work_unit_store(root: Path, clock: Clock) -> WorkUnitStore:
    """A real work-unit store over ``root``'s migrated state database.

    Creates ``root / 'state.sqlite3'`` if absent, initializes it, and migrates
    to head -- the choreography every runner-level test repeats.
    """
    database = StateDatabase(root / 'state.sqlite3')
    database.initialize()
    migrate_to_head(database)
    return WorkUnitStore(database, clock)


def pin_shard_names(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the uuid shard names with a fresh deterministic counter.

    Shard files are uuid-named and compaction folds them in sorted-name
    order, so byte-stability across runs requires pinning; a monotone
    counter preserves each partition's insertion order.
    """
    counter = itertools.count()
    monkeypatch.setattr(
        'fleetpull.storage.files.uuid4',
        lambda: SimpleNamespace(hex=f'{next(counter):08d}'),
    )


def partition_bytes(endpoint_dir: Path) -> dict[str, bytes]:
    """Each date partition's ``part.parquet`` bytes, keyed by partition name."""
    return {
        part_file.parent.name: part_file.read_bytes()
        for part_file in sorted(endpoint_dir.glob('date=*/part.parquet'))
    }
