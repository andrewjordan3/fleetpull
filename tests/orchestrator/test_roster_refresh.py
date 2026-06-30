"""Tests for fleetpull.orchestrator.roster_refresh."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.endpoints import EndpointRegistry
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.exceptions import AuthenticationError, ConfigurationError
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.network.contract import (
    DecodedPage,
    JsonObject,
    JsonValue,
    PageAdvance,
    PageDecoder,
    RequestSpec,
)
from fleetpull.orchestrator.roster_refresh import RosterRefreshCoordinator
from fleetpull.roster import RosterDefinition, RosterKey
from fleetpull.state import RosterDelta
from fleetpull.timing import FrozenClock
from fleetpull.vocabulary import Provider, QuotaScope

_NOW = datetime(2026, 6, 16, 12, 0, tzinfo=UTC)
_MAX_AGE = timedelta(days=1)
_KEY = RosterKey(Provider.MOTIVE, 'vehicle_ids')


class _Vehicle(ResponseModel):
    vehicle_id: str


class _TimestampedVehicle(ResponseModel):
    vehicle_id: str
    occurred_at: datetime


class _StubPageDecoder:
    """A PageDecoder double; the canned client bypasses it, so it is never called."""

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        return spec

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        return DecodedPage(
            records=[], advance=PageAdvance(next_spec=None, durable_progress=None)
        )


class _CannedClient(TransportClient):
    """Yields canned pages; opens no real pool (no ``super().__init__``)."""

    def __init__(self, pages: list[FetchedPage]) -> None:
        self._pages = pages

    def fetch_pages(
        self, spec: RequestSpec, page_decoder: PageDecoder, quota_scope: str
    ) -> Iterator[FetchedPage]:
        yield from self._pages


class _FailingClient(TransportClient):
    """Raises on the first page fetch (no ``super().__init__``)."""

    def __init__(self) -> None:
        pass

    def fetch_pages(
        self, spec: RequestSpec, page_decoder: PageDecoder, quota_scope: str
    ) -> Iterator[FetchedPage]:
        raise AuthenticationError(detail='simulated feeder failure')


class _FakeClientSource:
    """Hands back one fixed client for any provider."""

    def __init__(self, client: TransportClient) -> None:
        self._client = client

    def client_for(self, provider: Provider) -> TransportClient:
        return self._client


class _FakeLedger:
    """Returns one fixed last-success time for any feeder."""

    def __init__(self, last_success: datetime | None) -> None:
        self._last_success = last_success

    def last_success_at(self, provider: Provider, endpoint: str) -> datetime | None:
        return self._last_success


class _FakeStore:
    """A roster store double: fixed counts, recording every applied delta."""

    def __init__(self, counts: dict[str, int]) -> None:
        self._counts = counts
        self.applied: list[tuple[RosterKey, RosterDelta]] = []

    def read_counts(self, key: RosterKey) -> dict[str, int]:
        return dict(self._counts)

    def apply(self, key: RosterKey, delta: RosterDelta) -> None:
        self.applied.append((key, delta))


def _page(records: list[JsonObject]) -> FetchedPage:
    return FetchedPage(records=records, durable_progress=None)


def _snapshot_feeder() -> EndpointDefinition[ResponseModel]:
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='vehicles',
        spec_builder=StaticGetSpecBuilder(base_url='https://api.test', path='/v'),
        page_decoder=_StubPageDecoder(),
        response_model=_Vehicle,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
    )


def _watermark_feeder() -> EndpointDefinition[ResponseModel]:
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='vehicle_locations',
        spec_builder=StaticGetSpecBuilder(base_url='https://api.test', path='/v'),
        page_decoder=_StubPageDecoder(),
        response_model=_TimestampedVehicle,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=1)),
        event_time_column='occurred_at',
    )


def _roster_definition(
    *, source_endpoint: str = 'vehicles', eviction_threshold: int | None = 3
) -> RosterDefinition:
    return RosterDefinition(
        key=_KEY,
        source_endpoint=source_endpoint,
        source_column='vehicle_id',
        max_age=_MAX_AGE,
        eviction_threshold=eviction_threshold,
    )


def _coordinator(
    *,
    feeder: EndpointDefinition[ResponseModel],
    last_success: datetime | None,
    current: dict[str, int],
    client: TransportClient,
) -> tuple[RosterRefreshCoordinator, _FakeStore]:
    store = _FakeStore(current)
    coordinator = RosterRefreshCoordinator(
        endpoint_registry=EndpointRegistry([feeder]),
        store=store,
        ledger=_FakeLedger(last_success),
        client_source=_FakeClientSource(client),
        clock=FrozenClock(start_time_utc=_NOW),
    )
    return coordinator, store


class TestRefreshIfStale:
    def test_fresh_roster_is_not_refreshed(self) -> None:
        coordinator, store = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=_NOW - timedelta(hours=1),
            current={'1': 0},
            client=_CannedClient([_page([{'vehicle_id': '9'}])]),
        )
        coordinator.refresh_if_stale(_roster_definition())
        assert store.applied == []

    def test_stale_roster_refreshes_and_applies_the_reconciled_delta(self) -> None:
        coordinator, store = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=_NOW - timedelta(days=2),
            current={'1': 0, '3': 2},
            client=_CannedClient([_page([{'vehicle_id': '1'}, {'vehicle_id': '2'}])]),
        )
        coordinator.refresh_if_stale(_roster_definition())
        assert store.applied == [
            (
                _KEY,
                RosterDelta(
                    to_zero=frozenset({'2'}),
                    to_increment=frozenset({'3'}),
                    to_evict=frozenset(),
                ),
            )
        ]

    def test_cold_start_failure_reraises(self) -> None:
        coordinator, store = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=None,
            current={},
            client=_FailingClient(),
        )
        with pytest.raises(AuthenticationError):
            coordinator.refresh_if_stale(_roster_definition())
        assert store.applied == []

    def test_existing_roster_degrades_when_harvest_fails(self) -> None:
        coordinator, store = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=_NOW - timedelta(days=2),
            current={'1': 0},
            client=_FailingClient(),
        )
        coordinator.refresh_if_stale(_roster_definition())
        assert store.applied == []

    def test_non_snapshot_feeder_raises_configuration_error(self) -> None:
        coordinator, store = _coordinator(
            feeder=_watermark_feeder(),
            last_success=None,
            current={},
            client=_CannedClient([]),
        )
        with pytest.raises(ConfigurationError):
            coordinator.refresh_if_stale(
                _roster_definition(source_endpoint='vehicle_locations')
            )
        assert store.applied == []
