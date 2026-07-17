"""Tests for fleetpull.orchestrator.roster_refresh."""

import logging
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from fleetpull.endpoints import EndpointRegistry
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.exceptions import (
    AuthenticationError,
    ConfigurationError,
    ProviderResponseError,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.network.contract import PageDecoder, RequestSpec
from fleetpull.orchestrator.roster_refresh import RosterRefreshCoordinator
from fleetpull.records import extract_roster_members
from fleetpull.roster import RosterDefinition, RosterKey
from fleetpull.state import RosterDelta
from fleetpull.timing import FrozenClock
from fleetpull.vocabulary import JsonObject, Provider, QuotaScope
from tests.orchestrator.doubles import StubPageDecoder

_NOW = datetime(2026, 6, 16, 12, 0, tzinfo=UTC)
_MAX_AGE = timedelta(days=1)
_KEY = RosterKey(Provider.MOTIVE, 'vehicle_ids')


class _Vehicle(ResponseModel):
    vehicle_id: str


class _TimestampedVehicle(ResponseModel):
    vehicle_id: str
    occurred_at: datetime


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
    """A FeederRunLedger double recording the harvest run lifecycle.

    Completing a run advances ``last_success_at`` to the test clock instant,
    mirroring the real ledger's max(ended_at)-over-succeeded-runs read -- the
    property the consecutive-refresh regression rests on.
    """

    def __init__(self, last_success: datetime | None) -> None:
        self._last_success = last_success
        self.started: list[tuple[Provider, str]] = []
        self.completed: list[tuple[int, int]] = []
        self.failed: list[tuple[int, str]] = []

    def last_success_at(self, provider: Provider, endpoint: str) -> datetime | None:
        return self._last_success

    def start_snapshot_run(self, provider: Provider, endpoint: str) -> int:
        self.started.append((provider, endpoint))
        return len(self.started)

    def complete_run(self, run_id: int, *, row_count: int) -> None:
        self.completed.append((run_id, row_count))
        self._last_success = _NOW

    def fail_run(self, run_id: int, *, error_detail: str) -> None:
        self.failed.append((run_id, error_detail))


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
        page_decoder=StubPageDecoder(),
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
        page_decoder=StubPageDecoder(),
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
) -> tuple[RosterRefreshCoordinator, _FakeStore, _FakeLedger]:
    store = _FakeStore(current)
    ledger = _FakeLedger(last_success)
    coordinator = RosterRefreshCoordinator(
        endpoint_registry=EndpointRegistry([feeder]),
        store=store,
        ledger=ledger,
        client_source=_FakeClientSource(client),
        clock=FrozenClock(start_time_utc=_NOW),
    )
    return coordinator, store, ledger


class TestRefreshIfStale:
    def test_fresh_roster_is_not_refreshed(self) -> None:
        coordinator, store, ledger = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=_NOW - timedelta(hours=1),
            current={'1': 0},
            client=_CannedClient([_page([{'vehicle_id': '9'}])]),
        )
        coordinator.refresh_if_stale(_roster_definition())
        assert store.applied == []
        assert ledger.started == []

    def test_stale_roster_refreshes_and_applies_the_reconciled_delta(self) -> None:
        coordinator, store, _ = _coordinator(
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

    def test_successful_harvest_records_a_completed_run(self) -> None:
        coordinator, _, ledger = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=_NOW - timedelta(days=2),
            current={'1': 0},
            client=_CannedClient([_page([{'vehicle_id': '1'}, {'vehicle_id': '2'}])]),
        )
        coordinator.refresh_if_stale(_roster_definition())
        assert ledger.started == [(Provider.MOTIVE, 'vehicles')]
        # A harvest run's row_count is the distinct-member count of the listing.
        assert ledger.completed == [(1, 2)]
        assert ledger.failed == []

    def test_consecutive_refreshes_harvest_once(self) -> None:
        # The regression that would have caught the freshness defect: the
        # first refresh_if_stale harvests AND records a run the staleness key
        # can see, so the second call is a no-op -- before the coupling, the
        # harvest was invisible to the ledger and every call re-listed.
        coordinator, store, ledger = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=_NOW - timedelta(days=2),
            current={'1': 0},
            client=_CannedClient([_page([{'vehicle_id': '1'}])]),
        )
        coordinator.refresh_if_stale(_roster_definition())
        coordinator.refresh_if_stale(_roster_definition())
        assert ledger.started == [(Provider.MOTIVE, 'vehicles')]
        assert len(store.applied) == 1

    def test_empty_roster_with_fresh_ledger_refreshes(self) -> None:
        # An empty stored roster is stale regardless of the ledger verdict:
        # ledger history predating the harvest/ledger coupling (or a feeder
        # run from before the roster existed) must not mask a roster with
        # nothing to fan out over.
        coordinator, store, ledger = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=_NOW - timedelta(hours=1),
            current={},
            client=_CannedClient([_page([{'vehicle_id': '1'}])]),
        )
        coordinator.refresh_if_stale(_roster_definition())
        assert len(store.applied) == 1
        assert ledger.completed == [(1, 1)]

    def test_cold_start_failure_records_failed_and_reraises(self) -> None:
        coordinator, store, ledger = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=None,
            current={},
            client=_FailingClient(),
        )
        with pytest.raises(AuthenticationError):
            coordinator.refresh_if_stale(_roster_definition())
        assert store.applied == []
        assert ledger.completed == []
        assert len(ledger.failed) == 1

    def test_existing_roster_degrades_when_harvest_fails(self) -> None:
        coordinator, store, ledger = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=_NOW - timedelta(days=2),
            current={'1': 0},
            client=_FailingClient(),
        )
        coordinator.refresh_if_stale(_roster_definition())
        assert store.applied == []
        assert ledger.completed == []
        assert len(ledger.failed) == 1

    def test_non_snapshot_feeder_raises_before_any_run_row(self) -> None:
        coordinator, store, ledger = _coordinator(
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
        assert ledger.started == []


class TestApplyListing:
    def test_reconciles_unconditionally_with_no_staleness_consult(self) -> None:
        # A fresh ledger does not gate the feeder tap: an executed feeder
        # run's listing is always reconciled (rule: staleness gates only
        # whether the coordinator initiates a harvest).
        coordinator, store, _ = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=_NOW - timedelta(hours=1),
            current={'1': 0, '3': 2},
            client=_CannedClient([]),
        )
        coordinator.apply_listing(_roster_definition(), {'1', '2'})
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

    def test_records_no_ledger_row(self) -> None:
        # The run that produced the listing was already recorded by the run
        # executor; the tap handoff must not double-record it.
        coordinator, _, ledger = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=None,
            current={},
            client=_CannedClient([]),
        )
        coordinator.apply_listing(_roster_definition(), {'1'})
        assert ledger.started == []
        assert ledger.completed == []


class TestReconcileGuard:
    """The reconcile guard: a roster is never reconciled to empty (Part D)."""

    def test_apply_listing_rejects_a_zero_record_listing(self) -> None:
        # Trigger shape 1: the feeder listed nothing at all.
        coordinator, store, ledger = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=None,
            current={'1': 0, '2': 1},
            client=_CannedClient([]),
        )
        with pytest.raises(ProviderResponseError, match='never reconciled to empty'):
            coordinator.apply_listing(_roster_definition(), set())
        assert store.applied == []  # the prior roster is untouched
        assert ledger.completed == []  # staleness cannot advance

    def test_apply_listing_rejects_an_all_null_column_listing(self) -> None:
        # Trigger shape 2: records existed but every member value was null,
        # so the extractor filtered the listing down to nothing.
        collector_frame = pl.DataFrame({'vehicle_id': [None, None]})
        listed = extract_roster_members(collector_frame, 'vehicle_id')
        assert listed == set()
        coordinator, store, ledger = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=None,
            current={'1': 0},
            client=_CannedClient([]),
        )
        with pytest.raises(ProviderResponseError, match='never reconciled to empty'):
            coordinator.apply_listing(_roster_definition(), listed)
        assert store.applied == []
        assert ledger.completed == []

    def test_harvest_with_an_empty_listing_degrades_like_a_failed_refresh(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # The harvest routes its reconcile through apply_listing, so an empty
        # feeder listing takes exactly the failed-HTTP-refresh path: the run
        # is marked failed, the prior roster stays, staleness never advances.
        coordinator, store, ledger = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=_NOW - timedelta(days=2),
            current={'1': 0, '2': 1},
            client=_CannedClient([_page([])]),
        )
        with caplog.at_level(logging.WARNING):
            coordinator.refresh_if_stale(_roster_definition())
        assert store.applied == []
        assert ledger.completed == []
        assert len(ledger.failed) == 1
        assert any(
            'keeping 2 existing members' in r.getMessage() for r in caplog.records
        )

    def test_harvest_with_an_empty_listing_reraises_on_cold_start(self) -> None:
        # No prior roster to keep means nothing to degrade to: re-raise.
        coordinator, store, ledger = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=None,
            current={},
            client=_CannedClient([_page([])]),
        )
        with pytest.raises(ProviderResponseError, match='never reconciled to empty'):
            coordinator.refresh_if_stale(_roster_definition())
        assert store.applied == []
        assert ledger.completed == []
        assert len(ledger.failed) == 1
