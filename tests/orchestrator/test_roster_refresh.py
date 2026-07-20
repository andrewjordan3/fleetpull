"""Tests for fleetpull.orchestrator.roster_refresh."""

import logging
import threading
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


def _snapshot_feeder(
    name: str = 'vehicles', path: str = '/v'
) -> EndpointDefinition[ResponseModel]:
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name=name,
        spec_builder=StaticGetSpecBuilder(base_url='https://api.test', path=path),
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
    *,
    key: RosterKey = _KEY,
    source_endpoint: str = 'vehicles',
    eviction_threshold: int | None = 3,
) -> RosterDefinition:
    return RosterDefinition(
        key=key,
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

    def test_a_due_refresh_narrates_start_and_completion(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        coordinator, _, _ = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=_NOW - timedelta(days=2),
            current={'1': 0},
            client=_CannedClient([_page([{'vehicle_id': '1'}, {'vehicle_id': '2'}])]),
        )
        with caplog.at_level('INFO', logger='fleetpull.orchestrator.roster_refresh'):
            coordinator.refresh_if_stale(_roster_definition())
        info_messages = [
            record.getMessage()
            for record in caplog.records
            if record.levelname == 'INFO'
        ]
        assert any(
            'roster refresh started:' in message and 'members_held=1' in message
            for message in info_messages
        )
        assert any(
            'roster refreshed:' in message and 'members=2' in message
            for message in info_messages
        )

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


class _ParkingCountingClient(TransportClient):
    """Counts harvests and parks each inside ``fetch_pages`` until released.

    The single-flight probe (no ``super().__init__``): the first harvest
    parks holding the roster's lock; a second entrant under an unlocked
    coordinator would enter and park too, driving the count past one.
    """

    def __init__(self, pages: list[FetchedPage]) -> None:
        self._pages = pages
        self._count_lock = threading.Lock()
        self.harvest_count = 0
        self.first_entered = threading.Event()
        self.second_entered = threading.Event()
        self.release = threading.Event()

    def fetch_pages(
        self, spec: RequestSpec, page_decoder: PageDecoder, quota_scope: str
    ) -> Iterator[FetchedPage]:
        with self._count_lock:
            self.harvest_count += 1
            entered = self.harvest_count
        if entered == 1:
            self.first_entered.set()
        else:
            self.second_entered.set()
        assert self.release.wait(timeout=10), 'harvest was never released'
        yield from self._pages


class _CrossKeyHandshakeClient(TransportClient):
    """Each feeder's harvest waits for the other's to start (no ``super().__init__``).

    Passes only when the two rosters' refreshes overlap: a global rather
    than per-key lock would park one behind the other, the handshake would
    time out, and the failed harvest would fail the test loudly.
    """

    def __init__(self) -> None:
        self.a_started = threading.Event()
        self.b_started = threading.Event()

    def fetch_pages(
        self, spec: RequestSpec, page_decoder: PageDecoder, quota_scope: str
    ) -> Iterator[FetchedPage]:
        if spec.url.endswith('/feeder-a'):
            self.a_started.set()
            assert self.b_started.wait(timeout=10), (
                "roster B's harvest never started while A held its per-key lock"
            )
            yield _page([{'vehicle_id': 'a1'}])
        else:
            self.b_started.set()
            assert self.a_started.wait(timeout=10), (
                "roster A's harvest never started while B held its per-key lock"
            )
            yield _page([{'vehicle_id': 'b1'}])


class _PerEndpointLedger:
    """A ``FeederRunLedger`` double keying last-success by endpoint (the real shape)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_success: dict[str, datetime] = {}
        self.started: list[tuple[Provider, str]] = []

    def last_success_at(self, provider: Provider, endpoint: str) -> datetime | None:
        with self._lock:
            return self._last_success.get(endpoint)

    def start_snapshot_run(self, provider: Provider, endpoint: str) -> int:
        with self._lock:
            self.started.append((provider, endpoint))
            return len(self.started)

    def complete_run(self, run_id: int, *, row_count: int) -> None:
        with self._lock:
            _, endpoint = self.started[run_id - 1]
            self._last_success[endpoint] = _NOW

    def fail_run(self, run_id: int, *, error_detail: str) -> None:
        pass


class TestSingleFlight:
    """The per-key single-flight lock on ``refresh_if_stale`` (DESIGN section 7)."""

    def test_concurrent_same_key_consumers_harvest_exactly_once(self) -> None:
        client = _ParkingCountingClient([_page([{'vehicle_id': '1'}])])
        coordinator, store, ledger = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=_NOW - timedelta(days=2),
            current={'1': 0},
            client=client,
        )
        definition = _roster_definition()
        refresh_failures: list[Exception] = []

        def _refresh() -> None:
            try:
                coordinator.refresh_if_stale(definition)
            except Exception as failure:
                refresh_failures.append(failure)

        first = threading.Thread(target=_refresh)
        second = threading.Thread(target=_refresh)
        first.start()
        assert client.first_entered.wait(timeout=10), 'first harvest never started'
        second.start()
        # Under single-flight the second entrant parks on the roster's
        # lock, so this bounded wait expires untouched; under an unlocked
        # coordinator the second harvest enters within it and the count
        # assertion below fails the test.
        client.second_entered.wait(timeout=0.5)
        client.release.set()
        first.join(timeout=10)
        second.join(timeout=10)
        assert not first.is_alive(), 'first refresh never finished'
        assert not second.is_alive(), 'second refresh never finished'
        assert refresh_failures == []
        # Exactly one harvest, one run row, one applied delta: the second
        # entrant re-ran the freshness check under the lock and returned
        # early onto the just-refreshed roster.
        assert client.harvest_count == 1
        assert ledger.started == [(Provider.MOTIVE, 'vehicles')]
        assert len(store.applied) == 1

    def test_distinct_roster_keys_refresh_concurrently(self) -> None:
        # The locks are per-key, never global: each harvest waits inside
        # fetch_pages for the other to have started, so completion at all
        # proves the two keys' refreshes overlapped.
        client = _CrossKeyHandshakeClient()
        store = _FakeStore({'m1': 0})
        ledger = _PerEndpointLedger()
        coordinator = RosterRefreshCoordinator(
            endpoint_registry=EndpointRegistry(
                [
                    _snapshot_feeder(path='/feeder-a'),
                    _snapshot_feeder(name='drivers', path='/feeder-b'),
                ]
            ),
            store=store,
            ledger=ledger,
            client_source=_FakeClientSource(client),
            clock=FrozenClock(start_time_utc=_NOW),
        )
        driver_key = RosterKey(Provider.MOTIVE, 'driver_ids')
        definition_a = _roster_definition()
        definition_b = _roster_definition(key=driver_key, source_endpoint='drivers')
        refresh_failures: list[Exception] = []

        def _refresh(definition: RosterDefinition) -> None:
            try:
                coordinator.refresh_if_stale(definition)
            except Exception as failure:
                refresh_failures.append(failure)

        thread_a = threading.Thread(target=_refresh, args=(definition_a,))
        thread_b = threading.Thread(target=_refresh, args=(definition_b,))
        thread_a.start()
        thread_b.start()
        thread_a.join(timeout=15)
        thread_b.join(timeout=15)
        assert not thread_a.is_alive(), 'roster A refresh never finished'
        assert not thread_b.is_alive(), 'roster B refresh never finished'
        assert refresh_failures == []
        assert {key for key, _ in store.applied} == {_KEY, driver_key}
        assert set(ledger.started) == {
            (Provider.MOTIVE, 'vehicles'),
            (Provider.MOTIVE, 'drivers'),
        }

    def test_the_feeder_tap_serializes_behind_a_same_key_harvest(self) -> None:
        # ``apply_listing`` takes the same per-key lock the harvest holds,
        # so a feeder tap can never interleave its read-reconcile-write
        # with a concurrent harvest on the same roster -- the lost-update
        # on absence counts that would otherwise wrongly evict or
        # resurrect members.
        client = _ParkingCountingClient([_page([{'vehicle_id': '1'}])])
        coordinator, store, _ledger = _coordinator(
            feeder=_snapshot_feeder(),
            last_success=_NOW - timedelta(days=2),
            current={'1': 0},
            client=client,
        )
        definition = _roster_definition()
        tap_returned = threading.Event()
        failures: list[Exception] = []

        def _harvest() -> None:
            try:
                coordinator.refresh_if_stale(definition)
            except Exception as failure:
                failures.append(failure)

        def _tap() -> None:
            try:
                coordinator.apply_listing(definition, {'1', '2'})
            except Exception as failure:
                failures.append(failure)
            else:
                tap_returned.set()

        harvester = threading.Thread(target=_harvest)
        harvester.start()
        assert client.first_entered.wait(timeout=10), 'harvest never started'
        tap = threading.Thread(target=_tap)
        tap.start()
        # A regression-detection window, never passing-path
        # synchronization: under the shared lock the tap parks behind the
        # parked harvest, so this wait expires untouched; an unlocked tap
        # reconciles within it and fails the test loudly.
        assert not tap_returned.wait(timeout=0.5), (
            'the feeder tap reconciled while a same-key harvest held the lock'
        )
        client.release.set()
        harvester.join(timeout=10)
        tap.join(timeout=10)
        assert not harvester.is_alive(), 'harvest never finished'
        assert not tap.is_alive(), 'tap never finished'
        assert failures == []
        assert tap_returned.is_set()
        # Both writes landed, harvest first: the tap waited out the lock
        # rather than interleaving.
        assert len(store.applied) == 2


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
