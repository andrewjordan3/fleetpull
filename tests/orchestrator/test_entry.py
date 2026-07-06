# tests/orchestrator/test_entry.py
"""Tests for fleetpull.orchestrator.entry."""

from datetime import datetime, timedelta

import polars as pl
import pytest

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    FanOutBinding,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.exceptions import ConfigurationError, ProviderResponseError
from fleetpull.model_contract import ResponseModel
from fleetpull.network.contract import (
    DecodedPage,
    JsonValue,
    PageAdvance,
    RequestSpec,
)
from fleetpull.orchestrator.drivers import (
    FanOutRequestDriver,
    RequestDriver,
    SingleRequestDriver,
)
from fleetpull.orchestrator.entry import run_endpoint
from fleetpull.orchestrator.outcome import CaughtUp, Executed, RunOutcome
from fleetpull.orchestrator.runner import BatchObserver
from fleetpull.roster import RosterDefinition, RosterKey, RosterRegistry
from fleetpull.storage import WriteResult
from fleetpull.vocabulary import Provider, QuotaScope

VEHICLE_IDS_KEY = RosterKey(Provider.MOTIVE, 'vehicle_ids')

VEHICLE_IDS_DEFINITION = RosterDefinition(
    key=VEHICLE_IDS_KEY,
    source_endpoint='vehicles',
    source_column='vehicle_id',
    max_age=timedelta(days=1),
    eviction_threshold=3,
)


class _SnapshotModel(ResponseModel):
    id: int


class _WatermarkModel(ResponseModel):
    occurred_at: datetime


class _StubPageDecoder:
    """A PageDecoder double; the entry never drives it."""

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        return spec

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        return DecodedPage(
            records=[], advance=PageAdvance(next_spec=None, durable_progress=None)
        )


def _snapshot_definition() -> EndpointDefinition[_SnapshotModel]:
    """A no-fan-out definition (the vehicles shape)."""
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='vehicles',
        spec_builder=StaticGetSpecBuilder(base_url='https://x.test', path='/v1/v'),
        page_decoder=_StubPageDecoder(),
        response_model=_SnapshotModel,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
    )


def _fan_out_definition() -> EndpointDefinition[_WatermarkModel]:
    """A fan-out watermark definition (the vehicle_locations shape)."""
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='locations',
        spec_builder=StaticGetSpecBuilder(base_url='https://x.test', path='/v3/l'),
        page_decoder=_StubPageDecoder(),
        response_model=_WatermarkModel,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=1)),
        event_time_column='occurred_at',
        fan_out=FanOutBinding(roster=VEHICLE_IDS_KEY, path_placeholder='vehicle_id'),
    )


def _executed() -> Executed:
    return Executed(
        records_fetched=1,
        write=WriteResult(rows_written=1, duplicates_dropped=0, files_written=1),
    )


class _RecordingRunner:
    """An EndpointExecutor recording each (definition, driver, observer) run.

    Simulates the real runner's observer contract: each canned frame is
    handed to the observer (post-validation shape) before the outcome
    returns. With no frames it is a plain pass-through.
    """

    def __init__(
        self,
        outcome: RunOutcome | None = None,
        frames: list[pl.DataFrame] | None = None,
        failure: Exception | None = None,
    ) -> None:
        self.runs: list[tuple[EndpointDefinition[ResponseModel], RequestDriver]] = []
        self.observers: list[BatchObserver | None] = []
        self._outcome: RunOutcome = outcome if outcome is not None else CaughtUp()
        self._frames = frames or []
        self._failure = failure

    def run(
        self,
        definition: EndpointDefinition[ResponseModel],
        driver: RequestDriver,
        observer: BatchObserver | None = None,
    ) -> RunOutcome:
        self.runs.append((definition, driver))
        self.observers.append(observer)
        if observer is not None:
            for frame in self._frames:
                observer(frame)
        if self._failure is not None:
            raise self._failure
        return self._outcome


class _RecordingRefresher:
    """A RosterRefresher recording calls; optionally raising (cold start)."""

    def __init__(self, failure: Exception | None = None) -> None:
        self.refreshed: list[RosterDefinition] = []
        self.applied: list[tuple[RosterDefinition, set[str]]] = []
        self._failure = failure

    def refresh_if_stale(self, definition: RosterDefinition) -> None:
        self.refreshed.append(definition)
        if self._failure is not None:
            raise self._failure

    def apply_listing(self, definition: RosterDefinition, listed: set[str]) -> None:
        self.applied.append((definition, listed))


class _CannedMembers:
    """A RosterMembersReader returning a canned membership, recording reads."""

    def __init__(self, members: list[str]) -> None:
        self._members = members
        self.reads: list[RosterKey] = []

    def read_members(self, key: RosterKey) -> list[str]:
        self.reads.append(key)
        return self._members


def test_no_fan_out_gets_the_single_fetch_driver_and_never_touches_rosters() -> None:
    runner = _RecordingRunner()
    refresher = _RecordingRefresher()
    members = _CannedMembers(['should-not-be-read'])
    outcome = run_endpoint(
        _snapshot_definition(), runner, RosterRegistry([]), refresher, members
    )
    assert isinstance(outcome, CaughtUp)
    [(_, driver)] = runner.runs
    assert isinstance(driver, SingleRequestDriver)
    assert runner.observers == [None]
    assert refresher.refreshed == []
    assert refresher.applied == []
    assert members.reads == []


def test_fan_out_resolves_refreshes_reads_and_fans_out() -> None:
    runner = _RecordingRunner()
    refresher = _RecordingRefresher()
    members = _CannedMembers(['101', '202'])
    registry = RosterRegistry([VEHICLE_IDS_DEFINITION])
    run_endpoint(_fan_out_definition(), runner, registry, refresher, members)
    assert refresher.refreshed == [VEHICLE_IDS_DEFINITION]
    assert members.reads == [VEHICLE_IDS_KEY]
    [(_, driver)] = runner.runs
    assert isinstance(driver, FanOutRequestDriver)
    assert driver.members == ['101', '202']
    assert driver.path_placeholder == 'vehicle_id'


def test_cold_start_refresh_failure_propagates_unswallowed() -> None:
    runner = _RecordingRunner()
    cold_start_failure = ProviderResponseError(
        provider='motive', endpoint='vehicles', detail='feeder unreachable'
    )
    refresher = _RecordingRefresher(failure=cold_start_failure)
    registry = RosterRegistry([VEHICLE_IDS_DEFINITION])
    with pytest.raises(ProviderResponseError, match='feeder unreachable'):
        run_endpoint(
            _fan_out_definition(), runner, registry, refresher, _CannedMembers([])
        )
    assert runner.runs == []


def test_failed_refresh_degrades_to_the_existing_members() -> None:
    # The coordinator's best-effort contract observed through the entry: a
    # stale roster whose re-list failed keeps its stored members (the
    # refresher swallowed the failure), and the fan-out proceeds over them.
    runner = _RecordingRunner()
    refresher = _RecordingRefresher()  # a no-op models the swallowed failure
    members = _CannedMembers(['existing-1', 'existing-2'])
    registry = RosterRegistry([VEHICLE_IDS_DEFINITION])
    run_endpoint(_fan_out_definition(), runner, registry, refresher, members)
    [(_, driver)] = runner.runs
    assert isinstance(driver, FanOutRequestDriver)
    assert driver.members == ['existing-1', 'existing-2']


def test_empty_roster_after_refresh_raises() -> None:
    runner = _RecordingRunner()
    registry = RosterRegistry([VEHICLE_IDS_DEFINITION])
    with pytest.raises(ConfigurationError, match='empty'):
        run_endpoint(
            _fan_out_definition(),
            runner,
            registry,
            _RecordingRefresher(),
            _CannedMembers([]),
        )
    assert runner.runs == []


def test_unregistered_roster_raises() -> None:
    with pytest.raises(ConfigurationError, match='unknown roster'):
        run_endpoint(
            _fan_out_definition(),
            _RecordingRunner(),
            RosterRegistry([]),
            _RecordingRefresher(),
            _CannedMembers(['1']),
        )


def test_feeder_run_reconciles_its_sourced_rosters() -> None:
    # The fix for the fresh-ledger-stale-roster mode: a runner-driven feeder
    # run hands its collected listing to the coordinator, so a user-initiated
    # vehicles run can never advance the ledger without reconciling the
    # roster. The frames carry the post-validation column name (the model
    # field 'vehicle_id', not the wire alias 'id') -- what the observer sees.
    frames = [
        pl.DataFrame({'vehicle_id': ['101', '202']}),
        pl.DataFrame({'vehicle_id': ['202', '303']}),
    ]
    runner = _RecordingRunner(outcome=_executed(), frames=frames)
    refresher = _RecordingRefresher()
    registry = RosterRegistry([VEHICLE_IDS_DEFINITION])
    outcome = run_endpoint(
        _snapshot_definition(), runner, registry, refresher, _CannedMembers([])
    )
    assert isinstance(outcome, Executed)
    assert refresher.applied == [(VEHICLE_IDS_DEFINITION, {'101', '202', '303'})]
    # The tap is not the fan-out path: no refresh, no member read.
    assert refresher.refreshed == []


def test_failed_feeder_run_applies_nothing() -> None:
    runner = _RecordingRunner(
        frames=[pl.DataFrame({'vehicle_id': ['101']})],
        failure=RuntimeError('run blew up'),
    )
    refresher = _RecordingRefresher()
    registry = RosterRegistry([VEHICLE_IDS_DEFINITION])
    with pytest.raises(RuntimeError, match='run blew up'):
        run_endpoint(
            _snapshot_definition(), runner, registry, refresher, _CannedMembers([])
        )
    assert refresher.applied == []


def test_caught_up_feeder_run_applies_nothing() -> None:
    # CaughtUp means nothing executed and nothing was listed; reconciling an
    # empty non-listing would count absences against every stored member.
    runner = _RecordingRunner(outcome=CaughtUp())
    refresher = _RecordingRefresher()
    registry = RosterRegistry([VEHICLE_IDS_DEFINITION])
    run_endpoint(
        _snapshot_definition(), runner, registry, refresher, _CannedMembers([])
    )
    assert refresher.applied == []


def test_endpoint_that_sources_nothing_and_fans_out_nothing_is_untouched() -> None:
    # The baseline the agnosticism principle protects: an endpoint that is
    # nobody's source and declares no fan-out flows through the entry with no
    # observer installed and no roster machinery touched.
    runner = _RecordingRunner()
    refresher = _RecordingRefresher()
    members = _CannedMembers([])
    registry = RosterRegistry([VEHICLE_IDS_DEFINITION])
    other = EndpointDefinition(
        provider=Provider.MOTIVE,
        name='other',
        spec_builder=StaticGetSpecBuilder(base_url='https://x.test', path='/v1/o'),
        page_decoder=_StubPageDecoder(),
        response_model=_SnapshotModel,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
    )
    run_endpoint(other, runner, registry, refresher, members)
    assert runner.observers == [None]
    assert refresher.refreshed == []
    assert refresher.applied == []
    assert members.reads == []


def test_identical_entry_serves_snapshot_and_fan_out_polymorphically() -> None:
    # The agnosticism principle's regression test: a roster-sourcing snapshot
    # and a fan-out watermark definition flow through the identical entry
    # with identical collaborators; every difference in observed behavior
    # traces to declared facts (fan_out None vs the binding; sourced vs not
    # in the roster catalog), never to provider or endpoint identity.
    runner = _RecordingRunner()
    refresher = _RecordingRefresher()
    members = _CannedMembers(['101'])
    registry = RosterRegistry([VEHICLE_IDS_DEFINITION])
    snapshot = _snapshot_definition()
    fan_out = _fan_out_definition()

    run_endpoint(snapshot, runner, registry, refresher, members)
    run_endpoint(fan_out, runner, registry, refresher, members)

    (first_definition, first_driver), (second_definition, second_driver) = runner.runs
    assert first_definition is snapshot
    assert isinstance(first_driver, SingleRequestDriver)
    assert second_definition is fan_out
    assert isinstance(second_driver, FanOutRequestDriver)
    # The snapshot sources the vehicle_ids roster (catalog fact) -> observed;
    # the fan-out consumer sources nothing -> not observed.
    assert runner.observers[0] is not None
    assert runner.observers[1] is None
    # The refresh/read pair fires exactly once -- for the one definition that
    # declares a binding -- and with that binding's declared key.
    assert refresher.refreshed == [VEHICLE_IDS_DEFINITION]
    assert members.reads == [VEHICLE_IDS_KEY]
    declared_binding = fan_out.fan_out
    assert declared_binding is not None
    assert second_driver.path_placeholder == declared_binding.path_placeholder
