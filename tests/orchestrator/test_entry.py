# tests/orchestrator/test_entry.py
"""Tests for fleetpull.orchestrator.entry."""

from datetime import datetime, timedelta

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
from fleetpull.orchestrator.outcome import CaughtUp, RunOutcome
from fleetpull.roster import RosterDefinition, RosterKey, RosterRegistry
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


class _RecordingRunner:
    """An EndpointExecutor recording each (definition, driver) it runs."""

    def __init__(self) -> None:
        self.runs: list[tuple[EndpointDefinition[ResponseModel], RequestDriver]] = []

    def run(
        self,
        definition: EndpointDefinition[ResponseModel],
        driver: RequestDriver,
    ) -> RunOutcome:
        self.runs.append((definition, driver))
        return CaughtUp()


class _RecordingRefresher:
    """A RosterRefresher recording calls; optionally raising (cold start)."""

    def __init__(self, failure: Exception | None = None) -> None:
        self.refreshed: list[RosterDefinition] = []
        self._failure = failure

    def refresh_if_stale(self, definition: RosterDefinition) -> None:
        self.refreshed.append(definition)
        if self._failure is not None:
            raise self._failure


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
    assert refresher.refreshed == []
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


def test_identical_entry_serves_snapshot_and_fan_out_polymorphically() -> None:
    # The agnosticism principle's regression test: a snapshot (no fan-out)
    # and a fan-out watermark definition flow through the identical entry
    # with identical collaborators; every difference in observed behavior
    # traces to declared fields (fan_out None vs the binding), never to
    # provider or endpoint identity.
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
    # The roster machinery was touched exactly once -- for the one definition
    # that declares a binding -- and with that binding's declared key.
    assert refresher.refreshed == [VEHICLE_IDS_DEFINITION]
    assert members.reads == [VEHICLE_IDS_KEY]
    declared_binding = fan_out.fan_out
    assert declared_binding is not None
    assert second_driver.path_placeholder == declared_binding.path_placeholder
