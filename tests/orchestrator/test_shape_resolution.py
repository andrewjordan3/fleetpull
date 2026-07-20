# tests/orchestrator/test_shape_resolution.py
"""Tests for fleetpull.orchestrator.shape_resolution.

The seam owns exactly the shape-to-driver dispatch: each ``RequestShape``
member resolves to its driver, the fanned shapes draw the provider's pool,
and a ``RosterFanOut`` with no roster source (the stateless-caller case)
fails loudly instead of fetching a partial fleet.
"""

from datetime import datetime, timedelta

import pytest

from fleetpull.endpoints.shared import (
    BisectedWindowFetch,
    EndpointDefinition,
    ParamSweep,
    RequestShape,
    RosterFanOut,
    SingleFetch,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
    SyncMode,
    WatermarkMode,
)
from fleetpull.exceptions import ConfigurationError
from fleetpull.model_contract import ResponseModel
from fleetpull.orchestrator.bisection import BisectingWindowDriver
from fleetpull.orchestrator.drivers import FanOutRequestDriver, SingleRequestDriver
from fleetpull.orchestrator.fanout import FetchPool
from fleetpull.orchestrator.shape_resolution import resolve_request_driver
from fleetpull.roster import RosterKey
from fleetpull.vocabulary import Provider, QuotaScope
from tests.orchestrator.doubles import StubPageDecoder
from tests.orchestrator.serial_executor import SerialExecutor

VEHICLE_IDS_KEY = RosterKey(Provider.MOTIVE, 'vehicle_ids')

# A shared frozen marker for the helper's default (B008: no call in defaults).
_SNAPSHOT = SnapshotMode()


class _SnapshotModel(ResponseModel):
    id: int


class _WatermarkModel(ResponseModel):
    occurred_at: datetime


class _StubPoolSource:
    """A FetchPoolSource handing one synchronous pool, recording lookups."""

    def __init__(self) -> None:
        self.pool = FetchPool(executor=SerialExecutor(), submission_window=2)
        self.requested: list[Provider] = []

    def pool_for(self, provider: Provider) -> FetchPool:
        self.requested.append(provider)
        return self.pool


def _definition(
    shape: RequestShape,
    *,
    sync_mode: SyncMode = _SNAPSHOT,
    storage_kind: StorageKind = StorageKind.SINGLE,
    event_time_column: str | None = None,
) -> EndpointDefinition[ResponseModel]:
    model: type[ResponseModel] = (
        _WatermarkModel if event_time_column else _SnapshotModel
    )
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='shaped',
        spec_builder=StaticGetSpecBuilder(base_url='https://x.test', path='/v1/s'),
        page_decoder=StubPageDecoder(),
        response_model=model,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=storage_kind,
        sync_mode=sync_mode,
        event_time_column=event_time_column,
        request_shape=shape,
    )


def _windowed_definition(shape: RequestShape) -> EndpointDefinition[ResponseModel]:
    return _definition(
        shape,
        sync_mode=WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(0)),
        storage_kind=StorageKind.DATE_PARTITIONED,
        event_time_column='occurred_at',
    )


def test_single_fetch_resolves_the_single_request_driver() -> None:
    pools = _StubPoolSource()
    driver = resolve_request_driver(
        _definition(SingleFetch()), fetch_pools=pools, roster_members=None
    )
    assert isinstance(driver, SingleRequestDriver)
    assert pools.requested == []


def test_bisected_window_fetch_resolves_the_bisecting_driver() -> None:
    shape = BisectedWindowFetch(
        results_limit=100, floor=timedelta(minutes=1), event_time_wire_key='at'
    )
    pools = _StubPoolSource()
    driver = resolve_request_driver(
        _windowed_definition(shape), fetch_pools=pools, roster_members=None
    )
    assert isinstance(driver, BisectingWindowDriver)
    assert driver.shape is shape
    assert pools.requested == []


def test_param_sweep_resolves_a_fan_out_over_the_declared_values() -> None:
    # The sweep rides the member-agnostic fan-out driver: the declared
    # values are the members and the param is the member key -- no sweep
    # driver class exists to drift from the fan-out's semantics.
    sweep = ParamSweep(param='status', values=('active', 'deactivated'))
    pools = _StubPoolSource()
    driver = resolve_request_driver(
        _definition(sweep), fetch_pools=pools, roster_members=None
    )
    assert isinstance(driver, FanOutRequestDriver)
    assert driver.members == ('active', 'deactivated')
    assert driver.member_key == 'status'
    assert driver.fetch_pool is pools.pool
    assert pools.requested == [Provider.MOTIVE]


def test_roster_fan_out_resolves_over_the_supplied_members() -> None:
    shape = RosterFanOut(roster=VEHICLE_IDS_KEY, member_key='vehicle_id')
    pools = _StubPoolSource()
    seen_shapes: list[RosterFanOut] = []

    def members_for(requested: RosterFanOut) -> list[str]:
        seen_shapes.append(requested)
        return ['101', '202']

    driver = resolve_request_driver(
        _windowed_definition(shape), fetch_pools=pools, roster_members=members_for
    )
    assert isinstance(driver, FanOutRequestDriver)
    assert driver.members == ['101', '202']
    assert driver.member_key == 'vehicle_id'
    assert driver.fetch_pool is pools.pool
    assert seen_shapes == [shape]


def test_roster_fan_out_without_a_roster_source_raises() -> None:
    # The stateless-caller case (fetch): a roster fan-out needs durable
    # roster state, so resolution refuses instead of fetching nothing.
    shape = RosterFanOut(roster=VEHICLE_IDS_KEY, member_key='vehicle_id')
    with pytest.raises(ConfigurationError, match='no roster source') as raised:
        resolve_request_driver(
            _windowed_definition(shape),
            fetch_pools=_StubPoolSource(),
            roster_members=None,
        )
    message = str(raised.value)
    assert 'shaped' in message
    assert 'vehicle_ids' in message
