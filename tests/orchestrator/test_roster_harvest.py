"""Tests for fleetpull.orchestrator.roster_harvest."""

import pytest

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.orchestrator.roster_harvest import harvest_roster_members
from fleetpull.vocabulary import Provider, QuotaScope
from tests.orchestrator.doubles import CannedDriver, StubClient, StubPageDecoder


class _Vehicle(ResponseModel):
    vehicle_id: int


def _definition() -> EndpointDefinition[ResponseModel]:
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


class TestHarvestRosterMembers:
    def test_unions_distinct_members_across_batches(self) -> None:
        driver = CannedDriver(
            [
                [{'vehicle_id': 1}, {'vehicle_id': 2}],
                [{'vehicle_id': 2}, {'vehicle_id': 3}],
            ]
        )
        members = harvest_roster_members(
            _definition(), driver, StubClient(), 'vehicle_id'
        )
        assert members == {'1', '2', '3'}

    def test_empty_feeder_yields_empty_set(self) -> None:
        driver = CannedDriver([[]])
        members = harvest_roster_members(
            _definition(), driver, StubClient(), 'vehicle_id'
        )
        assert members == set()

    def test_propagates_missing_column_error(self) -> None:
        driver = CannedDriver([[{'vehicle_id': 1}]])
        with pytest.raises(ValueError, match='not in the frame'):
            harvest_roster_members(_definition(), driver, StubClient(), 'absent')
