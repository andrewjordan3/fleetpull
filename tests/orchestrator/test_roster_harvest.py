"""Tests for fleetpull.orchestrator.roster_harvest."""

from collections.abc import Iterator

import pytest

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    ResumeValue,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.network.contract import DecodedPage, PageAdvance, RequestSpec
from fleetpull.orchestrator.roster_harvest import harvest_roster_members
from fleetpull.vocabulary import JsonObject, JsonValue, Provider, QuotaScope


class _Vehicle(ResponseModel):
    vehicle_id: int


class _StubPageDecoder:
    """A PageDecoder double; the canned driver bypasses it, so it is never called."""

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        return spec

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        return DecodedPage(
            records=[], advance=PageAdvance(next_spec=None, durable_progress=None)
        )


class _StubClient(TransportClient):
    """A hollow client; the canned driver never calls it (no ``super().__init__``)."""

    def __init__(self) -> None:
        pass


class _CannedDriver:
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


def _definition() -> EndpointDefinition[ResponseModel]:
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


class TestHarvestRosterMembers:
    def test_unions_distinct_members_across_batches(self) -> None:
        driver = _CannedDriver(
            [
                [{'vehicle_id': 1}, {'vehicle_id': 2}],
                [{'vehicle_id': 2}, {'vehicle_id': 3}],
            ]
        )
        members = harvest_roster_members(
            _definition(), driver, _StubClient(), 'vehicle_id'
        )
        assert members == {'1', '2', '3'}

    def test_empty_feeder_yields_empty_set(self) -> None:
        driver = _CannedDriver([[]])
        members = harvest_roster_members(
            _definition(), driver, _StubClient(), 'vehicle_id'
        )
        assert members == set()

    def test_propagates_missing_column_error(self) -> None:
        driver = _CannedDriver([[{'vehicle_id': 1}]])
        with pytest.raises(ValueError, match='not in the frame'):
            harvest_roster_members(_definition(), driver, _StubClient(), 'absent')
