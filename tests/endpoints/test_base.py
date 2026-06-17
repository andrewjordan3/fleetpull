# tests/endpoints/test_base.py
"""Tests for fleetpull.endpoints.base."""

import dataclasses
from collections.abc import Mapping
from datetime import timedelta

import pytest

from fleetpull.endpoints.base import (
    EndpointDefinition,
    FeedMode,
    IncrementalMode,
    ResumeValue,
    StorageKind,
    TopLevelListExtractor,
    WatermarkMode,
)
from fleetpull.exceptions import ProviderResponseError
from fleetpull.model_contract import ResponseModel
from fleetpull.network.contract import (
    HttpMethod,
    JsonObject,
    JsonValue,
    PageAdvance,
    RequestSpec,
)
from fleetpull.vocabulary import Provider, QuotaScope


class _StubSpecBuilder:
    """A SpecBuilder double returning a fixed first request."""

    def build_spec(
        self, resume: ResumeValue, path_values: Mapping[str, str]
    ) -> RequestSpec:
        return RequestSpec(method=HttpMethod.GET, url='https://example.test/v1/items')


class _StubPagination:
    """A PaginationStrategy double that completes after the first page."""

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        return spec

    def advance(self, sent: RequestSpec, envelope: JsonValue) -> PageAdvance:
        return PageAdvance(next_spec=None, durable_progress=None)


class _StubExtractor:
    """A RecordExtractor double returning no records."""

    def extract(self, envelope: JsonValue) -> list[JsonObject]:
        return []


class _StubModel(ResponseModel):
    name: str


def _make_endpoint(incremental: IncrementalMode) -> EndpointDefinition[_StubModel]:
    """Build an EndpointDefinition from the stubs and a given incremental mode."""
    return EndpointDefinition(
        provider=Provider.SAMSARA,
        name='trips',
        spec_builder=_StubSpecBuilder(),
        pagination=_StubPagination(),
        response_model=_StubModel,
        record_extractor=_StubExtractor(),
        quota_scope=QuotaScope.SAMSARA,
        storage_kind=StorageKind.SINGLE,
        incremental=incremental,
    )


class TestTopLevelListExtractor:
    def test_returns_the_record_list(self) -> None:
        extractor = TopLevelListExtractor(key='data')
        envelope: JsonValue = {'data': [{'id': 1}, {'id': 2}]}
        expected: list[JsonObject] = [{'id': 1}, {'id': 2}]
        assert extractor.extract(envelope) == expected

    def test_rejects_a_non_object_envelope(self) -> None:
        extractor = TopLevelListExtractor(key='data')
        envelope: JsonValue = ['not', 'an', 'object']
        with pytest.raises(ProviderResponseError, match='JSON object envelope'):
            extractor.extract(envelope)

    def test_rejects_a_missing_key(self) -> None:
        extractor = TopLevelListExtractor(key='data')
        envelope: JsonValue = {'other': []}
        with pytest.raises(ProviderResponseError, match='missing the record key'):
            extractor.extract(envelope)

    def test_rejects_a_non_list_value(self) -> None:
        extractor = TopLevelListExtractor(key='data')
        envelope: JsonValue = {'data': {'not': 'a list'}}
        with pytest.raises(ProviderResponseError, match='is not a list'):
            extractor.extract(envelope)

    def test_rejects_a_non_object_element(self) -> None:
        extractor = TopLevelListExtractor(key='data')
        envelope: JsonValue = {'data': [{'id': 1}, 'x']}
        with pytest.raises(ProviderResponseError, match='is not a JSON object'):
            extractor.extract(envelope)


class TestEndpointDefinition:
    def test_constructs_and_reads_back_fields(self) -> None:
        endpoint = _make_endpoint(WatermarkMode(lookback=timedelta(days=1)))
        assert endpoint.provider == Provider.SAMSARA
        assert endpoint.name == 'trips'
        assert endpoint.response_model is _StubModel
        assert endpoint.quota_scope == QuotaScope.SAMSARA
        assert endpoint.storage_kind == StorageKind.SINGLE
        assert endpoint.incremental == WatermarkMode(lookback=timedelta(days=1))

    def test_accepts_a_feed_mode(self) -> None:
        endpoint = _make_endpoint(FeedMode())
        assert endpoint.incremental == FeedMode()

    def test_is_frozen(self) -> None:
        endpoint = _make_endpoint(FeedMode())
        with pytest.raises(dataclasses.FrozenInstanceError):
            endpoint.name = 'other'  # type: ignore[misc]

    def test_has_slots_no_dict(self) -> None:
        assert not hasattr(_make_endpoint(FeedMode()), '__dict__')


class TestIncrementalMode:
    def test_watermark_mode_holds_lookback(self) -> None:
        assert WatermarkMode(lookback=timedelta(hours=6)).lookback == timedelta(hours=6)

    def test_watermark_mode_is_frozen_and_slotted(self) -> None:
        mode = WatermarkMode(lookback=timedelta(hours=6))
        assert not hasattr(mode, '__dict__')
        with pytest.raises(dataclasses.FrozenInstanceError):
            mode.lookback = timedelta(0)  # type: ignore[misc]

    def test_feed_mode_is_slotted_and_equal(self) -> None:
        mode = FeedMode()
        assert not hasattr(mode, '__dict__')
        assert mode == FeedMode()


class TestStorageKind:
    def test_is_str_enum(self) -> None:
        assert issubclass(StorageKind, str)

    def test_member_values(self) -> None:
        assert StorageKind.SINGLE.value == 'single'
        assert StorageKind.DATE_PARTITIONED.value == 'date_partitioned'
