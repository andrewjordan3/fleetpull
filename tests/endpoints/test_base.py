"""Tests for fleetpull.endpoints.base."""

import dataclasses
from collections.abc import Mapping
from datetime import timedelta

import pytest

from fleetpull.endpoints.base import (
    EndpointDefinition,
    FeedMode,
    ResumeValue,
    SnapshotMode,
    StorageKind,
    SyncMode,
    WatermarkMode,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.network.contract import (
    DecodedPage,
    HttpMethod,
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


class _StubPageDecoder:
    """A PageDecoder double that returns one empty page."""

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        return spec

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        return DecodedPage(
            records=[], advance=PageAdvance(next_spec=None, durable_progress=None)
        )


class _StubModel(ResponseModel):
    name: str


def _make_endpoint(sync_mode: SyncMode) -> EndpointDefinition[_StubModel]:
    """Build an EndpointDefinition from the stubs and a given sync mode."""
    return EndpointDefinition(
        provider=Provider.SAMSARA,
        name='trips',
        spec_builder=_StubSpecBuilder(),
        page_decoder=_StubPageDecoder(),
        response_model=_StubModel,
        quota_scope=QuotaScope.SAMSARA,
        storage_kind=StorageKind.SINGLE,
        sync_mode=sync_mode,
    )


class TestEndpointDefinition:
    def test_constructs_and_reads_back_fields(self) -> None:
        endpoint = _make_endpoint(WatermarkMode(lookback=timedelta(days=1)))
        assert endpoint.provider == Provider.SAMSARA
        assert endpoint.name == 'trips'
        assert endpoint.response_model is _StubModel
        assert endpoint.quota_scope == QuotaScope.SAMSARA
        assert endpoint.storage_kind == StorageKind.SINGLE
        assert endpoint.sync_mode == WatermarkMode(lookback=timedelta(days=1))

    def test_accepts_a_feed_mode(self) -> None:
        endpoint = _make_endpoint(FeedMode())
        assert endpoint.sync_mode == FeedMode()

    def test_accepts_a_snapshot_mode(self) -> None:
        endpoint = _make_endpoint(SnapshotMode())
        assert endpoint.sync_mode == SnapshotMode()

    def test_is_frozen(self) -> None:
        endpoint = _make_endpoint(FeedMode())
        with pytest.raises(dataclasses.FrozenInstanceError):
            endpoint.name = 'other'  # type: ignore[misc]

    def test_has_slots_no_dict(self) -> None:
        assert not hasattr(_make_endpoint(FeedMode()), '__dict__')


class TestSyncMode:
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

    def test_snapshot_mode_is_slotted_and_equal(self) -> None:
        mode = SnapshotMode()
        assert not hasattr(mode, '__dict__')
        assert mode == SnapshotMode()


class TestStorageKind:
    def test_is_str_enum(self) -> None:
        assert issubclass(StorageKind, str)

    def test_member_values(self) -> None:
        assert StorageKind.SINGLE.value == 'single'
        assert StorageKind.DATE_PARTITIONED.value == 'date_partitioned'
