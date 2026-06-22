"""Tests for fleetpull.endpoints.shared.base."""

import dataclasses
from collections.abc import Mapping
from datetime import datetime, timedelta

import pytest

from fleetpull.endpoints.shared import (
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
    occurred_at: datetime
    maybe_at: datetime | None = None


def _make_endpoint(
    sync_mode: SyncMode,
    *,
    storage_kind: StorageKind = StorageKind.SINGLE,
    event_time_column: str | None = None,
) -> EndpointDefinition[_StubModel]:
    """Build an EndpointDefinition from the stubs and the given axes."""
    return EndpointDefinition(
        provider=Provider.SAMSARA,
        name='trips',
        spec_builder=_StubSpecBuilder(),
        page_decoder=_StubPageDecoder(),
        response_model=_StubModel,
        quota_scope=QuotaScope.SAMSARA,
        storage_kind=storage_kind,
        sync_mode=sync_mode,
        event_time_column=event_time_column,
    )


class TestEndpointDefinition:
    def test_constructs_and_reads_back_fields(self) -> None:
        endpoint = _make_endpoint(
            WatermarkMode(lookback=timedelta(days=1)),
            event_time_column='occurred_at',
        )
        assert endpoint.provider == Provider.SAMSARA
        assert endpoint.name == 'trips'
        assert endpoint.response_model is _StubModel
        assert endpoint.quota_scope == QuotaScope.SAMSARA
        assert endpoint.storage_kind == StorageKind.SINGLE
        assert endpoint.sync_mode == WatermarkMode(lookback=timedelta(days=1))
        assert endpoint.event_time_column == 'occurred_at'

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


class TestEndpointDefinitionValidation:
    def test_snapshot_with_date_partitioned_raises(self) -> None:
        with pytest.raises(ValueError, match='SINGLE'):
            _make_endpoint(SnapshotMode(), storage_kind=StorageKind.DATE_PARTITIONED)

    def test_snapshot_with_event_time_column_raises(self) -> None:
        with pytest.raises(ValueError, match='event_time_column must be None'):
            _make_endpoint(SnapshotMode(), event_time_column='occurred_at')

    def test_watermark_without_event_time_column_raises(self) -> None:
        with pytest.raises(ValueError, match='requires an event_time_column'):
            _make_endpoint(WatermarkMode(lookback=timedelta(days=1)))

    def test_date_partitioned_without_event_time_column_raises(self) -> None:
        with pytest.raises(ValueError, match='requires an event_time_column'):
            _make_endpoint(FeedMode(), storage_kind=StorageKind.DATE_PARTITIONED)

    def test_event_time_column_not_a_field_raises(self) -> None:
        with pytest.raises(ValueError, match='not a field'):
            _make_endpoint(
                WatermarkMode(lookback=timedelta(days=1)),
                event_time_column='not_a_field',
            )

    def test_non_date_like_event_time_column_raises(self) -> None:
        with pytest.raises(TypeError, match='date-like'):
            _make_endpoint(
                WatermarkMode(lookback=timedelta(days=1)),
                event_time_column='name',
            )

    def test_watermark_single_with_event_time_constructs(self) -> None:
        endpoint = _make_endpoint(
            WatermarkMode(lookback=timedelta(days=1)),
            event_time_column='occurred_at',
        )
        assert endpoint.event_time_column == 'occurred_at'

    def test_watermark_date_partitioned_constructs(self) -> None:
        endpoint = _make_endpoint(
            WatermarkMode(lookback=timedelta(days=1)),
            storage_kind=StorageKind.DATE_PARTITIONED,
            event_time_column='occurred_at',
        )
        assert endpoint.storage_kind == StorageKind.DATE_PARTITIONED

    def test_nullable_date_like_event_time_constructs(self) -> None:
        endpoint = _make_endpoint(
            WatermarkMode(lookback=timedelta(days=1)),
            event_time_column='maybe_at',
        )
        assert endpoint.event_time_column == 'maybe_at'

    def test_snapshot_single_without_event_time_constructs(self) -> None:
        endpoint = _make_endpoint(SnapshotMode())
        assert endpoint.event_time_column is None

    def test_feed_single_without_event_time_constructs(self) -> None:
        endpoint = _make_endpoint(FeedMode())
        assert endpoint.event_time_column is None
