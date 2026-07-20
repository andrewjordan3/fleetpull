"""Tests for fleetpull.endpoints.motive.driving_periods."""

from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.config import MotiveConfig
from fleetpull.endpoints.motive._spec_builders import MotiveFleetDateRangeSpecBuilder
from fleetpull.endpoints.motive.driving_periods import build_endpoint
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SingleFetch,
    SpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.models.motive import DrivingPeriod
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import MotiveWrappedListPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope


def _window() -> DateWindow:
    return DateWindow(
        start=datetime(2026, 6, 1, tzinfo=UTC),
        end=datetime(2026, 6, 4, tzinfo=UTC),
    )


class TestMotiveFleetDateRangeSpecBuilder:
    def test_satisfies_spec_builder_protocol(self) -> None:
        builder: SpecBuilder = MotiveFleetDateRangeSpecBuilder(
            base_url='https://api.example.test', path='/v1/driving_periods'
        )
        assert isinstance(builder, MotiveFleetDateRangeSpecBuilder)

    def test_maps_window_to_inclusive_date_params_unpadded(self) -> None:
        # end_date is the window's last covered date -- the day before
        # the exclusive midnight end (the vehicle_locations mapping).
        spec = MotiveFleetDateRangeSpecBuilder(
            base_url='https://api.example.test', path='/v1/driving_periods'
        ).build_spec(resume=_window(), member_values={})
        assert spec.method is HttpMethod.GET
        assert spec.url == 'https://api.example.test/v1/driving_periods'
        assert spec.params == {
            'start_date': '2026-06-01',
            'end_date': '2026-06-03',
        }

    def test_requires_a_date_window(self) -> None:
        builder = MotiveFleetDateRangeSpecBuilder(
            base_url='https://api.example.test', path='/v1/driving_periods'
        )
        with pytest.raises(TypeError):
            builder.build_spec(resume=None, member_values={})

    def test_no_credentials_or_body(self) -> None:
        spec = MotiveFleetDateRangeSpecBuilder(
            base_url='https://api.example.test', path='/v1/driving_periods'
        ).build_spec(resume=_window(), member_values={})
        assert spec.headers == {}
        assert spec.json_body is None


def _build_endpoint() -> EndpointDefinition[DrivingPeriod]:
    return build_endpoint(MotiveConfig(base_url='https://api.example.test'))


class TestBuildDrivingPeriodsEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.MOTIVE
        assert endpoint.name == 'driving_periods'
        assert endpoint.quota_scope is QuotaScope.MOTIVE
        assert endpoint.storage_kind is StorageKind.DATE_PARTITIONED
        assert endpoint.response_model is DrivingPeriod
        assert endpoint.event_time_column == 'start_time'
        assert endpoint.request_shape == SingleFetch()
        assert endpoint.completeness_check is None

    def test_watermark_knobs_come_from_config(self) -> None:
        endpoint = build_endpoint(
            MotiveConfig(
                base_url='https://api.example.test',
                lookback_days=3,
                cutoff_days=1,
            )
        )
        assert isinstance(endpoint.sync_mode, WatermarkMode)
        assert endpoint.sync_mode.lookback == timedelta(days=3)
        assert endpoint.sync_mode.cutoff == timedelta(days=1)

    def test_decoder_speaks_the_wrapped_list_wire_shape(self) -> None:
        endpoint = _build_endpoint()
        decoder = endpoint.page_decoder
        assert isinstance(decoder, MotiveWrappedListPageDecoder)
        assert decoder.list_key == 'driving_periods'
        assert decoder.item_key == 'driving_period'
        assert decoder.per_page == 100

    def test_spec_builder_carries_no_pad(self) -> None:
        # Window matching is start-anchored on UTC days for this
        # endpoint (DESIGN section 8) -- the wire window is the resume
        # window exactly.
        endpoint = _build_endpoint()
        builder = endpoint.spec_builder
        assert isinstance(builder, MotiveFleetDateRangeSpecBuilder)
        assert builder.window_pad_days == 0
