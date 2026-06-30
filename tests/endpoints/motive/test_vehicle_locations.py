"""Tests for fleetpull.endpoints.motive.vehicle_locations."""

from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.config import MotiveConfig
from fleetpull.endpoints.motive.vehicle_locations import (
    MotiveVehicleLocationsSpecBuilder,
    build_endpoint,
)
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SpecBuilder,
    StorageKind,
    UrlPathTemplateError,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.models.motive import VehicleLocation
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import MotiveWrappedSinglePageDecoder
from fleetpull.vocabulary import Provider, QuotaScope


def _build_builder() -> MotiveVehicleLocationsSpecBuilder:
    return MotiveVehicleLocationsSpecBuilder(
        base_url='https://api.example.test',
        path_template='/v3/vehicle_locations/{vehicle_id}',
    )


def _window() -> DateWindow:
    return DateWindow(
        start=datetime(2026, 6, 1, tzinfo=UTC),
        end=datetime(2026, 6, 4, tzinfo=UTC),
    )


class TestMotiveVehicleLocationsSpecBuilder:
    def test_satisfies_spec_builder_protocol(self) -> None:
        builder: SpecBuilder = _build_builder()
        assert isinstance(builder, MotiveVehicleLocationsSpecBuilder)

    def test_renders_the_per_vehicle_url(self) -> None:
        spec = _build_builder().build_spec(
            resume=_window(), path_values={'vehicle_id': '543180'}
        )
        assert spec.method is HttpMethod.GET
        assert spec.url == 'https://api.example.test/v3/vehicle_locations/543180'

    def test_maps_window_to_inclusive_date_params(self) -> None:
        # end_date is the window's last covered date -- the day before the
        # exclusive midnight end, not the date of end itself.
        spec = _build_builder().build_spec(
            resume=_window(), path_values={'vehicle_id': '543180'}
        )
        assert spec.params == {
            'start_date': '2026-06-01',
            'end_date': '2026-06-03',
        }

    def test_requires_a_date_window(self) -> None:
        with pytest.raises(TypeError):
            _build_builder().build_spec(resume=None, path_values={'vehicle_id': '1'})

    def test_strict_path_values_propagate(self) -> None:
        with pytest.raises(UrlPathTemplateError):
            _build_builder().build_spec(resume=_window(), path_values={})

    def test_no_credentials_or_body(self) -> None:
        spec = _build_builder().build_spec(
            resume=_window(), path_values={'vehicle_id': '543180'}
        )
        assert spec.headers == {}
        assert spec.json_body is None


def _build_endpoint() -> EndpointDefinition[VehicleLocation]:
    return build_endpoint(MotiveConfig(base_url='https://api.example.test'))


class TestBuildVehicleLocationsEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.MOTIVE
        assert endpoint.name == 'vehicle_locations'
        assert endpoint.response_model is VehicleLocation
        assert endpoint.quota_scope is QuotaScope.MOTIVE
        assert endpoint.storage_kind is StorageKind.DATE_PARTITIONED
        assert endpoint.event_time_column == 'located_at'
        assert isinstance(endpoint.sync_mode, WatermarkMode)

    def test_lookback_flows_from_config(self) -> None:
        default_endpoint = _build_endpoint()
        assert isinstance(default_endpoint.sync_mode, WatermarkMode)
        assert default_endpoint.sync_mode.lookback == timedelta(days=7)
        custom = build_endpoint(MotiveConfig(lookback_days=2))
        assert isinstance(custom.sync_mode, WatermarkMode)
        assert custom.sync_mode.lookback == timedelta(days=2)

    def test_uses_the_wrapped_single_page_decoder(self) -> None:
        decoder = _build_endpoint().page_decoder
        assert isinstance(decoder, MotiveWrappedSinglePageDecoder)
        assert decoder.list_key == 'vehicle_locations'
        assert decoder.item_key == 'vehicle_location'

    def test_spec_builder_fans_out_per_vehicle(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(), path_values={'vehicle_id': '543180'}
        )
        assert spec.url == 'https://api.example.test/v3/vehicle_locations/543180'

    def test_base_url_default_flows_through(self) -> None:
        endpoint = build_endpoint(MotiveConfig())
        spec = endpoint.spec_builder.build_spec(
            resume=_window(), path_values={'vehicle_id': '1'}
        )
        assert spec.url.startswith('https://api.gomotive.com')

    def test_constructs_without_raising(self) -> None:
        # The WatermarkMode + DATE_PARTITIONED + event_time_column='located_at'
        # triple passes EndpointDefinition's construction validation.
        endpoint = build_endpoint(MotiveConfig())
        assert endpoint.name == 'vehicle_locations'
