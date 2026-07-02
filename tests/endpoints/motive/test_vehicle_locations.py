"""Tests for fleetpull.endpoints.motive.vehicle_locations."""

from datetime import UTC, date, datetime, timedelta

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
from fleetpull.incremental import (
    DateWindow,
    resolve_resume_start,
    resolve_trailing_edge,
    window_or_none,
)
from fleetpull.models.motive import VehicleLocation
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import MotiveWrappedSinglePageDecoder
from fleetpull.storage.partitioning import window_dates
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

    def test_request_dates_equal_the_resolved_windows_covered_dates(self) -> None:
        # The seam where request, filter, and partition coverage must agree:
        # a window resolved through the real chain (a late-day watermark arm,
        # floored at resolution) must request exactly the dates
        # window_dates(window) covers -- what the fetch returns is what the
        # filter keeps and what the writer replaces and prunes.
        unfloored_arm = datetime(2026, 6, 29, 23, 59, 59, tzinfo=UTC)
        start = resolve_resume_start(
            unfloored_arm, None, datetime(2026, 1, 1, tzinfo=UTC)
        )
        end = resolve_trailing_edge(
            datetime(2026, 7, 2, 9, 0, tzinfo=UTC), timedelta(0)
        )
        window = window_or_none(start, end)
        assert window is not None
        spec = _build_builder().build_spec(
            resume=window, path_values={'vehicle_id': '543180'}
        )
        assert spec.params is not None
        start_date = date.fromisoformat(spec.params['start_date'])
        end_date = date.fromisoformat(spec.params['end_date'])
        requested_dates = [
            start_date + timedelta(days=offset)
            for offset in range((end_date - start_date).days + 1)
        ]
        assert requested_dates == window_dates(window)


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
