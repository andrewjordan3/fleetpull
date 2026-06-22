"""Tests for fleetpull.endpoints.motive.vehicle_locations."""

from datetime import UTC, datetime

import pytest

from fleetpull.endpoints.motive.vehicle_locations import (
    MotiveVehicleLocationsSpecBuilder,
)
from fleetpull.endpoints.shared import SpecBuilder, UrlPathTemplateError
from fleetpull.incremental import DateWindow
from fleetpull.network.contract import HttpMethod


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
