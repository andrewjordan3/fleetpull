"""Tests for fleetpull.endpoints.samsara.driver_fuel_energy_reports.

The driver arm of the fixed-unit-width fuel-energy report pair
(probe-settled 2026-07-21): the vehicle arm's binding with the path and
report key swapped -- the shared family builder rendering the resume
window as RFC3339 ``startDate``/``endDate`` (the family's OWN param
names), the ``SamsaraWindowReportPageDecoder`` at ``results_limit=100``
(documentation of the server's own paging; the ``limit`` param is
proven ignored on this family) with ``report_key='driverReports'``,
``fixed_unit_days=1`` per the pair's non-additivity proof, and
``event_time_column='window_start'`` on the decoder-stamped window.
"""

from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.samsara._spec_builders import (
    SamsaraFuelEnergyReportSpecBuilder,
)
from fleetpull.endpoints.samsara.driver_fuel_energy_reports import build_endpoint
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SingleFetch,
    SpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.models.samsara import DriverFuelEnergyReport
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import SamsaraWindowReportPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope
from tests.samsara_driver_fuel_energy_reports_capture import (
    DRIVER_FUEL_ENERGY_PAGE_RESPONSE,
    DRIVER_FUEL_ENERGY_TERMINAL_RESPONSE,
)


def _build_endpoint() -> EndpointDefinition[DriverFuelEnergyReport]:
    return build_endpoint(SamsaraConfig())


def _window() -> DateWindow:
    # One day exactly -- the unit width every planned window tiles into
    # under the declared fixed_unit_days=1.
    return DateWindow(
        start=datetime(2026, 1, 2, tzinfo=UTC),
        end=datetime(2026, 1, 3, tzinfo=UTC),
    )


class TestDriverFuelEnergyReportsSpecBuilder:
    def test_satisfies_spec_builder_protocol(self) -> None:
        builder: SpecBuilder = _build_endpoint().spec_builder
        assert isinstance(builder, SamsaraFuelEnergyReportSpecBuilder)

    def test_builds_the_get_with_the_start_end_date_param_names(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(), member_values={}
        )
        assert spec.method is HttpMethod.GET
        assert spec.url == ('https://api.samsara.com/fleet/reports/drivers/fuel-energy')
        # The surface family's OWN window param NAMES (startDate/endDate
        # accepting RFC3339 datetimes -- captured 2026-07-21); the
        # decoder injects pagination.
        assert spec.params == {
            'startDate': '2026-01-02T00:00:00Z',
            'endDate': '2026-01-03T00:00:00Z',
        }

    def test_requires_a_date_window(self) -> None:
        with pytest.raises(TypeError):
            _build_endpoint().spec_builder.build_spec(resume=None, member_values={})

    def test_no_credentials_or_body(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(), member_values={}
        )
        assert spec.headers == {}
        assert spec.json_body is None


class TestBuildDriverFuelEnergyReportsEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.SAMSARA
        assert endpoint.name == 'driver_fuel_energy_reports'
        assert endpoint.response_model is DriverFuelEnergyReport
        assert endpoint.quota_scope is QuotaScope.SAMSARA
        assert endpoint.storage_kind is StorageKind.DATE_PARTITIONED
        assert endpoint.event_time_column == 'window_start'
        assert isinstance(endpoint.sync_mode, WatermarkMode)
        assert endpoint.completeness_check is None

    def test_declares_the_fixed_one_day_unit_width(self) -> None:
        # The pair's window-grain declaration (the non-additivity proof
        # rides the vehicle arm's docstring): the unit width is part of
        # the row's meaning, pinned to one day.
        endpoint = _build_endpoint()
        assert isinstance(endpoint.sync_mode, WatermarkMode)
        assert endpoint.sync_mode.fixed_unit_days == 1

    def test_watermark_knobs_flow_from_config(self) -> None:
        custom = build_endpoint(SamsaraConfig(lookback_days=2, cutoff_days=1))
        assert isinstance(custom.sync_mode, WatermarkMode)
        assert custom.sync_mode.lookback == timedelta(days=2)
        assert custom.sync_mode.cutoff == timedelta(days=1)

    def test_uses_the_report_decoder_at_the_servers_own_page_size(self) -> None:
        # results_limit=100 documents the family's server-owned paging
        # (the limit param is proven ignored); the report key is this
        # arm's own.
        decoder = _build_endpoint().page_decoder
        assert isinstance(decoder, SamsaraWindowReportPageDecoder)
        assert decoder.records_key == 'data'
        assert decoder.report_key == 'driverReports'
        assert decoder.results_limit == 100

    def test_declares_the_default_single_fetch_shape(self) -> None:
        # Fleet-wide with per-record driver attribution: one chain, no
        # fan-out -- the SingleFetch default, declared by declaring
        # nothing.
        assert isinstance(_build_endpoint().request_shape, SingleFetch)

    def test_a_two_page_walk_stamps_every_report_with_the_sent_window(self) -> None:
        # The whole chain through the REAL decoder against the committed
        # captures: limit on page one, `after` merged onto the SENT spec
        # (the window persisting), every record on both pages stamped
        # with the sent window verbatim.
        endpoint = _build_endpoint()
        first = endpoint.page_decoder.first_request(
            endpoint.spec_builder.build_spec(resume=_window(), member_values={})
        )
        assert first.params == {
            'startDate': '2026-01-02T00:00:00Z',
            'endDate': '2026-01-03T00:00:00Z',
            'limit': '100',
        }
        continued = endpoint.page_decoder.decode_page(
            first, DRIVER_FUEL_ENERGY_PAGE_RESPONSE
        )
        next_spec = continued.advance.next_spec
        assert next_spec is not None
        assert next_spec.params == {
            'startDate': '2026-01-02T00:00:00Z',
            'endDate': '2026-01-03T00:00:00Z',
            'limit': '100',
            'after': 'c3ludGgtZHJpdmVyLWZ1ZWwtY3Vyc29yLTAwMQ',
        }
        terminal = endpoint.page_decoder.decode_page(
            next_spec, DRIVER_FUEL_ENERGY_TERMINAL_RESPONSE
        )
        assert terminal.advance.next_spec is None
        walked = continued.records + terminal.records
        assert len(walked) == 3
        for record in walked:
            assert record['windowStartDate'] == '2026-01-02T00:00:00Z'
            assert record['windowEndDate'] == '2026-01-03T00:00:00Z'

    def test_the_walked_records_validate_against_the_model(self) -> None:
        # The stamped decoder output IS the model's input grain.
        endpoint = _build_endpoint()
        first = endpoint.page_decoder.first_request(
            endpoint.spec_builder.build_spec(resume=_window(), member_values={})
        )
        continued = endpoint.page_decoder.decode_page(
            first, DRIVER_FUEL_ENERGY_PAGE_RESPONSE
        )
        assert continued.advance.next_spec is not None
        terminal = endpoint.page_decoder.decode_page(
            continued.advance.next_spec, DRIVER_FUEL_ENERGY_TERMINAL_RESPONSE
        )
        for record in continued.records + terminal.records:
            report = DriverFuelEnergyReport.model_validate(record)
            assert report.window_start == datetime(2026, 1, 2, tzinfo=UTC)
            assert report.window_end == datetime(2026, 1, 3, tzinfo=UTC)

    def test_constructs_without_raising(self) -> None:
        # The WatermarkMode + DATE_PARTITIONED + event_time_column=
        # 'window_start' triple passes EndpointDefinition's construction
        # validation against the DriverFuelEnergyReport model.
        endpoint = build_endpoint(SamsaraConfig())
        assert endpoint.name == 'driver_fuel_energy_reports'
