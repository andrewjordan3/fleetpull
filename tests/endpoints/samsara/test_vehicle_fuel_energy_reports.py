"""Tests for fleetpull.endpoints.samsara.vehicle_fuel_energy_reports.

The binding is the first fixed-unit-width watermark endpoint
(probe-settled 2026-07-21): a fleet-wide ``SingleFetch`` whose shared
family builder renders the resume window as RFC3339
``startDate``/``endDate`` (this surface family's OWN param names --
unlike every sibling's startTime/endTime), paired with the
``SamsaraWindowReportPageDecoder`` at ``results_limit=100`` (the
server's OWN observed page size; the ``limit`` param is proven ignored
-- 512/513/10 all paged identically) and ``report_key='vehicleReports'``
(the nested envelope). The rollup grain is the request window and day
rollups are NON-ADDITIVE (89/267 mismatched), so the ``WatermarkMode``
declares ``fixed_unit_days=1`` and the decoder stamps every report
with the sent window, routed on ``event_time_column='window_start'``.
"""

from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.samsara._spec_builders import (
    SamsaraFuelEnergyReportSpecBuilder,
)
from fleetpull.endpoints.samsara.vehicle_fuel_energy_reports import build_endpoint
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SingleFetch,
    SpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.models.samsara import VehicleFuelEnergyReport
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import SamsaraWindowReportPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope
from tests.samsara_vehicle_fuel_energy_reports_capture import (
    VEHICLE_FUEL_ENERGY_PAGE_RESPONSE,
    VEHICLE_FUEL_ENERGY_TERMINAL_RESPONSE,
)


def _build_endpoint() -> EndpointDefinition[VehicleFuelEnergyReport]:
    return build_endpoint(SamsaraConfig())


def _window() -> DateWindow:
    # One day exactly -- the unit width every planned window tiles into
    # under the declared fixed_unit_days=1.
    return DateWindow(
        start=datetime(2026, 1, 2, tzinfo=UTC),
        end=datetime(2026, 1, 3, tzinfo=UTC),
    )


class TestVehicleFuelEnergyReportsSpecBuilder:
    def test_satisfies_spec_builder_protocol(self) -> None:
        builder: SpecBuilder = _build_endpoint().spec_builder
        assert isinstance(builder, SamsaraFuelEnergyReportSpecBuilder)

    def test_builds_the_get_with_the_start_end_date_param_names(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(), member_values={}
        )
        assert spec.method is HttpMethod.GET
        assert spec.url == (
            'https://api.samsara.com/fleet/reports/vehicles/fuel-energy'
        )
        # The surface family's OWN window param NAMES: startDate/endDate,
        # unlike every other probed Samsara vertical's startTime/endTime
        # -- full RFC3339 datetimes accepted despite the names (captured
        # 2026-07-21). Pagination parameters are the decoder's, injected
        # by its first_request, so they do not appear here.
        assert spec.params == {
            'startDate': '2026-01-02T00:00:00Z',
            'endDate': '2026-01-03T00:00:00Z',
        }

    def test_configured_base_url_is_used(self) -> None:
        config = SamsaraConfig(base_url='https://alt.example.test/')
        spec = build_endpoint(config).spec_builder.build_spec(
            resume=_window(), member_values={}
        )
        # The config strips the trailing slash so the path joins cleanly.
        assert spec.url == (
            'https://alt.example.test/fleet/reports/vehicles/fuel-energy'
        )

    def test_requires_a_date_window(self) -> None:
        with pytest.raises(TypeError):
            _build_endpoint().spec_builder.build_spec(resume=None, member_values={})

    def test_no_credentials_or_body(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(), member_values={}
        )
        assert spec.headers == {}
        assert spec.json_body is None


class TestBuildVehicleFuelEnergyReportsEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.SAMSARA
        assert endpoint.name == 'vehicle_fuel_energy_reports'
        assert endpoint.response_model is VehicleFuelEnergyReport
        assert endpoint.quota_scope is QuotaScope.SAMSARA
        assert endpoint.storage_kind is StorageKind.DATE_PARTITIONED
        assert endpoint.event_time_column == 'window_start'
        assert isinstance(endpoint.sync_mode, WatermarkMode)
        assert endpoint.completeness_check is None

    def test_declares_the_fixed_one_day_unit_width(self) -> None:
        # The window-grain declaration: the rollup grain is the request
        # window (metrics GREW when the window widened) and day rollups
        # are NON-ADDITIVE into wider windows (89/267 mismatched,
        # captured 2026-07-21), so the unit width is part of the row's
        # meaning -- pinned to exactly one day, never floating with
        # sync.backfill_chunk_days.
        endpoint = _build_endpoint()
        assert isinstance(endpoint.sync_mode, WatermarkMode)
        assert endpoint.sync_mode.fixed_unit_days == 1

    def test_watermark_knobs_flow_from_config(self) -> None:
        default_endpoint = _build_endpoint()
        assert isinstance(default_endpoint.sync_mode, WatermarkMode)
        assert default_endpoint.sync_mode.lookback == timedelta(days=7)
        assert default_endpoint.sync_mode.cutoff == timedelta(days=0)
        custom = build_endpoint(SamsaraConfig(lookback_days=2, cutoff_days=1))
        assert isinstance(custom.sync_mode, WatermarkMode)
        assert custom.sync_mode.lookback == timedelta(days=2)
        assert custom.sync_mode.cutoff == timedelta(days=1)

    def test_uses_the_report_decoder_at_the_servers_own_page_size(self) -> None:
        # results_limit=100 is documentation-by-declaration: the server
        # pages at its own ~100-report size and the limit param is
        # proven ignored (512/513/10 on the same 2-day window all
        # returned identical 3-page paging; 513 NOT rejected -- no
        # enforced tier), so the declaration states the server's own
        # observed page size, never a working knob.
        decoder = _build_endpoint().page_decoder
        assert isinstance(decoder, SamsaraWindowReportPageDecoder)
        assert decoder.records_key == 'data'
        assert decoder.report_key == 'vehicleReports'
        assert decoder.results_limit == 100

    def test_declares_the_default_single_fetch_shape(self) -> None:
        # Fleet-wide with per-record vehicle attribution: one chain, no
        # fan-out -- the SingleFetch default, declared by declaring
        # nothing.
        assert isinstance(_build_endpoint().request_shape, SingleFetch)

    def test_a_two_page_walk_stamps_every_report_with_the_sent_window(self) -> None:
        # The whole chain through the REAL decoder against the committed
        # captures: the builder emits the startDate/endDate window, the
        # decoder injects limit on page one and merges `after` onto the
        # SENT spec (so the window persists), and every emitted record
        # on both pages lands stamped with the window verbatim.
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
            first, VEHICLE_FUEL_ENERGY_PAGE_RESPONSE
        )
        next_spec = continued.advance.next_spec
        assert next_spec is not None
        assert next_spec.params == {
            'startDate': '2026-01-02T00:00:00Z',
            'endDate': '2026-01-03T00:00:00Z',
            'limit': '100',
            'after': 'c3ludGgtZnVlbC1lbmVyZ3ktY3Vyc29yLTAwMQ',
        }
        terminal = endpoint.page_decoder.decode_page(
            next_spec, VEHICLE_FUEL_ENERGY_TERMINAL_RESPONSE
        )
        assert terminal.advance.next_spec is None
        walked = continued.records + terminal.records
        assert len(walked) == 4
        for record in walked:
            assert record['windowStartDate'] == '2026-01-02T00:00:00Z'
            assert record['windowEndDate'] == '2026-01-03T00:00:00Z'

    def test_the_walked_records_validate_against_the_model(self) -> None:
        # The stamped decoder output IS the model's input grain: every
        # record of the two-page walk validates, window bounds aware.
        endpoint = _build_endpoint()
        first = endpoint.page_decoder.first_request(
            endpoint.spec_builder.build_spec(resume=_window(), member_values={})
        )
        continued = endpoint.page_decoder.decode_page(
            first, VEHICLE_FUEL_ENERGY_PAGE_RESPONSE
        )
        assert continued.advance.next_spec is not None
        terminal = endpoint.page_decoder.decode_page(
            continued.advance.next_spec, VEHICLE_FUEL_ENERGY_TERMINAL_RESPONSE
        )
        for record in continued.records + terminal.records:
            report = VehicleFuelEnergyReport.model_validate(record)
            assert report.window_start == datetime(2026, 1, 2, tzinfo=UTC)
            assert report.window_end == datetime(2026, 1, 3, tzinfo=UTC)

    def test_constructs_without_raising(self) -> None:
        # The WatermarkMode + DATE_PARTITIONED + event_time_column=
        # 'window_start' triple passes EndpointDefinition's construction
        # validation against the VehicleFuelEnergyReport model.
        endpoint = build_endpoint(SamsaraConfig())
        assert endpoint.name == 'vehicle_fuel_energy_reports'
