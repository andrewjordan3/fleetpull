"""Tests for fleetpull.endpoints.motive.vehicle_utilizations.

The binding is Motive's arm of the fixed-unit-width watermark family
(probe-settled 2026-07-21): a fleet-wide ``SingleFetch`` whose shared
date-range builder renders the resume window as the INCLUSIVE
``start_date``/``end_date`` label pair (both labels the unit's day at
the fixed 1-day width; company-local interpretation documented, never
converted), paired with the ``MotiveWindowReportPageDecoder`` at the
configured page size and the ``vehicle_utilizations``/
``vehicle_utilization`` wrapper keys. The rollup grain is the request
window, so the ``WatermarkMode`` declares ``fixed_unit_days=1`` and the
decoder stamps every row with the sent window, routed on
``event_time_column='window_start'``.
"""

from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.config import MotiveConfig
from fleetpull.endpoints.motive._spec_builders import MotiveFleetDateRangeSpecBuilder
from fleetpull.endpoints.motive.vehicle_utilizations import build_endpoint
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SingleFetch,
    SpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.models.motive import VehicleUtilization
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import MotiveWindowReportPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope
from tests.motive_vehicle_utilizations_capture import (
    VEHICLE_UTILIZATIONS_PAGE_1_RESPONSE,
    VEHICLE_UTILIZATIONS_PAGE_2_RESPONSE,
)


def _build_endpoint() -> EndpointDefinition[VehicleUtilization]:
    return build_endpoint(MotiveConfig(base_url='https://api.example.test'))


def _window() -> DateWindow:
    # One day exactly -- the unit width every planned window tiles into
    # under the declared fixed_unit_days=1.
    return DateWindow(
        start=datetime(2026, 1, 5, tzinfo=UTC),
        end=datetime(2026, 1, 6, tzinfo=UTC),
    )


class TestVehicleUtilizationsSpecBuilder:
    def test_satisfies_spec_builder_protocol(self) -> None:
        builder: SpecBuilder = _build_endpoint().spec_builder
        assert isinstance(builder, MotiveFleetDateRangeSpecBuilder)

    def test_maps_the_one_day_unit_to_the_inclusive_label_pair(self) -> None:
        # THE date-label mapping: the half-open [2026-01-05, 2026-01-06)
        # unit renders as start_date=2026-01-05&end_date=2026-01-05 --
        # the pair is INCLUSIVE on both ends (start_date=end_date
        # returned exactly one day's rollup live), so both labels are
        # the unit's day. The labels are interpreted in COMPANY-LOCAL
        # days (DESIGN section 8); no pad, no conversion.
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(), member_values={}
        )
        assert spec.method is HttpMethod.GET
        assert spec.url == 'https://api.example.test/v2/vehicle_utilization'
        assert spec.params == {
            'start_date': '2026-01-05',
            'end_date': '2026-01-05',
        }

    def test_spec_builder_carries_no_pad(self) -> None:
        # The window IS the label pair on this surface: rows carry no
        # event time to trim against, so there is nothing a pad could
        # buy -- the rollup rides the labels verbatim.
        builder = _build_endpoint().spec_builder
        assert isinstance(builder, MotiveFleetDateRangeSpecBuilder)
        assert builder.window_pad_days == 0

    def test_requires_a_date_window(self) -> None:
        with pytest.raises(TypeError):
            _build_endpoint().spec_builder.build_spec(resume=None, member_values={})

    def test_no_credentials_or_body(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(), member_values={}
        )
        assert spec.headers == {}
        assert spec.json_body is None


class TestBuildVehicleUtilizationsEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.MOTIVE
        assert endpoint.name == 'vehicle_utilizations'
        assert endpoint.response_model is VehicleUtilization
        assert endpoint.quota_scope is QuotaScope.MOTIVE
        assert endpoint.storage_kind is StorageKind.DATE_PARTITIONED
        assert endpoint.event_time_column == 'window_start'
        assert isinstance(endpoint.sync_mode, WatermarkMode)
        assert endpoint.completeness_check is None

    def test_declares_the_fixed_one_day_unit_width(self) -> None:
        # The window-grain declaration: the rollup grain is the request
        # window (a 1-day and a 6-day request each returned one rollup
        # row per vehicle, captured 2026-07-21), so the unit width is
        # part of the row's meaning -- pinned to exactly one day, never
        # floating with sync.backfill_chunk_days (the fuel-energy
        # machinery's second consumer).
        endpoint = _build_endpoint()
        assert isinstance(endpoint.sync_mode, WatermarkMode)
        assert endpoint.sync_mode.fixed_unit_days == 1

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

    def test_decoder_speaks_the_wrapped_report_wire_shape(self) -> None:
        endpoint = _build_endpoint()
        decoder = endpoint.page_decoder
        assert isinstance(decoder, MotiveWindowReportPageDecoder)
        assert decoder.list_key == 'vehicle_utilizations'
        assert decoder.item_key == 'vehicle_utilization'
        assert decoder.per_page == 100

    def test_page_size_comes_from_config(self) -> None:
        endpoint = build_endpoint(
            MotiveConfig(base_url='https://api.example.test', records_per_page=50)
        )
        decoder = endpoint.page_decoder
        assert isinstance(decoder, MotiveWindowReportPageDecoder)
        assert decoder.per_page == 50

    def test_declares_the_default_single_fetch_shape(self) -> None:
        # Fleet-wide with per-record vehicle attribution: one chain, no
        # fan-out -- the SingleFetch default, declared by declaring
        # nothing.
        assert isinstance(_build_endpoint().request_shape, SingleFetch)

    def test_a_two_page_walk_stamps_every_record_with_the_sent_window(self) -> None:
        # The whole chain through the REAL decoder against the committed
        # captures: the builder emits the inclusive label pair, the
        # decoder injects page_no/per_page on page one and merges the
        # offset advance onto the SENT spec (so the labels persist), and
        # every emitted record on both pages lands stamped with the
        # labels verbatim.
        endpoint = _build_endpoint()
        first = endpoint.page_decoder.first_request(
            endpoint.spec_builder.build_spec(resume=_window(), member_values={})
        )
        assert first.params == {
            'start_date': '2026-01-05',
            'end_date': '2026-01-05',
            'page_no': '1',
            'per_page': '100',
        }
        continued = endpoint.page_decoder.decode_page(
            first, VEHICLE_UTILIZATIONS_PAGE_1_RESPONSE
        )
        next_spec = continued.advance.next_spec
        assert next_spec is not None
        assert next_spec.params == {
            'start_date': '2026-01-05',
            'end_date': '2026-01-05',
            'page_no': '2',
            'per_page': '2',
        }
        terminal = endpoint.page_decoder.decode_page(
            next_spec, VEHICLE_UTILIZATIONS_PAGE_2_RESPONSE
        )
        assert terminal.advance.next_spec is None
        walked = continued.records + terminal.records
        assert len(walked) == 3
        for record in walked:
            assert record['windowStartDate'] == '2026-01-05'
            assert record['windowEndDate'] == '2026-01-05'

    def test_the_walked_records_validate_against_the_model(self) -> None:
        # The stamped decoder output IS the model's input grain: every
        # record of the two-page walk validates, the date labels lifted
        # to their UTC-midnight instants.
        endpoint = _build_endpoint()
        first = endpoint.page_decoder.first_request(
            endpoint.spec_builder.build_spec(resume=_window(), member_values={})
        )
        continued = endpoint.page_decoder.decode_page(
            first, VEHICLE_UTILIZATIONS_PAGE_1_RESPONSE
        )
        assert continued.advance.next_spec is not None
        terminal = endpoint.page_decoder.decode_page(
            continued.advance.next_spec, VEHICLE_UTILIZATIONS_PAGE_2_RESPONSE
        )
        for record in continued.records + terminal.records:
            utilization = VehicleUtilization.model_validate(record)
            assert utilization.window_start == datetime(2026, 1, 5, tzinfo=UTC)
            assert utilization.window_end == datetime(2026, 1, 5, tzinfo=UTC)

    def test_constructs_without_raising(self) -> None:
        # The WatermarkMode + DATE_PARTITIONED + event_time_column=
        # 'window_start' triple passes EndpointDefinition's construction
        # validation against the VehicleUtilization model.
        endpoint = build_endpoint(MotiveConfig())
        assert endpoint.name == 'vehicle_utilizations'
