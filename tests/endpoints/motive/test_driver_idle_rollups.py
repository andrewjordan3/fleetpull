"""Tests for fleetpull.endpoints.motive.driver_idle_rollups.

The driver arm of the Motive utilization rollup pair (probe-settled
2026-07-21): the vehicle arm's binding with the path, wrapper keys, and
model swapped. NOTE the envelope vocabulary: the wire's OWN wrapper is
``driver_idle_rollups``/``driver_idle_rollup`` -- a different
vocabulary from the ``/v2/driver_utilization`` path -- and the endpoint
name mirrors the wire. The pair's window-grain facts are shared:
``fixed_unit_days=1``, the decoder-stamped ``window_start`` routing,
and the shared builder's INCLUSIVE ``start_date``/``end_date`` label
pair (company-local interpretation documented, never converted).
"""

from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.config import MotiveConfig
from fleetpull.endpoints.motive._spec_builders import MotiveFleetDateRangeSpecBuilder
from fleetpull.endpoints.motive.driver_idle_rollups import build_endpoint
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SingleFetch,
    SpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.models.motive import DriverIdleRollup
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import MotiveWindowReportPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope
from tests.motive_driver_idle_rollups_capture import (
    DRIVER_IDLE_ROLLUPS_PAGE_1_RESPONSE,
    DRIVER_IDLE_ROLLUPS_PAGE_2_RESPONSE,
)


def _build_endpoint() -> EndpointDefinition[DriverIdleRollup]:
    return build_endpoint(MotiveConfig(base_url='https://api.example.test'))


def _window() -> DateWindow:
    # One day exactly -- the unit width every planned window tiles into
    # under the declared fixed_unit_days=1.
    return DateWindow(
        start=datetime(2026, 1, 5, tzinfo=UTC),
        end=datetime(2026, 1, 6, tzinfo=UTC),
    )


class TestDriverIdleRollupsSpecBuilder:
    def test_satisfies_spec_builder_protocol(self) -> None:
        builder: SpecBuilder = _build_endpoint().spec_builder
        assert isinstance(builder, MotiveFleetDateRangeSpecBuilder)

    def test_maps_the_one_day_unit_to_the_inclusive_label_pair(self) -> None:
        # THE date-label mapping (the pair's, proven inclusive on both
        # ends live): the half-open [2026-01-05, 2026-01-06) unit
        # renders as start_date=2026-01-05&end_date=2026-01-05 against
        # the LEGACY-NAMED wire path -- the endpoint name follows the
        # wire's envelope vocabulary instead.
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(), member_values={}
        )
        assert spec.method is HttpMethod.GET
        assert spec.url == 'https://api.example.test/v2/driver_utilization'
        assert spec.params == {
            'start_date': '2026-01-05',
            'end_date': '2026-01-05',
        }

    def test_spec_builder_carries_no_pad(self) -> None:
        # The window IS the label pair on this surface (the vehicle
        # arm's reasoning): no event time exists to trim against.
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


class TestBuildDriverIdleRollupsEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.MOTIVE
        assert endpoint.name == 'driver_idle_rollups'
        assert endpoint.response_model is DriverIdleRollup
        assert endpoint.quota_scope is QuotaScope.MOTIVE
        assert endpoint.storage_kind is StorageKind.DATE_PARTITIONED
        assert endpoint.event_time_column == 'window_start'
        assert isinstance(endpoint.sync_mode, WatermarkMode)
        assert endpoint.completeness_check is None

    def test_declares_the_fixed_one_day_unit_width(self) -> None:
        # The pair's window-grain declaration: one rollup row per
        # driver per window (13 on a quiet day, 653 across six days --
        # captured 2026-07-21), so the unit width is part of the row's
        # meaning -- pinned to exactly one day, never floating with
        # sync.backfill_chunk_days.
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

    def test_decoder_speaks_the_wires_own_wrapper_vocabulary(self) -> None:
        # The wire's envelope vocabulary, NOT the path's: the wrapper
        # keys are driver_idle_rollups/driver_idle_rollup (captured
        # 2026-07-21) -- the naming decision the endpoint mirrors.
        endpoint = _build_endpoint()
        decoder = endpoint.page_decoder
        assert isinstance(decoder, MotiveWindowReportPageDecoder)
        assert decoder.list_key == 'driver_idle_rollups'
        assert decoder.item_key == 'driver_idle_rollup'
        assert decoder.per_page == 100

    def test_page_size_comes_from_config(self) -> None:
        endpoint = build_endpoint(
            MotiveConfig(base_url='https://api.example.test', records_per_page=50)
        )
        decoder = endpoint.page_decoder
        assert isinstance(decoder, MotiveWindowReportPageDecoder)
        assert decoder.per_page == 50

    def test_declares_the_default_single_fetch_shape(self) -> None:
        # Fleet-wide with per-record driver attribution: one chain, no
        # fan-out -- the SingleFetch default, declared by declaring
        # nothing.
        assert isinstance(_build_endpoint().request_shape, SingleFetch)

    def test_a_two_page_walk_stamps_every_record_with_the_sent_window(self) -> None:
        # The whole chain through the REAL decoder against the committed
        # captures: the builder emits the inclusive label pair, the
        # decoder pages by offset with the labels persisting on the
        # SENT spec, and every emitted record on both pages -- the
        # null-driver bucket row included -- lands stamped verbatim.
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
            first, DRIVER_IDLE_ROLLUPS_PAGE_1_RESPONSE
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
            next_spec, DRIVER_IDLE_ROLLUPS_PAGE_2_RESPONSE
        )
        assert terminal.advance.next_spec is None
        walked = continued.records + terminal.records
        assert len(walked) == 3
        for record in walked:
            assert record['windowStartDate'] == '2026-01-05'
            assert record['windowEndDate'] == '2026-01-05'
        assert terminal.records[0]['driver'] is None

    def test_the_walked_records_validate_against_the_model(self) -> None:
        # The stamped decoder output IS the model's input grain: every
        # record of the two-page walk validates -- the null-driver
        # bucket row landing a None ref -- with the date labels lifted
        # to their UTC-midnight instants.
        endpoint = _build_endpoint()
        first = endpoint.page_decoder.first_request(
            endpoint.spec_builder.build_spec(resume=_window(), member_values={})
        )
        continued = endpoint.page_decoder.decode_page(
            first, DRIVER_IDLE_ROLLUPS_PAGE_1_RESPONSE
        )
        assert continued.advance.next_spec is not None
        terminal = endpoint.page_decoder.decode_page(
            continued.advance.next_spec, DRIVER_IDLE_ROLLUPS_PAGE_2_RESPONSE
        )
        validated = [
            DriverIdleRollup.model_validate(record)
            for record in continued.records + terminal.records
        ]
        for rollup in validated:
            assert rollup.window_start == datetime(2026, 1, 5, tzinfo=UTC)
            assert rollup.window_end == datetime(2026, 1, 5, tzinfo=UTC)
        assert validated[0].driver is not None
        assert validated[2].driver is None

    def test_constructs_without_raising(self) -> None:
        # The WatermarkMode + DATE_PARTITIONED + event_time_column=
        # 'window_start' triple passes EndpointDefinition's construction
        # validation against the DriverIdleRollup model.
        endpoint = build_endpoint(MotiveConfig())
        assert endpoint.name == 'driver_idle_rollups'
