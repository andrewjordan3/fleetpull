"""Tests for fleetpull.endpoints.samsara.engine_states.

The binding declares the first vehicle-stats leaf (probe-settled
2026-07-20): a fleet-wide ``SingleFetch`` (the decoder synthesizes
per-reading vehicle attribution, so no fan-out -- the default shape,
declared by declaring nothing) whose shared leaf builder renders the
resume window as RFC3339 ``startTime``/``endTime`` plus the FIXED
``types=engineStates`` selector, paired with
``SamsaraVehicleSeriesPageDecoder`` at THIS surface's probed 512-limit
tier (512 -> 200, 513 -> 400; NOT idling's 200). The window and type
parameters persist across the vehicle-axis walk because the inner
cursor decoder's ``after`` advance merges onto the sent spec.
Retrieval is READING-TIME anchored on the half-open window, so
ownership anchors on ``time`` with retrieval and routing coinciding
natively.

The three vehicle-stats leaves share one builder class
(``SamsaraVehicleStatsSpecBuilder``), so the class-level builder
behaviors (sub-second rendering, the DateWindow guard, the
credential-less spec, base-URL joining) are pinned here once; the
sibling test modules pin their own bindings' facts.
"""

from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.samsara._spec_builders import SamsaraVehicleStatsSpecBuilder
from fleetpull.endpoints.samsara.engine_states import build_endpoint
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SingleFetch,
    SpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.models.samsara import EngineState
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import SamsaraVehicleSeriesPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope
from tests.samsara_engine_states_capture import (
    ENGINE_STATES_PAGE_RESPONSE,
    ENGINE_STATES_TERMINAL_RESPONSE,
)


def _build_endpoint() -> EndpointDefinition[EngineState]:
    return build_endpoint(SamsaraConfig())


def _window() -> DateWindow:
    # 2026-01-01T00:00:00Z .. 2026-01-08T00:00:00Z -- the default 7-day
    # chunk width.
    return DateWindow(
        start=datetime(2026, 1, 1, tzinfo=UTC),
        end=datetime(2026, 1, 8, tzinfo=UTC),
    )


class TestEngineStatesSpecBuilder:
    def test_satisfies_spec_builder_protocol(self) -> None:
        builder: SpecBuilder = _build_endpoint().spec_builder
        assert isinstance(builder, SamsaraVehicleStatsSpecBuilder)

    def test_builds_the_get_with_the_type_selector_and_window(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(), member_values={}
        )
        assert spec.method is HttpMethod.GET
        assert spec.url == 'https://api.samsara.com/fleet/vehicles/stats/history'
        # The FIXED types selector rides beside the half-open window's
        # RFC3339 bounds; pagination parameters are the decoder's,
        # injected by its first_request, so they do not appear here.
        assert spec.params == {
            'types': 'engineStates',
            'startTime': '2026-01-01T00:00:00Z',
            'endTime': '2026-01-08T00:00:00Z',
        }

    def test_sub_second_bounds_render_at_seconds_precision(self) -> None:
        # to_iso8601 is second-granular by contract; a sub-second bound
        # truncates rather than drifting the wire shape.
        window = DateWindow(
            start=datetime(2026, 1, 1, 0, 0, 0, 123000, tzinfo=UTC),
            end=datetime(2026, 1, 1, 1, 0, 0, tzinfo=UTC),
        )
        spec = _build_endpoint().spec_builder.build_spec(
            resume=window, member_values={}
        )
        assert spec.params is not None
        assert spec.params['startTime'] == '2026-01-01T00:00:00Z'
        assert spec.params['endTime'] == '2026-01-01T01:00:00Z'

    def test_configured_base_url_is_used(self) -> None:
        config = SamsaraConfig(base_url='https://alt.example.test/')
        spec = build_endpoint(config).spec_builder.build_spec(
            resume=_window(), member_values={}
        )
        # The config strips the trailing slash so the path joins cleanly.
        assert spec.url == 'https://alt.example.test/fleet/vehicles/stats/history'

    def test_requires_a_date_window(self) -> None:
        with pytest.raises(TypeError):
            _build_endpoint().spec_builder.build_spec(resume=None, member_values={})

    def test_no_credentials_or_body(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(), member_values={}
        )
        assert spec.headers == {}
        assert spec.json_body is None


class TestBuildEngineStatesEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.SAMSARA
        assert endpoint.name == 'engine_states'
        assert endpoint.response_model is EngineState
        assert endpoint.quota_scope is QuotaScope.SAMSARA
        assert endpoint.storage_kind is StorageKind.DATE_PARTITIONED
        assert endpoint.event_time_column == 'time'
        assert isinstance(endpoint.sync_mode, WatermarkMode)
        assert endpoint.completeness_check is None

    def test_watermark_knobs_flow_from_config(self) -> None:
        default_endpoint = _build_endpoint()
        assert isinstance(default_endpoint.sync_mode, WatermarkMode)
        assert default_endpoint.sync_mode.lookback == timedelta(days=7)
        assert default_endpoint.sync_mode.cutoff == timedelta(days=0)
        custom = build_endpoint(SamsaraConfig(lookback_days=2, cutoff_days=1))
        assert isinstance(custom.sync_mode, WatermarkMode)
        assert custom.sync_mode.lookback == timedelta(days=2)
        assert custom.sync_mode.cutoff == timedelta(days=1)

    def test_uses_the_series_decoder_at_the_512_limit_tier(self) -> None:
        # The series-unnesting decoder bound to THIS endpoint's stat
        # type, at THIS surface's probed maximum: 512 (513 is a loud
        # HTTP 400) -- the vehicles/drivers tier, NOT idling's 200;
        # never assume a sibling's limit.
        decoder = _build_endpoint().page_decoder
        assert isinstance(decoder, SamsaraVehicleSeriesPageDecoder)
        assert decoder.records_key == 'data'
        assert decoder.results_limit == 512
        assert decoder.series_key == 'engineStates'

    def test_declares_the_default_single_fetch_shape(self) -> None:
        # Fleet-wide with decoder-synthesized vehicle attribution: one
        # chain, no fan-out -- the SingleFetch default, declared by
        # declaring nothing.
        assert isinstance(_build_endpoint().request_shape, SingleFetch)

    def test_the_window_and_type_ride_every_page_of_the_walk(self) -> None:
        # The builder emits the window plus the types selector; the
        # decoder injects limit on page one and merges `after` onto the
        # SENT spec thereafter, so all three persist across the whole
        # vehicle-axis walk -- pinned here against the committed
        # continuation and terminal captures.
        endpoint = _build_endpoint()
        first = endpoint.page_decoder.first_request(
            endpoint.spec_builder.build_spec(resume=_window(), member_values={})
        )
        assert first is not None
        assert first.params == {
            'types': 'engineStates',
            'startTime': '2026-01-01T00:00:00Z',
            'endTime': '2026-01-08T00:00:00Z',
            'limit': '512',
        }
        continued = endpoint.page_decoder.decode_page(
            first, ENGINE_STATES_PAGE_RESPONSE
        )
        next_spec = continued.advance.next_spec
        assert next_spec is not None
        assert next_spec.params == {
            'types': 'engineStates',
            'startTime': '2026-01-01T00:00:00Z',
            'endTime': '2026-01-08T00:00:00Z',
            'limit': '512',
            'after': '00000000-0000-0000-0000-000000000041',
        }
        terminal = endpoint.page_decoder.decode_page(
            next_spec, ENGINE_STATES_TERMINAL_RESPONSE
        )
        assert terminal.advance.next_spec is None

    def test_constructs_without_raising(self) -> None:
        # The WatermarkMode + DATE_PARTITIONED + event_time_column=
        # 'time' triple passes EndpointDefinition's construction
        # validation against the EngineState model.
        endpoint = build_endpoint(SamsaraConfig())
        assert endpoint.name == 'engine_states'
