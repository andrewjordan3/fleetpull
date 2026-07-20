"""Tests for fleetpull.endpoints.samsara.idling_events.

The binding declares the first windowed+cursor pairing (probe-settled
2026-07-20): a fleet-wide ``SingleFetch`` (asset attribution rides
every record, so no fan-out -- the default shape, declared by declaring
nothing) whose leaf builder renders the resume window as RFC3339
``startTime``/``endTime``, paired with the existing
``SamsaraCursorPageDecoder`` at THIS endpoint's probed 200-limit tier
(NOT the 512 of vehicles/drivers). The window parameters persist
across the walk because the decoder's ``after`` advance merges onto
the sent spec -- the mechanism proven live on the drivers sweep.
Retrieval is START-anchored on UTC, so ownership anchors on
``start_time`` with retrieval and routing coinciding natively (no wire
pad; the runner's window filter is pure hygiene).
"""

from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.samsara.idling_events import (
    SamsaraIdlingEventsSpecBuilder,
    build_endpoint,
)
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SingleFetch,
    SpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.models.samsara import IdlingEvent
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import SamsaraCursorPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope
from tests.samsara_idling_events_capture import (
    IDLING_EVENTS_PAGE_RESPONSE,
    IDLING_EVENTS_TERMINAL_RESPONSE,
)


def _build_endpoint() -> EndpointDefinition[IdlingEvent]:
    return build_endpoint(SamsaraConfig())


def _window() -> DateWindow:
    # 2026-01-01T00:00:00Z .. 2026-01-08T00:00:00Z -- the default 7-day
    # chunk width, far inside the provider's sub-3-months cap.
    return DateWindow(
        start=datetime(2026, 1, 1, tzinfo=UTC),
        end=datetime(2026, 1, 8, tzinfo=UTC),
    )


class TestIdlingEventsSpecBuilder:
    def test_satisfies_spec_builder_protocol(self) -> None:
        builder: SpecBuilder = _build_endpoint().spec_builder
        assert isinstance(builder, SamsaraIdlingEventsSpecBuilder)

    def test_builds_the_get_with_the_rfc3339_window(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(), member_values={}
        )
        assert spec.method is HttpMethod.GET
        assert spec.url == 'https://api.samsara.com/idling/events'
        # The half-open window's bounds render as RFC3339 'Z' strings,
        # verbatim seconds precision -- the timing codec's to_iso8601.
        # Pagination parameters are the decoder's, injected by its
        # first_request, so they do not appear here.
        assert spec.params == {
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
        assert spec.url == 'https://alt.example.test/idling/events'

    def test_requires_a_date_window(self) -> None:
        with pytest.raises(TypeError):
            _build_endpoint().spec_builder.build_spec(resume=None, member_values={})

    def test_no_credentials_or_body(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(), member_values={}
        )
        assert spec.headers == {}
        assert spec.json_body is None


class TestBuildIdlingEventsEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.SAMSARA
        assert endpoint.name == 'idling_events'
        assert endpoint.response_model is IdlingEvent
        assert endpoint.quota_scope is QuotaScope.SAMSARA
        assert endpoint.storage_kind is StorageKind.DATE_PARTITIONED
        assert endpoint.event_time_column == 'start_time'
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

    def test_uses_the_cursor_decoder_at_the_200_limit_tier(self) -> None:
        # The existing cursor decoder, at THIS endpoint's probed
        # maximum: 200, NOT the 512 of vehicles/drivers (limit=512 is a
        # loud JSON 400 naming the cap) -- never assume a sibling's
        # limit.
        decoder = _build_endpoint().page_decoder
        assert isinstance(decoder, SamsaraCursorPageDecoder)
        assert decoder.records_key == 'data'
        assert decoder.results_limit == 200

    def test_declares_the_default_single_fetch_shape(self) -> None:
        # Fleet-wide with per-record asset attribution: one chain, no
        # fan-out -- the SingleFetch default, declared by declaring
        # nothing.
        assert isinstance(_build_endpoint().request_shape, SingleFetch)

    def test_the_window_rides_every_page_of_the_walk(self) -> None:
        # The first windowed+cursor pairing: the builder emits only the
        # window; the decoder injects limit on page one and merges
        # `after` onto the SENT spec thereafter, so startTime/endTime
        # persist across the whole walk -- pinned here against the
        # committed continuation and terminal captures.
        endpoint = _build_endpoint()
        first = endpoint.page_decoder.first_request(
            endpoint.spec_builder.build_spec(resume=_window(), member_values={})
        )
        assert first is not None
        assert first.params == {
            'startTime': '2026-01-01T00:00:00Z',
            'endTime': '2026-01-08T00:00:00Z',
            'limit': '200',
        }
        continued = endpoint.page_decoder.decode_page(
            first, IDLING_EVENTS_PAGE_RESPONSE
        )
        next_spec = continued.advance.next_spec
        assert next_spec is not None
        assert next_spec.params == {
            'startTime': '2026-01-01T00:00:00Z',
            'endTime': '2026-01-08T00:00:00Z',
            'limit': '200',
            'after': '00000000-0000-0000-0000-000000000031',
        }
        terminal = endpoint.page_decoder.decode_page(
            next_spec, IDLING_EVENTS_TERMINAL_RESPONSE
        )
        assert terminal.advance.next_spec is None

    def test_constructs_without_raising(self) -> None:
        # The WatermarkMode + DATE_PARTITIONED + event_time_column=
        # 'start_time' triple passes EndpointDefinition's construction
        # validation against the IdlingEvent model.
        endpoint = build_endpoint(SamsaraConfig())
        assert endpoint.name == 'idling_events'
