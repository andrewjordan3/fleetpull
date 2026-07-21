"""Tests for fleetpull.endpoints.samsara.driver_vehicle_assignments.

The binding is the idling_events species carrying the trips
overlap-anchoring decisions (probe-settled 2026-07-20): a fleet-wide
``SingleFetch`` (per-record driver AND vehicle attribution, so no
fan-out -- the default shape, declared by declaring nothing) whose leaf
builder renders the resume window as RFC3339 ``startTime``/``endTime``
plus the FIXED ``filterBy=vehicles`` selector (the stats triple's
``types`` idiom -- the two sweeps proved to be ONE dataset, so the axis
is baked in, never swept), paired with the existing
``SamsaraCursorPageDecoder`` at ``results_limit=50`` -- the server's
OWN fixed page size, declared as documentation because the ``limit``
param is proven ignored on this surface (no enforced tier). The window
and the selector persist across the walk because the decoder's
``after`` advance merges onto the sent spec. Retrieval is
OVERLAP-anchored, so ownership anchors on ``start_time`` with the
runner's post-fetch filter assigning each assignment to the single
chunk owning its start (no wire pad -- the trips reasoning).
"""

from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.samsara.driver_vehicle_assignments import (
    SamsaraDriverVehicleAssignmentsSpecBuilder,
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
from fleetpull.models.samsara import DriverVehicleAssignment
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import SamsaraCursorPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope
from tests.samsara_driver_vehicle_assignments_capture import (
    DRIVER_VEHICLE_ASSIGNMENTS_PAGE_RESPONSE,
    DRIVER_VEHICLE_ASSIGNMENTS_TERMINAL_RESPONSE,
)


def _build_endpoint() -> EndpointDefinition[DriverVehicleAssignment]:
    return build_endpoint(SamsaraConfig())


def _window() -> DateWindow:
    # 2026-01-01T00:00:00Z .. 2026-01-08T00:00:00Z -- the default 7-day
    # chunk width (live-proven on this surface, 2026-07-21).
    return DateWindow(
        start=datetime(2026, 1, 1, tzinfo=UTC),
        end=datetime(2026, 1, 8, tzinfo=UTC),
    )


class TestDriverVehicleAssignmentsSpecBuilder:
    def test_satisfies_spec_builder_protocol(self) -> None:
        builder: SpecBuilder = _build_endpoint().spec_builder
        assert isinstance(builder, SamsaraDriverVehicleAssignmentsSpecBuilder)

    def test_builds_the_get_with_the_fixed_filter_and_rfc3339_window(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(), member_values={}
        )
        assert spec.method is HttpMethod.GET
        assert spec.url == 'https://api.samsara.com/fleet/driver-vehicle-assignments'
        # The FIXED traversal selector rides beside the half-open
        # window's RFC3339 bounds: filterBy is REQUIRED and
        # API-enforced to {drivers, vehicles}, and the sweeps proved
        # identical, so 'vehicles' is baked into every request.
        # Pagination parameters are the decoder's, injected by its
        # first_request, so they do not appear here.
        assert spec.params == {
            'filterBy': 'vehicles',
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
        assert spec.url == 'https://alt.example.test/fleet/driver-vehicle-assignments'

    def test_requires_a_date_window(self) -> None:
        with pytest.raises(TypeError):
            _build_endpoint().spec_builder.build_spec(resume=None, member_values={})

    def test_no_credentials_or_body(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(), member_values={}
        )
        assert spec.headers == {}
        assert spec.json_body is None


class TestBuildDriverVehicleAssignmentsEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.SAMSARA
        assert endpoint.name == 'driver_vehicle_assignments'
        assert endpoint.response_model is DriverVehicleAssignment
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

    def test_uses_the_cursor_decoder_at_the_servers_own_page_size(self) -> None:
        # results_limit=50 is documentation-by-declaration: the server
        # pages at a FIXED 50 and the limit param is proven ignored
        # (limit=1/5/100/512/513 and no limit each returned a
        # 50-record first page; 513 NOT rejected -- no enforced tier),
        # so the declaration states the server's own observed page
        # size, never a working knob.
        decoder = _build_endpoint().page_decoder
        assert isinstance(decoder, SamsaraCursorPageDecoder)
        assert decoder.records_key == 'data'
        assert decoder.results_limit == 50

    def test_declares_the_default_single_fetch_shape(self) -> None:
        # Fleet-wide with per-record driver AND vehicle attribution:
        # one chain, no fan-out -- the SingleFetch default, declared by
        # declaring nothing.
        assert isinstance(_build_endpoint().request_shape, SingleFetch)

    def test_the_window_and_filter_ride_every_page_of_the_walk(self) -> None:
        # The builder emits the window plus the fixed filterBy; the
        # decoder injects limit on page one and merges `after` onto the
        # SENT spec thereafter, so all three persist across the whole
        # walk -- pinned here against the committed continuation and
        # terminal captures.
        endpoint = _build_endpoint()
        first = endpoint.page_decoder.first_request(
            endpoint.spec_builder.build_spec(resume=_window(), member_values={})
        )
        assert first is not None
        assert first.params == {
            'filterBy': 'vehicles',
            'startTime': '2026-01-01T00:00:00Z',
            'endTime': '2026-01-08T00:00:00Z',
            'limit': '50',
        }
        continued = endpoint.page_decoder.decode_page(
            first, DRIVER_VEHICLE_ASSIGNMENTS_PAGE_RESPONSE
        )
        next_spec = continued.advance.next_spec
        assert next_spec is not None
        assert next_spec.params == {
            'filterBy': 'vehicles',
            'startTime': '2026-01-01T00:00:00Z',
            'endTime': '2026-01-08T00:00:00Z',
            'limit': '50',
            'after': 'c3ludGgtYXNzaWdubWVudC1jdXJzb3ItMDAx',
        }
        terminal = endpoint.page_decoder.decode_page(
            next_spec, DRIVER_VEHICLE_ASSIGNMENTS_TERMINAL_RESPONSE
        )
        assert terminal.advance.next_spec is None

    def test_constructs_without_raising(self) -> None:
        # The WatermarkMode + DATE_PARTITIONED + event_time_column=
        # 'start_time' triple passes EndpointDefinition's construction
        # validation against the DriverVehicleAssignment model.
        endpoint = build_endpoint(SamsaraConfig())
        assert endpoint.name == 'driver_vehicle_assignments'
