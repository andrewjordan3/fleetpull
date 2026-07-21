"""Tests for fleetpull.endpoints.samsara.gps_readings.

The gps arm of the vehicle-stats triple (probe-settled 2026-07-20): a
fleet-wide ``SingleFetch`` whose shared leaf builder renders the resume
window as RFC3339 ``startTime``/``endTime`` plus the FIXED
``types=gps`` selector, paired with ``SamsaraVehicleSeriesPageDecoder``
at the surface's probed 512-limit tier. This module pins the binding's
own facts; the class-level builder behaviors shared by the three
leaves are pinned once in ``test_engine_states``.
"""

from datetime import UTC, datetime, timedelta

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.samsara._spec_builders import SamsaraVehicleStatsSpecBuilder
from fleetpull.endpoints.samsara.gps_readings import build_endpoint
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SingleFetch,
    SpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.models.samsara import GpsReading
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import SamsaraVehicleSeriesPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope
from tests.samsara_gps_readings_capture import (
    GPS_READINGS_PAGE_RESPONSE,
    GPS_READINGS_TERMINAL_RESPONSE,
)


def _build_endpoint() -> EndpointDefinition[GpsReading]:
    return build_endpoint(SamsaraConfig())


def _window() -> DateWindow:
    return DateWindow(
        start=datetime(2026, 1, 1, tzinfo=UTC),
        end=datetime(2026, 1, 8, tzinfo=UTC),
    )


class TestGpsReadingsSpecBuilder:
    def test_satisfies_spec_builder_protocol(self) -> None:
        builder: SpecBuilder = _build_endpoint().spec_builder
        assert isinstance(builder, SamsaraVehicleStatsSpecBuilder)

    def test_builds_the_get_with_the_type_selector_and_window(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(), member_values={}
        )
        assert spec.method is HttpMethod.GET
        assert spec.url == 'https://api.samsara.com/fleet/vehicles/stats/history'
        assert spec.params == {
            'types': 'gps',
            'startTime': '2026-01-01T00:00:00Z',
            'endTime': '2026-01-08T00:00:00Z',
        }


class TestBuildGpsReadingsEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.SAMSARA
        assert endpoint.name == 'gps_readings'
        assert endpoint.response_model is GpsReading
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
        # type, at the surface's probed maximum: 512 (513 is a loud
        # HTTP 400) -- never assume a sibling's limit.
        decoder = _build_endpoint().page_decoder
        assert isinstance(decoder, SamsaraVehicleSeriesPageDecoder)
        assert decoder.records_key == 'data'
        assert decoder.results_limit == 512
        assert decoder.series_key == 'gps'

    def test_declares_the_default_single_fetch_shape(self) -> None:
        assert isinstance(_build_endpoint().request_shape, SingleFetch)

    def test_the_window_and_type_ride_every_page_of_the_walk(self) -> None:
        # The builder emits the window plus the types selector; the
        # decoder injects limit on page one and merges `after` onto the
        # SENT spec thereafter -- pinned against the committed captures.
        endpoint = _build_endpoint()
        first = endpoint.page_decoder.first_request(
            endpoint.spec_builder.build_spec(resume=_window(), member_values={})
        )
        assert first is not None
        assert first.params == {
            'types': 'gps',
            'startTime': '2026-01-01T00:00:00Z',
            'endTime': '2026-01-08T00:00:00Z',
            'limit': '512',
        }
        continued = endpoint.page_decoder.decode_page(first, GPS_READINGS_PAGE_RESPONSE)
        next_spec = continued.advance.next_spec
        assert next_spec is not None
        assert next_spec.params == {
            'types': 'gps',
            'startTime': '2026-01-01T00:00:00Z',
            'endTime': '2026-01-08T00:00:00Z',
            'limit': '512',
            'after': '00000000-0000-0000-0000-000000000051',
        }
        terminal = endpoint.page_decoder.decode_page(
            next_spec, GPS_READINGS_TERMINAL_RESPONSE
        )
        assert terminal.advance.next_spec is None

    def test_constructs_without_raising(self) -> None:
        # The WatermarkMode + DATE_PARTITIONED + event_time_column=
        # 'time' triple passes EndpointDefinition's construction
        # validation against the GpsReading model.
        endpoint = build_endpoint(SamsaraConfig())
        assert endpoint.name == 'gps_readings'
