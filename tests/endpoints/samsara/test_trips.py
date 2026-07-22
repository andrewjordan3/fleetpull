"""Tests for fleetpull.endpoints.samsara.trips.

The binding declares the roster machinery's first cross-provider
``RosterFanOut`` (probe-settled 2026-07-20): the legacy v1-only surface
requires ``vehicleId``, so the leaf builder merges the fan-out member
verbatim as a query parameter (the drivers-leaf precedent) beside the
resume window rendered as ``startMs``/``endMs`` epoch milliseconds; the
unpaginated ``{"trips": [...]}`` envelope pairs with
``SamsaraTripsPageDecoder``, which stamps the fan-out ``vehicleId`` onto
every record (the wire never echoes it); and retrieval is
overlap-anchored, so ownership anchors on ``start_time`` with no wire
pad (DESIGN §4).
"""

from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.samsara.trips import (
    SamsaraTripsSpecBuilder,
    build_endpoint,
)
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    RosterFanOut,
    SpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.models.samsara import Trip
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import SamsaraTripsPageDecoder
from fleetpull.roster import RosterKey
from fleetpull.vocabulary import Provider, QuotaScope
from tests.samsara_trips_capture import SYNTHETIC_VEHICLE_ID

_VEHICLE_ID_PARAM = 'vehicleId'


def _build_endpoint() -> EndpointDefinition[Trip]:
    return build_endpoint(SamsaraConfig())


def _window() -> DateWindow:
    # 2026-01-01T00:00:00Z .. 2026-01-08T00:00:00Z -- the default 7-day
    # chunk width, far inside the provider's 90-day cap.
    return DateWindow(
        start=datetime(2026, 1, 1, tzinfo=UTC),
        end=datetime(2026, 1, 8, tzinfo=UTC),
    )


class TestTripsSpecBuilder:
    def test_satisfies_spec_builder_protocol(self) -> None:
        builder: SpecBuilder = _build_endpoint().spec_builder
        assert isinstance(builder, SamsaraTripsSpecBuilder)

    def test_builds_the_get_with_member_and_epoch_window(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(),
            member_values={_VEHICLE_ID_PARAM: SYNTHETIC_VEHICLE_ID},
        )
        assert spec.method is HttpMethod.GET
        assert spec.url == 'https://api.samsara.com/v1/fleet/trips'
        # The member binding merges verbatim (the fan-out member key IS
        # the wire query parameter), and the half-open window's bounds
        # render as epoch MILLISECONDS: 2026-01-01T00:00:00Z is
        # 1767225600 s and the exclusive end is 7 days (604800 s)
        # later, both times 1000.
        assert spec.params == {
            _VEHICLE_ID_PARAM: SYNTHETIC_VEHICLE_ID,
            'startMs': '1767225600000',
            'endMs': '1767830400000',
        }

    def test_epoch_conversion_is_millisecond_exact(self) -> None:
        # A sub-second bound survives to the millisecond -- exact
        # integer arithmetic, no second-flooring.
        window = DateWindow(
            start=datetime(2026, 1, 1, 0, 0, 0, 123000, tzinfo=UTC),
            end=datetime(2026, 1, 1, 1, 0, 0, tzinfo=UTC),
        )
        spec = _build_endpoint().spec_builder.build_spec(
            resume=window,
            member_values={_VEHICLE_ID_PARAM: SYNTHETIC_VEHICLE_ID},
        )
        assert spec.params is not None
        assert spec.params['startMs'] == '1767225600123'
        assert spec.params['endMs'] == '1767229200000'

    def test_epoch_conversion_survives_float_inexact_instants(self) -> None:
        # 2039-03-03T01:19:32.319Z renders one ms low under float
        # `.timestamp() * 1000` + int() truncation (2182727972318); the
        # integer-arithmetic conversion must hold the exact value.
        window = DateWindow(
            start=datetime(2039, 3, 3, 1, 19, 32, 319000, tzinfo=UTC),
            end=datetime(2039, 3, 4, tzinfo=UTC),
        )
        spec = _build_endpoint().spec_builder.build_spec(
            resume=window,
            member_values={_VEHICLE_ID_PARAM: SYNTHETIC_VEHICLE_ID},
        )
        assert spec.params is not None
        assert spec.params['startMs'] == '2182727972319'

    def test_configured_base_url_is_used(self) -> None:
        config = SamsaraConfig(base_url='https://alt.example.test/')
        spec = build_endpoint(config).spec_builder.build_spec(
            resume=_window(),
            member_values={_VEHICLE_ID_PARAM: SYNTHETIC_VEHICLE_ID},
        )
        # The config strips the trailing slash so the path joins cleanly.
        assert spec.url == 'https://alt.example.test/v1/fleet/trips'

    def test_requires_a_date_window(self) -> None:
        with pytest.raises(TypeError):
            _build_endpoint().spec_builder.build_spec(
                resume=None,
                member_values={_VEHICLE_ID_PARAM: SYNTHETIC_VEHICLE_ID},
            )

    def test_no_credentials_or_body(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(),
            member_values={_VEHICLE_ID_PARAM: SYNTHETIC_VEHICLE_ID},
        )
        assert spec.headers == {}
        assert spec.json_body is None


class TestBuildTripsEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.SAMSARA
        assert endpoint.name == 'trips'
        assert endpoint.response_model is Trip
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

    def test_uses_the_trips_stamp_decoder_on_the_trips_key(self) -> None:
        # One unpaginated response per (vehicle, window); the decoder
        # stamps the fan-out vehicleId onto every record (the wire never
        # echoes it). member_key is the same token the RosterFanOut fans
        # out, so the stamp reads back exactly what was sent.
        decoder = _build_endpoint().page_decoder
        assert isinstance(decoder, SamsaraTripsPageDecoder)
        assert decoder.records_key == 'trips'
        assert decoder.member_key == _VEHICLE_ID_PARAM

    def test_declares_the_cross_provider_roster_fan_out(self) -> None:
        shape = _build_endpoint().request_shape
        assert isinstance(shape, RosterFanOut)
        assert shape.roster == RosterKey(Provider.SAMSARA, 'vehicle_ids')
        # The member key must match the wire query parameter the spec
        # builder merges verbatim
        # (test_builds_the_get_with_member_and_epoch_window exercises
        # the pairing end to end).
        assert shape.member_key == _VEHICLE_ID_PARAM

    def test_constructs_without_raising(self) -> None:
        # The WatermarkMode + DATE_PARTITIONED + event_time_column=
        # 'start_time' triple passes EndpointDefinition's construction
        # validation against the Trip model.
        endpoint = build_endpoint(SamsaraConfig())
        assert endpoint.name == 'trips'
