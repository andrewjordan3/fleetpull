"""Tests for fleetpull.endpoints.samsara.asset_locations.

The binding declares the first ``BatchedRosterFanOut`` (probe-settled
2026-07-20): the surface REQUIRES an ``ids`` filter (an id-less request
is a loud HTTP 400) with the batch cap API-enforced at 50, so the leaf
builder merges each sorted comma-joined batch verbatim as a query
parameter (the trips member-merge precedent) beside the resume window
rendered as RFC3339 ``startTime``/``endTime`` (the idling_events
precedent), paired with the standard ``SamsaraCursorPageDecoder`` at
the surface's probed 512-limit tier. Records arrive at the reading
grain with per-record asset attribution, so the batch is transport
packing only.
"""

from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.samsara.asset_locations import (
    SamsaraAssetLocationsSpecBuilder,
    build_endpoint,
)
from fleetpull.endpoints.shared import (
    BatchedRosterFanOut,
    EndpointDefinition,
    SpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.models.samsara import AssetLocation
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import SamsaraCursorPageDecoder
from fleetpull.roster import RosterKey
from fleetpull.vocabulary import Provider, QuotaScope
from tests.samsara_asset_locations_capture import (
    ASSET_LOCATIONS_PAGE_RESPONSE,
    ASSET_LOCATIONS_TERMINAL_RESPONSE,
)

_IDS_PARAM = 'ids'
# One sorted, comma-joined batch value -- the member string the shape
# seam's chunking hands each chain.
_SYNTHETIC_BATCH = '281474981110001,281474981110002,281474981110003'


def _build_endpoint() -> EndpointDefinition[AssetLocation]:
    return build_endpoint(SamsaraConfig())


def _window() -> DateWindow:
    return DateWindow(
        start=datetime(2026, 1, 1, tzinfo=UTC),
        end=datetime(2026, 1, 8, tzinfo=UTC),
    )


class TestAssetLocationsSpecBuilder:
    def test_satisfies_spec_builder_protocol(self) -> None:
        builder: SpecBuilder = _build_endpoint().spec_builder
        assert isinstance(builder, SamsaraAssetLocationsSpecBuilder)

    def test_builds_the_get_with_batch_and_window(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(),
            member_values={_IDS_PARAM: _SYNTHETIC_BATCH},
        )
        assert spec.method is HttpMethod.GET
        assert spec.url == 'https://api.samsara.com/assets/location-and-speed/stream'
        # The batch binding merges verbatim (the fan-out member key IS
        # the wire query parameter -- `ids` is REQUIRED by the wire),
        # and the half-open window renders as RFC3339 bounds.
        assert spec.params == {
            _IDS_PARAM: _SYNTHETIC_BATCH,
            'startTime': '2026-01-01T00:00:00Z',
            'endTime': '2026-01-08T00:00:00Z',
        }

    def test_configured_base_url_is_used(self) -> None:
        config = SamsaraConfig(base_url='https://alt.example.test/')
        spec = build_endpoint(config).spec_builder.build_spec(
            resume=_window(),
            member_values={_IDS_PARAM: _SYNTHETIC_BATCH},
        )
        # The config strips the trailing slash so the path joins cleanly.
        assert spec.url == 'https://alt.example.test/assets/location-and-speed/stream'

    def test_requires_a_date_window(self) -> None:
        with pytest.raises(TypeError):
            _build_endpoint().spec_builder.build_spec(
                resume=None,
                member_values={_IDS_PARAM: _SYNTHETIC_BATCH},
            )

    def test_no_credentials_or_body(self) -> None:
        spec = _build_endpoint().spec_builder.build_spec(
            resume=_window(),
            member_values={_IDS_PARAM: _SYNTHETIC_BATCH},
        )
        assert spec.headers == {}
        assert spec.json_body is None


class TestBuildAssetLocationsEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.SAMSARA
        assert endpoint.name == 'asset_locations'
        assert endpoint.response_model is AssetLocation
        assert endpoint.quota_scope is QuotaScope.SAMSARA
        assert endpoint.storage_kind is StorageKind.DATE_PARTITIONED
        assert endpoint.event_time_column == 'happened_at_time'
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

    def test_uses_the_cursor_decoder_at_the_512_limit_tier(self) -> None:
        # Records are already reading-grain, so the STANDARD cursor
        # decoder fits (no series unnesting), at the surface's probed
        # maximum: 512 (513 is a loud HTTP 400) -- never assume a
        # sibling's limit.
        decoder = _build_endpoint().page_decoder
        assert isinstance(decoder, SamsaraCursorPageDecoder)
        assert decoder.records_key == 'data'
        assert decoder.results_limit == 512

    def test_declares_the_batched_roster_fan_out(self) -> None:
        # The first BatchedRosterFanOut consumer: `ids` REQUIRED by the
        # wire, the cap API-enforced at 50 (probed: 50 ids -> 200;
        # 100/200/609 -> 400 'Need to filter by 50 or less asset IDs
        # or syncTokens.').
        shape = _build_endpoint().request_shape
        assert isinstance(shape, BatchedRosterFanOut)
        assert shape.roster == RosterKey(Provider.SAMSARA, 'vehicle_ids')
        # The member key must match the wire query parameter the spec
        # builder merges verbatim
        # (test_builds_the_get_with_batch_and_window exercises the
        # pairing end to end).
        assert shape.member_key == _IDS_PARAM
        assert shape.batch_size == 50

    def test_the_window_and_batch_ride_every_page_of_the_walk(self) -> None:
        # The builder emits the window plus the batch binding; the
        # decoder injects limit on page one and merges `after` onto the
        # SENT spec thereafter -- pinned against the committed captures
        # (the fat composite endCursor passed back verbatim).
        endpoint = _build_endpoint()
        first = endpoint.page_decoder.first_request(
            endpoint.spec_builder.build_spec(
                resume=_window(),
                member_values={_IDS_PARAM: _SYNTHETIC_BATCH},
            )
        )
        assert first is not None
        assert first.params == {
            _IDS_PARAM: _SYNTHETIC_BATCH,
            'startTime': '2026-01-01T00:00:00Z',
            'endTime': '2026-01-08T00:00:00Z',
            'limit': '512',
        }
        continued = endpoint.page_decoder.decode_page(
            first, ASSET_LOCATIONS_PAGE_RESPONSE
        )
        next_spec = continued.advance.next_spec
        assert next_spec is not None
        assert next_spec.params == {
            _IDS_PARAM: _SYNTHETIC_BATCH,
            'startTime': '2026-01-01T00:00:00Z',
            'endTime': '2026-01-08T00:00:00Z',
            'limit': '512',
            'after': (
                'eyJzeW50aGV0aWMiOiJjb21wb3NpdGUtY3Vyc29yLTAwMDEiLCJvZmZzZXQiOjN9'
            ),
        }
        terminal = endpoint.page_decoder.decode_page(
            next_spec, ASSET_LOCATIONS_TERMINAL_RESPONSE
        )
        assert terminal.advance.next_spec is None

    def test_constructs_without_raising(self) -> None:
        # The WatermarkMode + DATE_PARTITIONED + event_time_column=
        # 'happened_at_time' triple passes EndpointDefinition's
        # construction validation against the AssetLocation model.
        endpoint = build_endpoint(SamsaraConfig())
        assert endpoint.name == 'asset_locations'
