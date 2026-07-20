"""Tests for fleetpull.endpoints.samsara.vehicles.

The binding rides the shared snapshot spec-builder and the cursor
decoder proven live 2026-07-17; no completeness check is declared
because the cursor walk is complete by construction (continuation is
explicit per page, and the decoder fails loudly on a promised
continuation without a cursor). The module also declares the Samsara
``vehicle_ids`` roster the trips fan-out reads, beside its feeder.
"""

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.samsara.vehicles import VEHICLE_IDS_ROSTER, build_endpoint
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SingleFetch,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.models.samsara import Vehicle
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import SamsaraCursorPageDecoder
from fleetpull.roster import RosterKey
from fleetpull.vocabulary import Provider, QuotaScope


def _build_endpoint() -> EndpointDefinition[Vehicle]:
    return build_endpoint(SamsaraConfig())


class TestVehiclesSpecBuilder:
    def test_builds_the_static_get(self) -> None:
        endpoint = _build_endpoint()
        assert isinstance(endpoint.spec_builder, StaticGetSpecBuilder)
        spec = endpoint.spec_builder.build_spec(resume=None, member_values={})
        assert spec.method is HttpMethod.GET
        assert spec.url == 'https://api.samsara.com/fleet/vehicles'

    def test_configured_base_url_is_used(self) -> None:
        config = SamsaraConfig(base_url='https://alt.example.test/')
        spec = build_endpoint(config).spec_builder.build_spec(
            resume=None, member_values={}
        )
        # The config strips the trailing slash so the path joins cleanly.
        assert spec.url == 'https://alt.example.test/fleet/vehicles'


class TestBuildVehiclesEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.SAMSARA
        assert endpoint.name == 'vehicles'
        assert endpoint.quota_scope is QuotaScope.SAMSARA
        assert endpoint.storage_kind is StorageKind.SINGLE
        assert endpoint.response_model is Vehicle
        assert isinstance(endpoint.sync_mode, SnapshotMode)
        assert endpoint.request_shape == SingleFetch()
        assert endpoint.completeness_check is None

    def test_the_decoder_is_the_cursor_walk_at_the_documented_max(self) -> None:
        endpoint = _build_endpoint()
        decoder = endpoint.page_decoder
        assert isinstance(decoder, SamsaraCursorPageDecoder)
        assert decoder.records_key == 'data'
        assert decoder.results_limit == 512


class TestVehicleIdsRoster:
    def test_is_fed_by_this_modules_listing(self) -> None:
        assert VEHICLE_IDS_ROSTER.key == RosterKey(Provider.SAMSARA, 'vehicle_ids')
        assert VEHICLE_IDS_ROSTER.source_endpoint == 'vehicles'
        # The vehicles frame's id column: the top-level model field
        # 'id', flattened verbatim.
        assert VEHICLE_IDS_ROSTER.source_column == 'id'

    def test_declares_hysteresis_not_append_only(self) -> None:
        # Vehicle ids evict on consecutive absence (an efficiency
        # lever, not append-only); unplugged units stay covered at the
        # feeder population (/fleet/vehicles lists them, captured
        # 2026-07-17), and this hysteresis is what retires a member the
        # listing stops returning.
        assert VEHICLE_IDS_ROSTER.eviction_threshold is not None
        assert VEHICLE_IDS_ROSTER.max_age.total_seconds() > 0
