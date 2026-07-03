"""Tests for fleetpull.endpoints.motive.vehicles."""

from fleetpull.config import MotiveConfig
from fleetpull.endpoints.motive.vehicles import VEHICLE_IDS_ROSTER, build_endpoint
from fleetpull.endpoints.shared import EndpointDefinition, SnapshotMode, StorageKind
from fleetpull.models.motive import Vehicle
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import MotiveWrappedListPageDecoder
from fleetpull.roster import RosterKey
from fleetpull.vocabulary import Provider, QuotaScope


def _make_endpoint() -> EndpointDefinition[Vehicle]:
    return build_endpoint(MotiveConfig(base_url='https://api.example.test'))


class TestBuildVehiclesEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _make_endpoint()
        assert endpoint.provider is Provider.MOTIVE
        assert endpoint.name == 'vehicles'
        assert endpoint.response_model is Vehicle
        assert endpoint.quota_scope is QuotaScope.MOTIVE
        assert endpoint.storage_kind is StorageKind.SINGLE
        assert isinstance(endpoint.sync_mode, SnapshotMode)

    def test_uses_the_motive_wrapped_list_decoder(self) -> None:
        decoder = _make_endpoint().page_decoder
        assert isinstance(decoder, MotiveWrappedListPageDecoder)
        assert decoder.list_key == 'vehicles'
        assert decoder.item_key == 'vehicle'

    def test_decoder_page_size_follows_config(self) -> None:
        endpoint = build_endpoint(
            MotiveConfig(base_url='https://api.example.test', records_per_page=25)
        )
        decoder = endpoint.page_decoder
        assert isinstance(decoder, MotiveWrappedListPageDecoder)
        assert decoder.per_page == 25

    def test_page_size_defaults_to_one_hundred(self) -> None:
        decoder = build_endpoint(MotiveConfig()).page_decoder
        assert isinstance(decoder, MotiveWrappedListPageDecoder)
        assert decoder.per_page == 100

    def test_spec_builder_joins_config_base_url_to_path(self) -> None:
        spec = _make_endpoint().spec_builder.build_spec(resume=None, path_values={})
        assert spec.method is HttpMethod.GET
        assert spec.url == 'https://api.example.test/v1/vehicles'

    def test_base_url_default_flows_through(self) -> None:
        endpoint = build_endpoint(MotiveConfig())
        spec = endpoint.spec_builder.build_spec(resume=None, path_values={})
        assert spec.url == 'https://api.gomotive.com/v1/vehicles'

    def test_does_not_fan_out(self) -> None:
        assert _make_endpoint().fan_out is None


class TestVehicleIdsRoster:
    def test_is_fed_by_this_modules_listing(self) -> None:
        assert VEHICLE_IDS_ROSTER.key == RosterKey(Provider.MOTIVE, 'vehicle_ids')
        assert VEHICLE_IDS_ROSTER.source_endpoint == 'vehicles'
        assert VEHICLE_IDS_ROSTER.source_column == 'vehicle_id'

    def test_declares_hysteresis_not_append_only(self) -> None:
        # Vehicle ids evict on consecutive absence (an efficiency lever, not
        # append-only); the include-inactive guarantee binds at the feeder
        # population (/v1/vehicles lists inactive and retired vehicles), not
        # at this policy.
        assert VEHICLE_IDS_ROSTER.eviction_threshold is not None
        assert VEHICLE_IDS_ROSTER.max_age.total_seconds() > 0
