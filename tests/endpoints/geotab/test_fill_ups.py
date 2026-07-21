"""Tests for fleetpull.endpoints.geotab.fill_ups.

The binding pins (the feed declaration quintuple: mode, layout,
event-time column, typeName, resultsLimit, decoder, scope) plus the
vertical's drive-through over the real decoder, append writer, and
cursor store. The load-bearing leaf pin here is the 10,000
``resultsLimit`` — the DOCUMENTED FillUp cap with dual provenance (a
50,000 request was accepted at the probed tenant's 380-record
population, so the cap was unprobeable; DESIGN §8). The shared
``GeotabGetFeedSpecBuilder`` wire shapes are pinned in
``test_requests.py``.
"""

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from fleetpull.config import GeotabAuthConfig, GeotabConfig
from fleetpull.endpoints.geotab._requests import GeotabGetFeedSpecBuilder
from fleetpull.endpoints.geotab.fill_ups import build_endpoint
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    FeedMode,
    SingleFetch,
    StorageKind,
)
from fleetpull.incremental import FeedSeed, FeedToken
from fleetpull.models.geotab import FillUp
from fleetpull.network.decoders import GeotabFeedPageDecoder
from fleetpull.orchestrator.outcome import Executed
from fleetpull.vocabulary import Provider, QuotaScope
from tests.endpoints.geotab.feed_harness import drive_feed_endpoint
from tests.geotab_fill_ups_capture import (
    FILL_UPS_FEED_PAGE_1_RESPONSE,
    FILL_UPS_FEED_PAGE_2_RESPONSE,
)


def _build_endpoint() -> EndpointDefinition[FillUp]:
    return build_endpoint(GeotabConfig())


class TestBuildFillUpsEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.GEOTAB
        assert endpoint.name == 'fill_ups'
        assert endpoint.quota_scope is QuotaScope.GEOTAB_FEED
        assert endpoint.storage_kind is StorageKind.APPEND_LOG
        assert isinstance(endpoint.sync_mode, FeedMode)
        assert endpoint.response_model is FillUp
        assert endpoint.event_time_column == 'date_time'
        assert endpoint.request_shape == SingleFetch()
        assert endpoint.completeness_check is None

    def test_composes_the_shared_feed_builder_at_the_documented_cap(self) -> None:
        # 10,000 is the DOCUMENTED per-type cap, declared with dual
        # provenance: the probe could not falsify it (a 50,000 request
        # was accepted at the 380-record population — the cap was
        # structurally unprobeable), so the documented figure stands.
        endpoint = _build_endpoint()
        assert isinstance(endpoint.spec_builder, GeotabGetFeedSpecBuilder)
        assert endpoint.spec_builder.type_name == 'FillUp'
        assert endpoint.spec_builder.results_limit == 10000
        assert endpoint.spec_builder.server == 'my.geotab.com'

    def test_configured_auth_server_is_used(self) -> None:
        config = GeotabConfig(
            auth=GeotabAuthConfig(
                username='user@example.com',
                password='synthetic-password-123',
                database='synthetic_db',
                server='alt.example.test',
            )
        )
        builder = build_endpoint(config).spec_builder
        assert isinstance(builder, GeotabGetFeedSpecBuilder)
        assert builder.server == 'alt.example.test'

    def test_the_decoder_is_the_feed_decoder(self) -> None:
        assert isinstance(_build_endpoint().page_decoder, GeotabFeedPageDecoder)

    def test_credentials_are_never_written_here(self) -> None:
        endpoint = _build_endpoint()
        spec = endpoint.spec_builder.build_spec(
            resume=FeedSeed(start=datetime(2024, 1, 1, tzinfo=UTC)),
            member_values={},
        )
        assert isinstance(spec.json_body, dict)
        params = spec.json_body['params']
        assert isinstance(params, dict)
        assert 'credentials' not in params
        assert params['typeName'] == 'FillUp'


class TestFeedDrive:
    def test_pages_land_as_parts_and_the_token_advances(self, tmp_path: Path) -> None:
        result = drive_feed_endpoint(
            _build_endpoint(),
            [FILL_UPS_FEED_PAGE_1_RESPONSE, FILL_UPS_FEED_PAGE_2_RESPONSE],
            tmp_path,
            page_size=2,
        )
        assert isinstance(result.outcome, Executed)
        assert result.outcome.records_fetched == 3
        part_files = sorted(result.endpoint_dir.rglob('part-*.parquet'))
        assert {part.parent.name for part in part_files} == {
            'date=2026-07-14',
            'date=2026-07-15',
        }
        combined = pl.concat([pl.read_parquet(part) for part in part_files])
        assert combined.height == 3
        # Both driver arms and the -1.0 sentinel survive the whole
        # drive into storage.
        assert 'UnknownDriverId' in combined['driver__id'].to_list()
        assert -1.0 in combined['derived_volume'].to_list()
        assert result.cursor == FeedToken(from_version='00000000000016c3')
        first, second = result.sent_bodies
        first_params = first['params']
        assert isinstance(first_params, dict)
        assert first_params['search'] == {'fromDate': '2024-01-01T00:00:00Z'}
        assert 'fromVersion' not in first_params
        second_params = second['params']
        assert isinstance(second_params, dict)
        assert second_params['fromVersion'] == '00000000000016c2'
        assert 'search' not in second_params
