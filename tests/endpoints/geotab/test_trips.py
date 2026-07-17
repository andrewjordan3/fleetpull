"""Tests for fleetpull.endpoints.geotab.trips.

The sort-beside-search assertion is the load-bearing positive here:
id-sort seek paging composed with a windowed ``search`` is supported
for ``Trip`` (live-verified 2026-07-13 — ExceptionEvent rejects the
same composition outright), and the first-page shape (``sortBy: id``,
ascending, an EXPLICIT null ``offset`` beside ``search``) is the probed
one the decoder advances from.
"""

from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.config import GeotabAuthConfig, GeotabConfig
from fleetpull.endpoints.geotab.trips import (
    _GeotabWindowedGetSpecBuilder,
    build_endpoint,
)
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    StorageKind,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.models.geotab import Trip
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import GeotabGetPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope


def _window() -> DateWindow:
    return DateWindow(
        start=datetime(2026, 7, 6, tzinfo=UTC),
        end=datetime(2026, 7, 13, tzinfo=UTC),
    )


def _build_endpoint() -> EndpointDefinition[Trip]:
    return build_endpoint(GeotabConfig())


class TestGeotabWindowedGetSpecBuilder:
    def test_builds_the_windowed_sorted_get(self) -> None:
        endpoint = _build_endpoint()
        spec = endpoint.spec_builder.build_spec(resume=_window(), path_values={})
        assert spec.method is HttpMethod.POST
        assert spec.url == 'https://my.geotab.com/apiv1'
        assert isinstance(spec.json_body, dict)
        assert spec.json_body['method'] == 'Get'
        params = spec.json_body['params']
        assert isinstance(params, dict)
        assert params['typeName'] == 'Trip'
        assert params['resultsLimit'] == 5000
        assert params['search'] == {
            'fromDate': '2026-07-06T00:00:00Z',
            'toDate': '2026-07-13T00:00:00Z',
        }
        # The probed shape: an EXPLICIT null offset, never an absent key.
        assert params['sort'] == {
            'sortBy': 'id',
            'sortDirection': 'asc',
            'offset': None,
        }

    def test_requires_a_date_window(self) -> None:
        builder = _GeotabWindowedGetSpecBuilder(
            server='my.geotab.com',
            type_name='Trip',
            results_limit=5000,
        )
        with pytest.raises(TypeError):
            builder.build_spec(resume=None, path_values={})

    def test_credentials_are_never_written_here(self) -> None:
        # The session strategy injects params.credentials; the builder
        # must leave the slot untouched.
        endpoint = _build_endpoint()
        spec = endpoint.spec_builder.build_spec(resume=_window(), path_values={})
        assert isinstance(spec.json_body, dict)
        params = spec.json_body['params']
        assert isinstance(params, dict)
        assert 'credentials' not in params

    def test_configured_auth_server_is_used(self) -> None:
        config = GeotabConfig(
            auth=GeotabAuthConfig(
                username='user@example.com',
                password='synthetic-password-123',
                database='synthetic_db',
                server='alt.example.test',
            )
        )
        spec = build_endpoint(config).spec_builder.build_spec(
            resume=_window(), path_values={}
        )
        assert spec.url == 'https://alt.example.test/apiv1'


class TestBuildTripsEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.GEOTAB
        assert endpoint.name == 'trips'
        assert endpoint.quota_scope is QuotaScope.GEOTAB_GET
        assert endpoint.storage_kind is StorageKind.DATE_PARTITIONED
        assert endpoint.response_model is Trip
        assert endpoint.event_time_column == 'stop'
        assert endpoint.fan_out is None
        assert endpoint.completeness_check is None
        assert endpoint.window_bisection is None

    def test_the_decoder_is_the_seek_walk_decoder(self) -> None:
        endpoint = _build_endpoint()
        assert isinstance(endpoint.page_decoder, GeotabGetPageDecoder)

    def test_watermark_knobs_come_from_config(self) -> None:
        endpoint = build_endpoint(GeotabConfig(lookback_days=2, cutoff_days=1))
        assert isinstance(endpoint.sync_mode, WatermarkMode)
        assert endpoint.sync_mode.lookback == timedelta(days=2)
        assert endpoint.sync_mode.cutoff == timedelta(days=1)
