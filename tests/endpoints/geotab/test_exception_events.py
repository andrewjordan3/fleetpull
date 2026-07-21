"""Tests for fleetpull.endpoints.geotab.exception_events.

The no-sort assertion is the load-bearing negative: id-sort is rejected
outright for this type, and any sort composed with a search degrades to
the deterministic ``-32000 GenericException`` (captured 2026-07-15) —
a ``sort`` member ever appearing in this builder's body is the
regression that crashes every request.
"""

from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.config import GeotabAuthConfig, GeotabConfig
from fleetpull.endpoints.geotab._requests import GeotabWindowedGetSpecBuilder
from fleetpull.endpoints.geotab.exception_events import build_endpoint
from fleetpull.endpoints.shared import (
    BisectedWindowFetch,
    EndpointDefinition,
    StorageKind,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.models.geotab import ExceptionEvent
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import SinglePageDecoder
from fleetpull.vocabulary import Provider, QuotaScope


def _window() -> DateWindow:
    return DateWindow(
        start=datetime(2026, 7, 6, tzinfo=UTC),
        end=datetime(2026, 7, 13, tzinfo=UTC),
    )


def _build_endpoint() -> EndpointDefinition[ExceptionEvent]:
    return build_endpoint(GeotabConfig())


class TestExceptionEventsSpecBuilder:
    def test_composes_the_shared_builder_unsorted(self) -> None:
        # id_sort=False is the per-type declared capability: id-sort is
        # rejected outright for ExceptionEvent, never assumed sortable.
        endpoint = _build_endpoint()
        assert isinstance(endpoint.spec_builder, GeotabWindowedGetSpecBuilder)
        assert endpoint.spec_builder.id_sort is False

    def test_builds_the_windowed_unsorted_get(self) -> None:
        endpoint = _build_endpoint()
        spec = endpoint.spec_builder.build_spec(resume=_window(), member_values={})
        assert spec.method is HttpMethod.POST
        assert spec.url == 'https://my.geotab.com/apiv1'
        assert isinstance(spec.json_body, dict)
        assert spec.json_body['method'] == 'Get'
        params = spec.json_body['params']
        assert isinstance(params, dict)
        assert params['typeName'] == 'ExceptionEvent'
        assert params['resultsLimit'] == 5000
        assert params['search'] == {
            'fromDate': '2026-07-06T00:00:00Z',
            'toDate': '2026-07-13T00:00:00Z',
        }

    def test_never_writes_a_sort_member(self) -> None:
        endpoint = _build_endpoint()
        spec = endpoint.spec_builder.build_spec(resume=_window(), member_values={})
        assert isinstance(spec.json_body, dict)
        params = spec.json_body['params']
        assert isinstance(params, dict)
        assert 'sort' not in params

    def test_requires_a_date_window(self) -> None:
        builder = GeotabWindowedGetSpecBuilder(
            server='my.geotab.com',
            type_name='ExceptionEvent',
            results_limit=5000,
            id_sort=False,
        )
        with pytest.raises(TypeError):
            builder.build_spec(resume=None, member_values={})

    def test_credentials_are_never_written_here(self) -> None:
        # The session strategy injects params.credentials; the builder
        # must leave the slot untouched.
        endpoint = _build_endpoint()
        spec = endpoint.spec_builder.build_spec(resume=_window(), member_values={})
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
            resume=_window(), member_values={}
        )
        assert spec.url == 'https://alt.example.test/apiv1'


class TestBuildExceptionEventsEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.GEOTAB
        assert endpoint.name == 'exception_events'
        assert endpoint.quota_scope is QuotaScope.GEOTAB_GET
        assert endpoint.storage_kind is StorageKind.DATE_PARTITIONED
        assert endpoint.response_model is ExceptionEvent
        assert endpoint.event_time_column == 'active_from'
        assert endpoint.completeness_check is None

    def test_declares_the_bisected_window_shape(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.request_shape == BisectedWindowFetch(
            results_limit=5000,
            floor=timedelta(minutes=1),
            event_time_wire_key='activeFrom',
        )

    def test_the_decoder_is_single_page_over_the_result_key(self) -> None:
        endpoint = _build_endpoint()
        decoder = endpoint.page_decoder
        assert isinstance(decoder, SinglePageDecoder)
        assert decoder.records_key == 'result'

    def test_watermark_knobs_come_from_config(self) -> None:
        endpoint = build_endpoint(GeotabConfig(lookback_days=2, cutoff_days=1))
        assert isinstance(endpoint.sync_mode, WatermarkMode)
        assert endpoint.sync_mode.lookback == timedelta(days=2)
        assert endpoint.sync_mode.cutoff == timedelta(days=1)
