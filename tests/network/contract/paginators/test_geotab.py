"""Tests for fleetpull.network.contract.paginators.geotab.

Fixtures are synthetic, constructed in the verified GetFeed shapes:
zeroed-pattern version strings, scrubbed ids, real camelCase keys.
"""

import pytest

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract.pagination import PaginationStrategy
from fleetpull.network.contract.paginators.geotab import GeotabFeedPagination
from fleetpull.network.contract.request import HttpMethod, JsonValue, RequestSpec

BOOTSTRAP_SEARCH: dict[str, JsonValue] = {'fromDate': '2026-06-01T00:00:00Z'}


def build_feed_spec(results_limit: int = 2) -> RequestSpec:
    return RequestSpec(
        method=HttpMethod.POST,
        url='https://resolved.example.geotab.com/apiv1',
        json_body={
            'method': 'GetFeed',
            'params': {
                'typeName': 'LogRecord',
                'resultsLimit': results_limit,
                'search': BOOTSTRAP_SEARCH,
            },
        },
    )


def build_envelope(record_count: int, to_version: str) -> dict[str, JsonValue]:
    records: list[JsonValue] = [
        {'id': f'synthetic-{record_index}'} for record_index in range(record_count)
    ]
    return {
        'result': {'data': records, 'toVersion': to_version},
        'jsonrpc': '2.0',
    }


class TestGeotabFeedPagination:
    def test_satisfies_pagination_strategy_protocol(self) -> None:
        strategy: PaginationStrategy = GeotabFeedPagination()
        assert isinstance(strategy, GeotabFeedPagination)

    def test_first_request_is_identity(self) -> None:
        spec = build_feed_spec()
        assert GeotabFeedPagination().first_request(spec) is spec

    def test_advance_strips_search_and_sets_from_version(self) -> None:
        verdict = GeotabFeedPagination().advance(
            build_feed_spec(results_limit=2),
            build_envelope(record_count=2, to_version='0000000000000001'),
        )
        assert verdict.next_spec is not None
        assert verdict.next_spec.json_body is not None
        next_params = verdict.next_spec.json_body['params']
        assert isinstance(next_params, dict)
        # Both properties of the rewrite, asserted explicitly:
        assert 'search' not in next_params
        assert next_params['fromVersion'] == '0000000000000001'
        # Untouched params survive the rewrite.
        assert next_params['typeName'] == 'LogRecord'
        assert next_params['resultsLimit'] == 2

    def test_full_page_continues(self) -> None:
        verdict = GeotabFeedPagination().advance(
            build_feed_spec(results_limit=2),
            build_envelope(record_count=2, to_version='0000000000000001'),
        )
        assert verdict.next_spec is not None
        assert verdict.durable_progress == '0000000000000001'

    def test_short_page_completes_with_durable_progress(self) -> None:
        verdict = GeotabFeedPagination().advance(
            build_feed_spec(results_limit=2),
            build_envelope(record_count=1, to_version='0000000000000002'),
        )
        assert verdict.next_spec is None
        assert verdict.durable_progress == '0000000000000002'

    def test_empty_page_completes_with_the_envelope_to_version(self) -> None:
        verdict = GeotabFeedPagination().advance(
            build_feed_spec(results_limit=2),
            build_envelope(record_count=0, to_version='0000000000000003'),
        )
        assert verdict.next_spec is None
        assert verdict.durable_progress == '0000000000000003'

    def test_every_page_carries_durable_progress(self) -> None:
        strategy = GeotabFeedPagination()
        continuing = strategy.advance(
            build_feed_spec(results_limit=2),
            build_envelope(record_count=2, to_version='0000000000000004'),
        )
        terminal = strategy.advance(
            build_feed_spec(results_limit=2),
            build_envelope(record_count=0, to_version='0000000000000005'),
        )
        assert continuing.durable_progress == '0000000000000004'
        assert terminal.durable_progress == '0000000000000005'

    @pytest.mark.parametrize(
        'envelope',
        [
            {'result': {'data': []}, 'jsonrpc': '2.0'},  # toVersion missing
            {
                'result': {'data': 'not a list', 'toVersion': '0000000000000006'},
                'jsonrpc': '2.0',
            },
            {'jsonrpc': '2.0'},  # result missing entirely
        ],
    )
    def test_malformed_envelope_raises(self, envelope: JsonValue) -> None:
        with pytest.raises(ProviderResponseError, match='malformed response envelope'):
            GeotabFeedPagination().advance(build_feed_spec(), envelope)

    @pytest.mark.parametrize(
        'malformed_spec',
        [
            RequestSpec(method=HttpMethod.POST, url='https://x.example/apiv1'),
            RequestSpec(
                method=HttpMethod.POST,
                url='https://x.example/apiv1',
                json_body={'method': 'GetFeed'},
            ),
            RequestSpec(
                method=HttpMethod.POST,
                url='https://x.example/apiv1',
                json_body={'method': 'GetFeed', 'params': {'typeName': 'LogRecord'}},
            ),
            RequestSpec(
                method=HttpMethod.POST,
                url='https://x.example/apiv1',
                json_body={
                    'method': 'GetFeed',
                    'params': {'typeName': 'LogRecord', 'resultsLimit': 'many'},
                },
            ),
        ],
    )
    def test_malformed_sent_body_raises_value_error(
        self, malformed_spec: RequestSpec
    ) -> None:
        envelope = build_envelope(record_count=0, to_version='0000000000000007')
        with pytest.raises(ValueError, match='GeoTab feed request'):
            GeotabFeedPagination().advance(malformed_spec, envelope)
