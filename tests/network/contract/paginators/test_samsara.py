"""Tests for fleetpull.network.contract.paginators.samsara.

Fixtures are synthetic, constructed in the verified envelope shape
with the API's real camelCase keys — the alias handling is part of
what is under test.
"""

import pytest

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract.pagination import PaginationStrategy
from fleetpull.network.contract.paginators.samsara import SamsaraPagination
from fleetpull.network.contract.request import HttpMethod, JsonValue, RequestSpec


def build_spec() -> RequestSpec:
    return RequestSpec(
        method=HttpMethod.GET,
        url='https://api.example.com/fleet/vehicles/stats',
        params={'types': 'gps'},
    )


class TestSamsaraPagination:
    def test_satisfies_pagination_strategy_protocol(self) -> None:
        strategy: PaginationStrategy = SamsaraPagination()
        assert isinstance(strategy, SamsaraPagination)

    def test_first_request_is_identity_without_after(self) -> None:
        spec = build_spec()
        prepared = SamsaraPagination().first_request(spec)
        assert prepared is spec
        assert prepared.params is not None
        assert 'after' not in prepared.params

    def test_cursor_advance_merges_after(self) -> None:
        envelope: dict[str, JsonValue] = {
            'pagination': {'hasNextPage': True, 'endCursor': 'cursor-0001'},
            'data': [],
        }
        verdict = SamsaraPagination().advance(build_spec(), envelope)
        assert verdict.next_spec is not None
        assert verdict.next_spec.params == {'types': 'gps', 'after': 'cursor-0001'}
        assert verdict.durable_progress is None

    def test_has_next_page_false_completes(self) -> None:
        # endCursor absent on the terminal page: the default-None field
        # is exercised alongside termination.
        envelope: dict[str, JsonValue] = {
            'pagination': {'hasNextPage': False},
            'data': [],
        }
        verdict = SamsaraPagination().advance(build_spec(), envelope)
        assert verdict.next_spec is None
        assert verdict.durable_progress is None

    def test_continuation_without_cursor_raises_truncation_guard(self) -> None:
        envelope: dict[str, JsonValue] = {
            'pagination': {'hasNextPage': True},
            'data': [],
        }
        with pytest.raises(ProviderResponseError, match='endCursor'):
            SamsaraPagination().advance(build_spec(), envelope)

    def test_continuation_with_empty_cursor_raises_truncation_guard(self) -> None:
        envelope: dict[str, JsonValue] = {
            'pagination': {'hasNextPage': True, 'endCursor': ''},
            'data': [],
        }
        with pytest.raises(ProviderResponseError, match='endCursor'):
            SamsaraPagination().advance(build_spec(), envelope)

    @pytest.mark.parametrize(
        'envelope',
        [
            {'data': []},  # pagination block missing entirely
            {'pagination': {'endCursor': 'cursor-0001'}, 'data': []},  # no flag
            # Mistyped flag. Deliberately NOT 'yes'/'true': Pydantic's
            # lax mode coerces bool-ish strings, so only a genuinely
            # uncoercible value exercises the rejection.
            {'pagination': {'hasNextPage': 'maybe'}, 'data': []},
        ],
    )
    def test_malformed_metadata_raises(self, envelope: JsonValue) -> None:
        with pytest.raises(ProviderResponseError, match='malformed pagination'):
            SamsaraPagination().advance(build_spec(), envelope)
