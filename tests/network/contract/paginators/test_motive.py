"""Tests for fleetpull.network.contract.paginators.motive.

Fixtures are synthetic, constructed in the verified envelope shape:
a top-level ``pagination`` echo beside the records key.
"""

import pytest

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract.pagination import PaginationStrategy
from fleetpull.network.contract.paginators.motive import MotivePagination
from fleetpull.network.contract.request import HttpMethod, JsonValue, RequestSpec


def build_spec() -> RequestSpec:
    return RequestSpec(
        method=HttpMethod.GET,
        url='https://api.example.com/v1/vehicles',
        params={'lookback': '7'},
    )


def build_envelope(page_no: int, per_page: int, total: int) -> dict[str, JsonValue]:
    return {
        'pagination': {'page_no': page_no, 'per_page': per_page, 'total': total},
        'vehicles': [],
    }


class TestMotivePagination:
    def test_satisfies_pagination_strategy_protocol(self) -> None:
        strategy: PaginationStrategy = MotivePagination(per_page=100)
        assert isinstance(strategy, MotivePagination)

    def test_first_request_merges_page_parameters(self) -> None:
        prepared = MotivePagination(per_page=100).first_request(build_spec())
        assert prepared.params == {
            'lookback': '7',
            'page_no': '1',
            'per_page': '100',
        }

    def test_advance_to_page_two_from_mid_feed_echo(self) -> None:
        verdict = MotivePagination(per_page=100).advance(
            build_spec(), build_envelope(page_no=1, per_page=100, total=250)
        )
        assert verdict.next_spec is not None
        assert verdict.next_spec.params == {
            'lookback': '7',
            'page_no': '2',
            'per_page': '100',
        }
        assert verdict.durable_progress is None

    def test_terminates_at_exact_boundary(self) -> None:
        # page_no * per_page >= total: 3 * 100 >= 250.
        verdict = MotivePagination(per_page=100).advance(
            build_spec(), build_envelope(page_no=3, per_page=100, total=250)
        )
        assert verdict.next_spec is None
        assert verdict.durable_progress is None

    def test_terminates_at_equality(self) -> None:
        verdict = MotivePagination(per_page=100).advance(
            build_spec(), build_envelope(page_no=2, per_page=100, total=200)
        )
        assert verdict.next_spec is None

    def test_single_page_total_completes_immediately(self) -> None:
        verdict = MotivePagination(per_page=100).advance(
            build_spec(), build_envelope(page_no=1, per_page=100, total=40)
        )
        assert verdict.next_spec is None

    def test_next_params_echo_the_response_values_not_the_field(self) -> None:
        # The drift case: the server clamped per_page to 50 and total
        # moved; the next request must trust the fresh echo.
        verdict = MotivePagination(per_page=100).advance(
            build_spec(), build_envelope(page_no=2, per_page=50, total=500)
        )
        assert verdict.next_spec is not None
        assert verdict.next_spec.params == {
            'lookback': '7',
            'page_no': '3',
            'per_page': '50',
        }

    @pytest.mark.parametrize(
        ('envelope', 'offending_field'),
        [
            ({'vehicles': []}, 'pagination'),
            ({'pagination': 'nope', 'vehicles': []}, 'pagination'),
            (
                {
                    'pagination': {'page_no': 1, 'per_page': 100, 'total': 'many'},
                    'vehicles': [],
                },
                'total',
            ),
        ],
    )
    def test_malformed_echo_raises_naming_the_offending_field(
        self, envelope: JsonValue, offending_field: str
    ) -> None:
        with pytest.raises(ProviderResponseError) as exception_info:
            MotivePagination(per_page=100).advance(build_spec(), envelope)
        assert offending_field in str(exception_info.value)
