"""Tests for fleetpull.network.contract.paginators.single_page."""

from fleetpull.network.contract.pagination import PageAdvance, PaginationStrategy
from fleetpull.network.contract.paginators.single_page import SinglePageStrategy
from fleetpull.network.contract.request import HttpMethod, JsonValue, RequestSpec


def build_spec() -> RequestSpec:
    return RequestSpec(
        method=HttpMethod.GET,
        url='https://api.example.com/v3/vehicle_locations',
        params={'date': '2026-06-01'},
    )


class TestSinglePageStrategy:
    def test_satisfies_pagination_strategy_protocol(self) -> None:
        strategy: PaginationStrategy = SinglePageStrategy()
        assert isinstance(strategy, SinglePageStrategy)

    def test_first_request_is_identity(self) -> None:
        spec = build_spec()
        assert SinglePageStrategy().first_request(spec) is spec

    def test_advance_completes_regardless_of_envelope_content(self) -> None:
        spec = build_spec()
        envelopes: tuple[JsonValue, ...] = ({'vehicles': []}, 'not even a dict', None)
        for envelope in envelopes:
            verdict = SinglePageStrategy().advance(spec, envelope)
            assert verdict == PageAdvance(next_spec=None, durable_progress=None)
