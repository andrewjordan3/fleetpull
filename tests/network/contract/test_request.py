"""Tests for fleetpull.network.contract.request."""

import dataclasses

import pytest

from fleetpull.network.contract.request import HttpMethod, RequestSpec


def build_spec() -> RequestSpec:
    return RequestSpec(
        method=HttpMethod.GET,
        url='https://api.example.com/v1/vehicles',
        headers={'Accept': 'application/json'},
        params={'page': '1'},
    )


class TestRequestSpec:
    def test_is_frozen(self) -> None:
        spec = build_spec()
        with pytest.raises(dataclasses.FrozenInstanceError):
            spec.url = 'https://other.example.com'  # type: ignore[misc]

    def test_has_slots_no_dict(self) -> None:
        assert not hasattr(build_spec(), '__dict__')

    def test_headers_default_to_empty(self) -> None:
        spec = RequestSpec(method=HttpMethod.POST, url='https://api.example.com')
        assert spec.headers == {}
        assert spec.params is None
        assert spec.json_body is None


class TestWithExtraHeaders:
    def test_merges_and_extra_wins_on_collision(self) -> None:
        spec = build_spec()
        merged_spec = spec.with_extra_headers(
            {'Authorization': 'Bearer synthetic', 'Accept': 'text/plain'}
        )
        assert merged_spec.headers == {
            'Accept': 'text/plain',
            'Authorization': 'Bearer synthetic',
        }

    def test_original_spec_unchanged(self) -> None:
        spec = build_spec()
        spec.with_extra_headers({'Authorization': 'Bearer synthetic'})
        assert spec.headers == {'Accept': 'application/json'}

    def test_other_fields_preserved(self) -> None:
        spec = build_spec()
        merged_spec = spec.with_extra_headers({'Authorization': 'Bearer synthetic'})
        assert merged_spec.method is spec.method
        assert merged_spec.url == spec.url
        assert merged_spec.params == spec.params
        assert merged_spec.json_body is spec.json_body


class TestWithMergedParams:
    def test_adds_replaces_and_keeps_unnamed(self) -> None:
        spec = build_spec()
        merged_spec = spec.with_merged_params({'page': '2', 'per_page': '100'})
        assert merged_spec.params == {'page': '2', 'per_page': '100'}

    def test_merges_into_none_params(self) -> None:
        spec = RequestSpec(method=HttpMethod.GET, url='https://api.example.com')
        merged_spec = spec.with_merged_params({'page': '1'})
        assert merged_spec.params == {'page': '1'}

    def test_original_spec_unchanged(self) -> None:
        spec = build_spec()
        spec.with_merged_params({'page': '2'})
        assert spec.params == {'page': '1'}

    def test_other_fields_preserved(self) -> None:
        spec = build_spec()
        merged_spec = spec.with_merged_params({'page': '2'})
        assert merged_spec.method is spec.method
        assert merged_spec.url == spec.url
        assert merged_spec.headers == spec.headers
        assert merged_spec.json_body is spec.json_body


class TestWithJsonBody:
    def test_replaces_body_wholesale(self) -> None:
        spec = RequestSpec(
            method=HttpMethod.POST,
            url='https://api.example.com',
            json_body={'method': 'Get', 'params': {'typeName': 'Device'}},
        )
        replaced_spec = spec.with_json_body({'method': 'GetSystemTimeUtc'})
        assert replaced_spec.json_body == {'method': 'GetSystemTimeUtc'}

    def test_original_spec_unchanged(self) -> None:
        spec = RequestSpec(
            method=HttpMethod.POST,
            url='https://api.example.com',
            json_body={'method': 'Get'},
        )
        spec.with_json_body({'method': 'GetSystemTimeUtc'})
        assert spec.json_body == {'method': 'Get'}

    def test_other_fields_preserved(self) -> None:
        spec = build_spec()
        replaced_spec = spec.with_json_body({'method': 'Get'})
        assert replaced_spec.method is spec.method
        assert replaced_spec.url == spec.url
        assert replaced_spec.headers == spec.headers
        assert replaced_spec.params == spec.params
