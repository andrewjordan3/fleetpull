"""Tests for fleetpull.endpoints.shared.spec_builders."""

from fleetpull.endpoints.shared import SpecBuilder, StaticGetSpecBuilder
from fleetpull.network.contract import HttpMethod


def build_builder() -> StaticGetSpecBuilder:
    return StaticGetSpecBuilder(
        base_url='https://api.example.test', path='/v1/vehicles'
    )


class TestStaticGetSpecBuilder:
    def test_satisfies_spec_builder_protocol(self) -> None:
        builder: SpecBuilder = build_builder()
        assert isinstance(builder, StaticGetSpecBuilder)

    def test_builds_a_get_for_base_url_plus_path(self) -> None:
        spec = build_builder().build_spec(resume=None, path_values={})
        assert spec.method is HttpMethod.GET
        assert spec.url == 'https://api.example.test/v1/vehicles'

    def test_carries_no_credentials_or_pagination(self) -> None:
        spec = build_builder().build_spec(resume=None, path_values={})
        assert spec.headers == {}
        assert spec.params is None
        assert spec.json_body is None

    def test_ignores_resume_and_path_values(self) -> None:
        builder = build_builder()
        baseline = builder.build_spec(resume=None, path_values={})
        ignored = builder.build_spec(resume=None, path_values={'id': '999'})
        assert ignored.url == baseline.url
