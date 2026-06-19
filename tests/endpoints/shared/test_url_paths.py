# tests/endpoints/shared/test_url_paths.py
"""Tests for fleetpull.endpoints.shared.url_paths."""

import pytest

from fleetpull.endpoints.shared.url_paths import (
    UrlPathTemplateError,
    render_url_path_template,
)


class TestRenderUrlPathTemplate:
    def test_fills_a_single_placeholder(self) -> None:
        result = render_url_path_template(
            '/v3/vehicle_locations/{vehicle_id}', {'vehicle_id': '123'}
        )
        assert result == '/v3/vehicle_locations/123'

    def test_fills_multiple_placeholders(self) -> None:
        result = render_url_path_template(
            '/v1/{company_id}/vehicles/{vehicle_id}',
            {'company_id': 'acme', 'vehicle_id': '7'},
        )
        assert result == '/v1/acme/vehicles/7'

    def test_no_placeholders_with_empty_mapping_passes_through(self) -> None:
        assert render_url_path_template('/v1/vehicles', {}) == '/v1/vehicles'

    def test_url_encodes_a_value_as_a_single_segment(self) -> None:
        result = render_url_path_template(
            '/v1/vehicles/{vehicle_id}/locations', {'vehicle_id': 'abc/123'}
        )
        assert result == '/v1/vehicles/abc%2F123/locations'

    def test_missing_placeholder_value_raises(self) -> None:
        with pytest.raises(UrlPathTemplateError, match='Missing'):
            render_url_path_template('/v1/{vehicle_id}', {})

    def test_unused_value_raises(self) -> None:
        with pytest.raises(UrlPathTemplateError, match='Unused'):
            render_url_path_template(
                '/v1/{vehicle_id}', {'vehicle_id': '1', 'driver_id': '2'}
            )

    def test_empty_value_raises(self) -> None:
        with pytest.raises(UrlPathTemplateError, match='empty'):
            render_url_path_template('/v1/{vehicle_id}', {'vehicle_id': ''})

    def test_dangling_brace_raises(self) -> None:
        with pytest.raises(UrlPathTemplateError, match='brace'):
            render_url_path_template('/v1/{vehicle_id', {'vehicle_id': '1'})

    def test_unsupported_placeholder_name_raises(self) -> None:
        with pytest.raises(UrlPathTemplateError, match='brace'):
            render_url_path_template('/v1/{vehicle-id}', {'vehicle-id': '1'})

    def test_repeated_placeholder_uses_one_value(self) -> None:
        result = render_url_path_template(
            '/v1/{vehicle_id}/x/{vehicle_id}', {'vehicle_id': '9'}
        )
        assert result == '/v1/9/x/9'
