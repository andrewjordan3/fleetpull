"""Tests for fleetpull.config.motive."""

import pytest
from pydantic import ValidationError

from fleetpull.config import MotiveConfig


class TestMotiveConfig:
    def test_defaults(self) -> None:
        config = MotiveConfig()
        assert config.base_url == 'https://api.gomotive.com'
        assert config.records_per_page == 100
        assert config.lookback_days == 7
        assert config.cutoff_days == 0

    def test_accepts_a_lookback_override(self) -> None:
        assert MotiveConfig(lookback_days=2).lookback_days == 2

    def test_rejects_a_negative_lookback(self) -> None:
        with pytest.raises(ValidationError):
            MotiveConfig(lookback_days=-1)

    def test_accepts_a_cutoff_override(self) -> None:
        assert MotiveConfig(cutoff_days=3).cutoff_days == 3

    def test_rejects_a_negative_cutoff(self) -> None:
        with pytest.raises(ValidationError):
            MotiveConfig(cutoff_days=-1)

    def test_accepts_an_override_base_url(self) -> None:
        assert MotiveConfig(base_url='https://motive.test').base_url == (
            'https://motive.test'
        )

    def test_strips_a_trailing_slash(self) -> None:
        assert MotiveConfig(base_url='https://motive.test/').base_url == (
            'https://motive.test'
        )

    def test_rejects_a_schemeless_base_url(self) -> None:
        with pytest.raises(ValidationError, match='http'):
            MotiveConfig(base_url='motive.test')

    def test_accepts_an_in_range_page_size(self) -> None:
        assert MotiveConfig(records_per_page=1).records_per_page == 1
        assert MotiveConfig(records_per_page=100).records_per_page == 100

    def test_rejects_a_page_size_below_one(self) -> None:
        with pytest.raises(ValidationError):
            MotiveConfig(records_per_page=0)

    def test_rejects_a_page_size_above_the_cap(self) -> None:
        with pytest.raises(ValidationError):
            MotiveConfig(records_per_page=101)

    def test_is_frozen(self) -> None:
        config = MotiveConfig()
        with pytest.raises(ValidationError):
            config.base_url = 'https://other.test'  # type: ignore[misc]

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(ValidationError):
            MotiveConfig(base_ur='https://typo.test')  # type: ignore[call-arg]

    def test_api_key_defaults_to_none(self) -> None:
        assert MotiveConfig().api_key is None

    def test_api_key_string_coerces_to_secret(self) -> None:
        config = MotiveConfig(api_key='synthetic-motive-key-000')
        assert config.api_key is not None
        assert config.api_key.get_secret_value() == 'synthetic-motive-key-000'

    def test_api_key_is_masked_in_reprs(self) -> None:
        config = MotiveConfig(api_key='synthetic-motive-key-000')
        assert 'synthetic-motive-key-000' not in repr(config)
        assert 'synthetic-motive-key-000' not in str(config)

    def test_endpoints_default_to_empty(self) -> None:
        assert MotiveConfig().endpoints == ()

    def test_endpoints_list_coerces_to_tuple(self) -> None:
        config = MotiveConfig(endpoints=['vehicles', 'vehicle_locations'])
        assert config.endpoints == ('vehicles', 'vehicle_locations')
