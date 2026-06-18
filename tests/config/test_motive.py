"""Tests for fleetpull.config.motive."""

import pytest
from pydantic import ValidationError

from fleetpull.config import MotiveConfig


class TestMotiveConfig:
    def test_defaults(self) -> None:
        config = MotiveConfig()
        assert config.base_url == 'https://api.gomotive.com'
        assert config.records_per_page == 100

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
