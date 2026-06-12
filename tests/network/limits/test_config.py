"""Tests for fleetpull.network.limits.config."""

import pytest
from pydantic import ValidationError

from fleetpull.network.limits.config import RateLimitConfig


@pytest.fixture
def valid_config() -> RateLimitConfig:
    return RateLimitConfig(
        requests_per_period=100, period_seconds=60.0, burst=20, max_concurrency=5
    )


class TestFieldValidation:
    @pytest.mark.parametrize(
        ('field_name', 'invalid_value'),
        [
            ('requests_per_period', 0),
            ('requests_per_period', -1),
            ('period_seconds', 0.0),
            ('period_seconds', -10.0),
            ('burst', 0),
            ('burst', -5),
            ('max_concurrency', 0),
            ('max_concurrency', -2),
        ],
    )
    def test_rejects_out_of_range_values(
        self, field_name: str, invalid_value: float
    ) -> None:
        config_kwargs: dict[str, float] = {
            'requests_per_period': 100,
            'period_seconds': 60.0,
            'burst': 20,
            'max_concurrency': 5,
            field_name: invalid_value,
        }
        with pytest.raises(ValidationError):
            RateLimitConfig(**config_kwargs)

    def test_rejects_unknown_field(self) -> None:
        with pytest.raises(ValidationError):
            RateLimitConfig(
                requests_per_period=100,
                period_seconds=60.0,
                burst=20,
                max_concurrency=5,
                unknown_field=1,  # type: ignore[call-arg]
            )

    def test_is_frozen(self, valid_config: RateLimitConfig) -> None:
        with pytest.raises(ValidationError):
            valid_config.burst = 99  # type: ignore[misc]


class TestRefillRate:
    def test_refill_rate_arithmetic(self, valid_config: RateLimitConfig) -> None:
        assert valid_config.refill_rate_per_second == pytest.approx(100 / 60)

    def test_refill_rate_one_per_second(self) -> None:
        config = RateLimitConfig(
            requests_per_period=10, period_seconds=10.0, burst=1, max_concurrency=1
        )
        assert config.refill_rate_per_second == pytest.approx(1.0)
