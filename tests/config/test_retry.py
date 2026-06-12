"""Tests for fleetpull.config.retry."""

import pytest
from pydantic import ValidationError

from fleetpull.config.retry import RetryConfig


class TestDefaults:
    def test_bare_config_defaults(self) -> None:
        config = RetryConfig()
        assert config.transient_max_failures == 3
        assert config.transient_backoff_base_seconds == 1.0
        assert config.transient_backoff_cap_seconds == 30.0
        assert config.rate_limited_max_failures == 10
        assert config.fallback_penalty_seconds == 60.0


class TestFieldValidation:
    @pytest.mark.parametrize(
        'budget_field',
        ['transient_max_failures', 'rate_limited_max_failures'],
    )
    def test_zero_budgets_accepted(self, budget_field: str) -> None:
        config = RetryConfig(**{budget_field: 0})
        assert getattr(config, budget_field) == 0

    @pytest.mark.parametrize(
        ('field_name', 'invalid_value'),
        [
            ('transient_max_failures', -1),
            ('rate_limited_max_failures', -1),
            ('transient_backoff_base_seconds', 0.0),
            ('transient_backoff_base_seconds', -1.0),
            ('transient_backoff_cap_seconds', 0.0),
            ('transient_backoff_cap_seconds', -30.0),
            ('fallback_penalty_seconds', 0.0),
            ('fallback_penalty_seconds', -5.0),
        ],
    )
    def test_rejects_out_of_range_values(
        self, field_name: str, invalid_value: float
    ) -> None:
        with pytest.raises(ValidationError):
            RetryConfig(**{field_name: invalid_value})

    def test_cap_below_base_rejected(self) -> None:
        with pytest.raises(
            ValidationError,
            match='transient_backoff_cap_seconds must be >=',
        ):
            RetryConfig(
                transient_backoff_base_seconds=10.0,
                transient_backoff_cap_seconds=5.0,
            )

    def test_cap_equal_to_base_accepted(self) -> None:
        config = RetryConfig(
            transient_backoff_base_seconds=2.0,
            transient_backoff_cap_seconds=2.0,
        )
        assert config.transient_backoff_cap_seconds == 2.0

    def test_is_frozen(self) -> None:
        config = RetryConfig()
        with pytest.raises(ValidationError):
            config.transient_max_failures = 99  # type: ignore[misc]

    def test_unknown_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            RetryConfig(max_retries=3)  # type: ignore[call-arg]
