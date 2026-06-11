"""Tests for fleetpull.network.limits.bucket_math."""

import pytest

from fleetpull.network.limits.bucket_math import refill_tokens, seconds_until_available

__all__: list[str] = []


class TestRefillTokens:
    def test_refill_from_empty_over_known_elapsed_time(self) -> None:
        refilled_tokens: float = refill_tokens(
            current_tokens=0.0,
            elapsed_seconds=3.0,
            refill_rate_per_second=2.0,
            capacity=100.0,
        )
        assert refilled_tokens == pytest.approx(6.0)

    def test_refill_caps_at_capacity(self) -> None:
        refilled_tokens: float = refill_tokens(
            current_tokens=5.0,
            elapsed_seconds=1_000_000.0,
            refill_rate_per_second=10.0,
            capacity=20.0,
        )
        assert refilled_tokens == pytest.approx(20.0)

    def test_zero_elapsed_is_a_no_op(self) -> None:
        refilled_tokens: float = refill_tokens(
            current_tokens=7.5,
            elapsed_seconds=0.0,
            refill_rate_per_second=10.0,
            capacity=20.0,
        )
        assert refilled_tokens == pytest.approx(7.5)

    def test_negative_elapsed_raises(self) -> None:
        with pytest.raises(ValueError, match='non-negative'):
            refill_tokens(
                current_tokens=0.0,
                elapsed_seconds=-0.001,
                refill_rate_per_second=10.0,
                capacity=20.0,
            )


class TestSecondsUntilAvailable:
    def test_zero_when_tokens_suffice(self) -> None:
        wait_seconds: float = seconds_until_available(
            current_tokens=1.0, refill_rate_per_second=0.5
        )
        assert wait_seconds == 0.0

    def test_zero_when_tokens_exceed_needed(self) -> None:
        wait_seconds: float = seconds_until_available(
            current_tokens=5.0, refill_rate_per_second=0.5, tokens_needed=2.0
        )
        assert wait_seconds == 0.0

    def test_exact_deficit_math(self) -> None:
        # 0.25 tokens at 0.5 tokens/sec: 0.75 deficit / 0.5 = 1.5 seconds.
        wait_seconds: float = seconds_until_available(
            current_tokens=0.25, refill_rate_per_second=0.5
        )
        assert wait_seconds == pytest.approx(1.5)

    def test_exact_deficit_math_with_custom_tokens_needed(self) -> None:
        wait_seconds: float = seconds_until_available(
            current_tokens=1.0, refill_rate_per_second=2.0, tokens_needed=4.0
        )
        assert wait_seconds == pytest.approx(1.5)
