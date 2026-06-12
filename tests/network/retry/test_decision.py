"""Tests for fleetpull.network.retry.decision.

All randomness flows through a test-local stub feeding preset
fractions, so every delay assertion is exact arithmetic; no test
sleeps real time.
"""

import dataclasses
import random

import pytest

from fleetpull.config.retry import RetryConfig
from fleetpull.network.contract.outcome import ResponseCategory
from fleetpull.network.retry.decision import RetryDecision, decide_retry

# Closest float below 1.0 the stub can feed: pins the half-open upper
# bound of the delay interval.
NEAR_ONE_FRACTION: float = 1.0 - 1e-12


class FixedFractionGenerator:
    """Feeds a preset fraction and records how often it is consulted."""

    def __init__(self, fraction: float) -> None:
        self.fraction = fraction
        self.call_count = 0

    def random(self) -> float:
        self.call_count += 1
        return self.fraction


def build_config(
    *,
    transient_max_failures: int = 5,
    base_seconds: float = 1.0,
    cap_seconds: float = 4.0,
    rate_limited_max_failures: int = 10,
) -> RetryConfig:
    # cap_seconds=4.0 is reachable at failure 3 (envelopes 1, 2, 4),
    # so the clamp is exercised within the default budget.
    return RetryConfig(
        transient_max_failures=transient_max_failures,
        transient_backoff_base_seconds=base_seconds,
        transient_backoff_cap_seconds=cap_seconds,
        rate_limited_max_failures=rate_limited_max_failures,
    )


class TestTransientJitter:
    @pytest.mark.parametrize('fraction', [0.0, 0.5, NEAR_ONE_FRACTION])
    @pytest.mark.parametrize(
        ('failure_count', 'expected_envelope'),
        [(1, 1.0), (2, 2.0), (3, 4.0), (4, 4.0), (5, 4.0)],
    )
    def test_delay_is_fraction_of_clamped_envelope(
        self, failure_count: int, expected_envelope: float, fraction: float
    ) -> None:
        decision = decide_retry(
            ResponseCategory.TRANSIENT,
            failure_count,
            build_config(),
            FixedFractionGenerator(fraction),
        )
        assert decision.should_retry is True
        assert decision.local_delay_seconds == pytest.approx(
            fraction * expected_envelope
        )

    def test_delay_never_reaches_the_envelope(self) -> None:
        # The fraction source is [0.0, 1.0): the interval is half-open.
        decision = decide_retry(
            ResponseCategory.TRANSIENT,
            1,
            build_config(),
            FixedFractionGenerator(NEAR_ONE_FRACTION),
        )
        assert decision.local_delay_seconds < 1.0


class TestProtocolSatisfaction:
    def test_stdlib_random_satisfies_the_seam_end_to_end(self) -> None:
        # random.Random satisfies RandomFractionGenerator structurally;
        # only the bounds are assertable with real randomness.
        decision = decide_retry(
            ResponseCategory.TRANSIENT, 1, build_config(), random.Random()
        )
        assert decision.should_retry is True
        assert 0.0 <= decision.local_delay_seconds < 1.0


class TestExhaustionBoundary:
    def test_transient_at_budget_retries(self) -> None:
        config = build_config(transient_max_failures=3)
        decision = decide_retry(
            ResponseCategory.TRANSIENT, 3, config, FixedFractionGenerator(0.5)
        )
        assert decision.should_retry is True

    def test_transient_past_budget_refuses_with_inert_delay(self) -> None:
        config = build_config(transient_max_failures=3)
        decision = decide_retry(
            ResponseCategory.TRANSIENT, 4, config, FixedFractionGenerator(0.5)
        )
        assert decision == RetryDecision(should_retry=False, local_delay_seconds=0.0)

    def test_zero_transient_budget_refuses_the_first_failure(self) -> None:
        config = build_config(transient_max_failures=0)
        decision = decide_retry(
            ResponseCategory.TRANSIENT, 1, config, FixedFractionGenerator(0.5)
        )
        assert decision == RetryDecision(should_retry=False, local_delay_seconds=0.0)

    def test_rate_limited_at_budget_retries(self) -> None:
        config = build_config(rate_limited_max_failures=2)
        decision = decide_retry(
            ResponseCategory.RATE_LIMITED, 2, config, FixedFractionGenerator(0.5)
        )
        assert decision.should_retry is True

    def test_rate_limited_past_budget_refuses_with_inert_delay(self) -> None:
        config = build_config(rate_limited_max_failures=2)
        decision = decide_retry(
            ResponseCategory.RATE_LIMITED, 3, config, FixedFractionGenerator(0.5)
        )
        assert decision == RetryDecision(should_retry=False, local_delay_seconds=0.0)

    def test_zero_rate_limited_budget_refuses_the_first_failure(self) -> None:
        config = build_config(rate_limited_max_failures=0)
        decision = decide_retry(
            ResponseCategory.RATE_LIMITED, 1, config, FixedFractionGenerator(0.5)
        )
        assert decision == RetryDecision(should_retry=False, local_delay_seconds=0.0)


class TestRateLimitedWithinBudget:
    def test_no_local_delay_and_no_jitter_consultation(self) -> None:
        config = build_config(rate_limited_max_failures=4)
        recording_generator = FixedFractionGenerator(0.5)
        for failure_count in range(1, 5):
            decision = decide_retry(
                ResponseCategory.RATE_LIMITED,
                failure_count,
                config,
                recording_generator,
            )
            assert decision == RetryDecision(should_retry=True, local_delay_seconds=0.0)
        # The penalized limiter is this category's backoff; jitter is
        # never consulted.
        assert recording_generator.call_count == 0


class TestProgrammingErrors:
    @pytest.mark.parametrize(
        'category',
        [
            ResponseCategory.SUCCESS,
            ResponseCategory.FATAL,
            ResponseCategory.AUTH_FAILURE,
        ],
    )
    def test_non_retryable_category_raises(self, category: ResponseCategory) -> None:
        with pytest.raises(ValueError, match='not retryable'):
            decide_retry(category, 1, build_config(), FixedFractionGenerator(0.5))

    @pytest.mark.parametrize('bad_failure_count', [0, -1])
    def test_failure_count_below_one_raises(self, bad_failure_count: int) -> None:
        with pytest.raises(ValueError, match='one-based'):
            decide_retry(
                ResponseCategory.TRANSIENT,
                bad_failure_count,
                build_config(),
                FixedFractionGenerator(0.5),
            )


class TestRetryDecision:
    def test_is_frozen(self) -> None:
        decision = RetryDecision(should_retry=True, local_delay_seconds=1.0)
        with pytest.raises(dataclasses.FrozenInstanceError):
            decision.should_retry = False  # type: ignore[misc]

    def test_has_slots_no_dict(self) -> None:
        decision = RetryDecision(should_retry=False, local_delay_seconds=0.0)
        assert not hasattr(decision, '__dict__')
