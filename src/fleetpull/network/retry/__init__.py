"""Retry policy: the pure decision the client consults after each retryable failure."""

from fleetpull.network.retry.decision import (
    RandomFractionGenerator,
    RetryDecision,
    decide_retry,
)

__all__: list[str] = [
    'RandomFractionGenerator',
    'RetryDecision',
    'decide_retry',
]
