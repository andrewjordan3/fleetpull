# src/fleetpull/config/retry.py
"""Retry policy configuration: attempt budgets and backoff shape.

Vocabulary (shared with ``network/retry/decision.py``): a *failure
count* is one-based within the current retryable category — the first
TRANSIENT failure of an attempt sequence is 1. ``*_max_failures = N``
means failures 1..N are each answered with a retry and the (N+1)th
exhausts the budget: at most N + 1 requests. Counters are independent
per category within an attempt sequence; a RATE_LIMITED failure
neither resets nor advances the TRANSIENT count.
"""

import logging
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

__all__: list[str] = ['RetryConfig']

logger = logging.getLogger(__name__)


class RetryConfig(BaseModel):
    """
    User-facing retry policy, one instance per run.

    Attributes:
        transient_max_failures: Highest TRANSIENT failure count still
            retried. 0 means TRANSIENT failures are never retried.
        transient_backoff_base_seconds: Full-jitter base; the delay
            envelope for failure ``n`` is
            ``min(cap, base * 2 ** (n - 1))``.
        transient_backoff_cap_seconds: Ceiling on the delay envelope.
            Only reachable when an operator raises the failure budget.
        rate_limited_max_failures: Highest RATE_LIMITED failure count
            still retried. A circuit breaker against 429 storms, not a
            pacer — pacing is the limiter's job, and RATE_LIMITED
            retries never sleep locally.
        fallback_penalty_seconds: Quota-scope penalty the client
            applies when a rate-limited response carries no usable
            Retry-After. Fires only when a provider is already
            misbehaving, so it errs long; the penalty log line carries
            the raw header value to keep the case diagnosable.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', validate_default=True)

    transient_max_failures: int = Field(default=3, ge=0)
    transient_backoff_base_seconds: float = Field(default=1.0, gt=0)
    transient_backoff_cap_seconds: float = Field(default=30.0, gt=0)
    rate_limited_max_failures: int = Field(default=10, ge=0)
    fallback_penalty_seconds: float = Field(default=60.0, gt=0)

    @model_validator(mode='after')
    def _cap_not_below_base(self) -> Self:
        """
        Reject a cap below the base: the envelope would be constant at
        the cap from the first failure, which is never what a config
        author meant.

        Returns:
            The validated model.

        Raises:
            ValueError: When the cap is below the base.
        """
        if self.transient_backoff_cap_seconds < self.transient_backoff_base_seconds:
            raise ValueError(
                'transient_backoff_cap_seconds must be >= '
                'transient_backoff_base_seconds'
            )
        return self
