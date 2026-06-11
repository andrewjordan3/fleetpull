# src/fleetpull/network/limits/config.py
"""Rate-limit configuration for a single quota scope.

One ``RateLimitConfig`` describes the request budget for one quota scope
(default: one provider). The registry maps quota-scope strings to these
configs; the limiter reads its refill rate and capacity from here.
"""

from pydantic import BaseModel, ConfigDict, Field

__all__: list[str] = ['RateLimitConfig']


class RateLimitConfig(BaseModel):
    """Token-bucket and concurrency settings for one quota scope.

    ``burst`` is the bucket CAPACITY: the maximum number of tokens the
    bucket holds, and therefore the number of requests a cold-started
    limiter can fire immediately before settling to the steady rate of
    ``requests_per_period / period_seconds``. This semantics is a settled
    design decision (DESIGN.md §7).

    Attributes:
        requests_per_period: Requests allowed per period (>= 1).
        period_seconds: Length of the rate period in seconds (> 0).
        burst: Bucket capacity in tokens (>= 1).
        max_concurrency: Maximum requests in flight at once (>= 1).
    """

    model_config = ConfigDict(frozen=True, extra='forbid', validate_default=True)

    requests_per_period: int = Field(ge=1)
    period_seconds: float = Field(gt=0)
    burst: int = Field(ge=1)
    max_concurrency: int = Field(ge=1)

    @property
    def refill_rate_per_second(self) -> float:
        """Token refill rate: ``requests_per_period / period_seconds``.

        Returns:
            Tokens added to the bucket per second.
        """
        return self.requests_per_period / self.period_seconds
