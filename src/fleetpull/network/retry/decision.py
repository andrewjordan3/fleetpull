# src/fleetpull/network/retry/decision.py
"""The retry decision: pure policy, no loop, no sleep, no state.

The client asks one question after each retryable failure — "retry,
and with what local delay?" — and this module answers it. The answer
travels as a frozen ``RetryDecision`` rather than an overloaded
``float | None``: the None-versus-0.0 distinction is exactly the kind
of subtle contract retry bugs breed in.

Division of labor (DESIGN §7): the limiter owns all rate-limit
waiting. RATE_LIMITED decisions therefore never carry a local delay —
the 429 penalized the shared quota scope, and the next
``request_slot()`` call waits it out. Local delay exists only for
TRANSIENT backoff, computed here, slept by the client.
"""

import logging
from dataclasses import dataclass
from typing import Protocol

from fleetpull.config.retry import RetryConfig
from fleetpull.vocabulary import ResponseCategory

__all__: list[str] = [
    'RandomFractionGenerator',
    'RetryDecision',
    'decide_retry',
]

logger = logging.getLogger(__name__)


class RandomFractionGenerator(Protocol):
    """
    Generates random fractions for retry jitter.

    The policy needs exactly one capability — a uniform fraction — so
    the seam is one method, not a concrete randomness class (the Clock
    precedent applied to jitter). ``random.Random`` satisfies this
    protocol structurally; the composition root passes
    ``random.Random()`` directly.
    """

    def random(self) -> float:
        """Return a float in the half-open interval [0.0, 1.0)."""
        ...


@dataclass(frozen=True, slots=True)
class RetryDecision:
    """
    The answer to "retry, and with what local delay?".

    Attributes:
        should_retry: Whether the failure budget permits another
            attempt.
        local_delay_seconds: Seconds the client sleeps before the next
            attempt. Meaningful only when ``should_retry`` is True;
            0.0 otherwise (the established inert-field pattern).
            Always 0.0 for RATE_LIMITED — the penalized limiter is
            that category's backoff.
    """

    should_retry: bool
    local_delay_seconds: float


def _transient_delay_seconds(
    failure_count: int,
    config: RetryConfig,
    random_source: RandomFractionGenerator,
) -> float:
    """
    Full-jitter exponential backoff for one TRANSIENT failure.

    Args:
        failure_count: One-based failure count within TRANSIENT.
        config: The retry policy.
        random_source: Injected randomness seam; stubs make the
            arithmetic exactly assertable in tests.

    Returns:
        A delay drawn uniformly from the half-open interval
        ``[0, min(cap, base * 2 ** (failure_count - 1)))`` — the
        fraction source is ``[0.0, 1.0)``, so the delay never equals
        the envelope; the same arithmetic ``random.uniform`` performs
        internally.
    """
    envelope_seconds: float = min(
        config.transient_backoff_cap_seconds,
        config.transient_backoff_base_seconds * 2 ** (failure_count - 1),
    )
    return envelope_seconds * random_source.random()


def decide_retry(
    category: ResponseCategory,
    failure_count: int,
    config: RetryConfig,
    random_source: RandomFractionGenerator,
) -> RetryDecision:
    """
    Decide whether a retryable failure gets another attempt.

    ``failure_count`` is one-based within the current retryable
    category: the first TRANSIENT failure of an attempt sequence is 1.
    Category counters are independent — the caller keeps one per
    category and neither resets the other. The comparison is
    ``failure_count > max_failures``, so ``max_failures = N`` retries
    failures 1..N and exhausts on the (N+1)th: at most N + 1 requests.

    Args:
        category: The classification of the failure. Only TRANSIENT
            and RATE_LIMITED are retryable; anything else here is a
            caller bug.
        failure_count: One-based failure count within ``category``.
        config: The retry policy.
        random_source: Injected randomness seam for jitter. Required —
            implementations are stateful, so the parameter earns no
            frozen-and-stateless default; the composition root owns
            one instance (``random.Random()`` satisfies the protocol
            structurally).

    Returns:
        The decision. Exhausted budgets return
        ``RetryDecision(should_retry=False, local_delay_seconds=0.0)``;
        the caller raises ``RetriesExhaustedError`` with the terminal
        failure count as ``attempt_count`` (equal by definition —
        every attempt failed).

    Raises:
        ValueError: When ``category`` is not retryable or
            ``failure_count`` is less than 1 — programming errors,
            deliberately stdlib per the hierarchy's closure stance.
    """
    if failure_count < 1:
        raise ValueError(f'failure_count is one-based; got {failure_count}')
    match category:
        case ResponseCategory.TRANSIENT:
            if failure_count > config.transient_max_failures:
                return RetryDecision(should_retry=False, local_delay_seconds=0.0)
            return RetryDecision(
                should_retry=True,
                local_delay_seconds=_transient_delay_seconds(
                    failure_count, config, random_source
                ),
            )
        case ResponseCategory.RATE_LIMITED:
            if failure_count > config.rate_limited_max_failures:
                return RetryDecision(should_retry=False, local_delay_seconds=0.0)
            # The limiter already absorbed this 429's penalty; the
            # retry waits in request_slot(), never here.
            return RetryDecision(should_retry=True, local_delay_seconds=0.0)
        case _:
            raise ValueError(
                f'category {category!r} is not retryable; only TRANSIENT '
                'and RATE_LIMITED reach retry policy'
            )
