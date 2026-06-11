# src/fleetpull/network/limits/bucket_math.py
"""Pure token-bucket arithmetic.

Stateless functions with no clock, no threading, and no state — just math.
They exist so the bucket arithmetic is exhaustively testable single-threaded
with plain assertions, leaving ``limiter.py`` to hold only state and thread
choreography.
"""

__all__: list[str] = ['refill_tokens', 'seconds_until_available']


def refill_tokens(
    current_tokens: float,
    elapsed_seconds: float,
    refill_rate_per_second: float,
    capacity: float,
) -> float:
    """Compute the token count after lazily refilling for elapsed time.

    Args:
        current_tokens: Tokens in the bucket before the refill.
        elapsed_seconds: Seconds since the last refill (must be >= 0).
        refill_rate_per_second: Tokens added per second.
        capacity: Maximum tokens the bucket holds.

    Returns:
        ``min(capacity, current_tokens + elapsed_seconds * refill_rate_per_second)``.

    Raises:
        ValueError: If ``elapsed_seconds`` is negative. A monotonic clock can
            never produce one; if it appears, something upstream is broken
            and must fail loudly.
    """
    if elapsed_seconds < 0:
        raise ValueError(
            f'elapsed_seconds must be non-negative, got {elapsed_seconds}.'
        )
    return min(capacity, current_tokens + elapsed_seconds * refill_rate_per_second)


def seconds_until_available(
    current_tokens: float,
    refill_rate_per_second: float,
    tokens_needed: float = 1.0,
) -> float:
    """Compute the exact wait until the bucket holds ``tokens_needed`` tokens.

    Args:
        current_tokens: Tokens currently in the bucket.
        refill_rate_per_second: Tokens added per second.
        tokens_needed: Tokens required before proceeding.

    Returns:
        0.0 if ``current_tokens >= tokens_needed``; otherwise the seconds
        needed for the deficit to refill at ``refill_rate_per_second``.
    """
    if current_tokens >= tokens_needed:
        return 0.0
    return (tokens_needed - current_tokens) / refill_rate_per_second
