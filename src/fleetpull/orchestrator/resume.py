# src/fleetpull/orchestrator/resume.py
"""The watermark arm's pure resume decisions: interpret the cursor, gate the advance.

Two pure functions, no I/O and no ``self``: ``resolve_watermark_start`` turns the
stored cursor into resume arm 1 (the watermark less the lookback margin), carrying
the future-watermark guard (Guard A) and the cross-mode rejection that
``incremental/resolution.py`` deliberately omits (that module is cursor-free, so
cursor interpretation and its guards are the orchestrator's policy);
``should_advance_watermark`` decides whether an observed maximum is a
strictly-forward advance -- the monotonicity discipline the cursor store omits
(DESIGN section 4/5). The runner reads and writes the cursor; these only compute and
validate, following the ``batch.py`` precedent that the runner's pure logic lives
beside it, not on the class.
"""

from datetime import datetime, timedelta

from fleetpull.exceptions import ConfigurationError
from fleetpull.incremental import DateWatermark, IncrementalCursor
from fleetpull.vocabulary import Provider

__all__: list[str] = ['resolve_watermark_start', 'should_advance_watermark']


def resolve_watermark_start(
    stored: IncrementalCursor | None,
    lookback: timedelta,
    now: datetime,
    provider: Provider,
    endpoint: str,
) -> datetime | None:
    """Resolve resume arm 1 from the stored cursor, guarding a future date.

    Arm 1 of the resume precedence (DESIGN section 4): the committed watermark less
    the lookback re-fetch margin, or ``None`` when no watermark is committed (the
    caller then falls through to the coverage frontier and the cold-start
    anchor). The future-watermark guard (Guard A) and the cross-mode rejection
    live here -- the resolution math in ``incremental/resolution.py`` is
    deliberately cursor-free, so cursor interpretation and its guards are the
    orchestrator's policy.

    Args:
        stored: The persisted cursor, or ``None`` when none is committed.
        lookback: The watermark mode's late-arrival re-fetch margin.
        now: The run's clock instant.
        provider: The endpoint's provider (error context).
        endpoint: The endpoint name (error context).

    Returns:
        The watermark instant less ``lookback`` when a ``DateWatermark`` is
        stored, else ``None``. This is the datetime-granular arm-1 candidate;
        ``resolve_resume_start`` floors whichever arm it picks to the UTC
        midnight of its date (the floored-window invariant).

    Raises:
        ConfigurationError: The stored watermark is dated after ``now`` (Guard A
            -- it would otherwise stall the endpoint as a permanent caught-up),
            or a ``FeedToken`` is stored for this watermark endpoint (cross-mode
            state corruption).

    Side Effects:
        None -- pure.
    """
    match stored:
        case None:
            return None
        case DateWatermark(watermark=watermark):
            if watermark > now:
                raise ConfigurationError(
                    'stored watermark is in the future',
                    provider=provider.value,
                    endpoint=endpoint,
                    detail=(
                        f'watermark {watermark.isoformat()} is after the '
                        f'run clock {now.isoformat()}'
                    ),
                )
            return watermark - lookback
        case _:
            raise ConfigurationError(
                'feed cursor stored for a watermark endpoint',
                provider=provider.value,
                endpoint=endpoint,
            )


def should_advance_watermark(
    stored: IncrementalCursor | None, observed: datetime
) -> bool:
    """Whether an observed in-window maximum is a strictly-forward advance.

    The only-persist-a-strictly-forward-watermark discipline the cursor store
    deliberately omits (DESIGN section 5). ``observed`` is the run's folded in-window
    maximum; the caller only asks when the run observed at least one in-window
    event (so ``observed`` is never ``None`` here, and the empty-run "hold the
    cursor" case is the caller's gate, not this function's). Returns ``True``
    when no watermark is stored (the first advance) or when ``observed`` is
    strictly greater than the stored watermark; a ``FeedToken`` cannot reach
    here (it is rejected in ``resolve_watermark_start``).

    Args:
        stored: The cursor read at the start of the run.
        observed: The run's folded in-window maximum event time.

    Returns:
        ``True`` when the cursor should advance to ``observed``.

    Side Effects:
        None -- pure.
    """
    if isinstance(stored, DateWatermark):
        return observed > stored.watermark
    return True
