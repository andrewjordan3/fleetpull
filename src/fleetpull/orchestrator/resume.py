# src/fleetpull/orchestrator/resume.py
"""The incremental arms' pure resume decisions: interpret the stored cursor.

Pure functions, no I/O and no ``self``: ``resolve_watermark_start`` turns the
stored cursor into resume arm 1 (the watermark less the lookback margin), carrying
the future-watermark guard (Guard A) and the cross-mode rejection that
``incremental/resolution.py`` deliberately omits (that module is cursor-free, so
cursor interpretation and its guards are the orchestrator's policy);
``resolve_feed_resume`` is the feed arm's mirror -- the stored token used
directly, the cold-start ``FeedSeed`` when none is stored (the seed-once
invariant's structural half, DESIGN section 14 I4), and the same cross-mode
rejection in the other direction. The
strictly-forward advance discipline that once lived beside them moved into the
cursor store's atomic ``advance_watermark_forward`` with the prefix-advance rule
(DESIGN section 5, 2026-07-20) -- concurrent unit completions cannot enforce
monotonicity race-free from outside the statement. The runner reads and writes
the cursor; this only computes and validates, following the ``batch.py``
precedent that the runner's pure logic lives beside it, not on the class.
"""

from datetime import datetime, timedelta

from fleetpull.exceptions import ConfigurationError
from fleetpull.incremental import (
    DateWatermark,
    FeedResume,
    FeedSeed,
    FeedToken,
    IncrementalCursor,
)
from fleetpull.vocabulary import Provider

__all__: list[str] = ['resolve_feed_resume', 'resolve_watermark_start']


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


def resolve_feed_resume(
    stored: IncrementalCursor | None,
    default_start: datetime,
    provider: Provider,
    endpoint: str,
) -> FeedResume:
    """Resolve the feed arm's resume value from the stored cursor.

    The feed mirror of ``resolve_watermark_start``: a stored ``FeedToken``
    resumes directly (no transformation — DESIGN section 4), and no stored
    cursor seeds the feed at the sync-wide cold-start anchor. Constructing
    the ``FeedSeed`` here, and ONLY here, is the structural half of the
    seed-once invariant (section 14's I4): a resumed run can never carry a
    seed, because the seed exists only on the no-cursor branch. The
    cross-mode rejection guards the other direction of the watermark
    resolver's — a watermark stored for a feed endpoint is state corruption.

    Args:
        stored: The persisted cursor, or ``None`` when none is committed.
        default_start: The sync-wide cold-start anchor
            (``sync.default_start_datetime``) the seed carries.
        provider: The endpoint's provider (error context).
        endpoint: The endpoint name (error context).

    Returns:
        The stored ``FeedToken``, or a ``FeedSeed`` at ``default_start``
        when no cursor is committed.

    Raises:
        ConfigurationError: A ``DateWatermark`` is stored for this feed
            endpoint (cross-mode state corruption).

    Side Effects:
        None -- pure.
    """
    match stored:
        case None:
            return FeedSeed(start=default_start)
        case FeedToken():
            return stored
        case _:
            raise ConfigurationError(
                'watermark cursor stored for a feed endpoint',
                provider=provider.value,
                endpoint=endpoint,
            )
