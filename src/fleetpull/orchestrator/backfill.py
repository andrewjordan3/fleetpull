# src/fleetpull/orchestrator/backfill.py
"""Window decomposition: a windowed run's range into per-chunk work units.

The caller-side planning the work-unit store defers to its driver (DESIGN §5).
Every windowed run -- the daily pull and a long backfill alike -- tiles its
(provider, endpoint) window into whole-UTC-day chunks, one unit per chunk (a
window smaller than one chunk is a single unit). A unit fans the whole roster
at execution, so it carries no member -- one planner serves fan-out and
non-fan-out watermark endpoints alike, the fan-out distinction being the
driver's, not the unit's. Pure functions only: the runner resolves the window,
calls these to plan, and drives the queue.
"""

from datetime import datetime, timedelta

from fleetpull.incremental import DateWindow
from fleetpull.state import WorkUnitSpec
from fleetpull.vocabulary import Provider

__all__: list[str] = ['plan_backfill_units']


def _is_utc_midnight(value: datetime) -> bool:
    """True when ``value`` is exactly midnight UTC -- a whole-UTC-day boundary.

    A date-partition boundary: a zero UTC offset and a zeroed time of day. Naive
    and non-UTC datetimes are rejected (``utcoffset`` is ``None`` or nonzero), so
    this also gates the timezone validity ``DateWindow`` does not.
    """
    return (
        value.utcoffset() == timedelta(0)
        and value.hour == 0
        and value.minute == 0
        and value.second == 0
        and value.microsecond == 0
    )


def _date_chunks(span: DateWindow, chunk: timedelta) -> list[tuple[datetime, datetime]]:
    """Tile a half-open span into contiguous whole-UTC-day chunks.

    Splits ``[span.start, span.end)`` into half-open chunks of ``chunk`` width,
    left to right; the final chunk runs to ``span.end`` and so may be shorter
    (but still a whole number of days). A chunk is emitted only while the cursor
    is strictly before ``span.end``, so no zero-width chunk is produced and every
    bound pair satisfies the work-unit store's ``chunk_start < chunk_end``.

    Both span bounds must be midnight UTC and ``chunk`` a positive whole number of
    days, so every chunk bound lands on midnight UTC. The date-partitioned
    watermark writer replaces whole date partitions, so a chunk boundary mid-day
    would drive partial-day replacement and silently corrupt the partitions it
    touches -- hence the guards rather than a permissive ``timedelta``.

    Args:
        span: The half-open range to tile; both bounds midnight UTC.
        chunk: The width of each chunk; a positive whole number of days.

    Returns:
        The chunk bounds in order, each ``(chunk_start, chunk_end)`` on midnight
        UTC with ``chunk_start < chunk_end``; contiguous (each chunk's end is the
        next's start), the first at ``span.start`` and the last at ``span.end``.

    Raises:
        ValueError: When ``chunk`` is not a positive whole number of days, or a
            span bound is not midnight UTC -- caller bugs, kept stdlib.
    """
    if chunk <= timedelta(0) or chunk % timedelta(days=1) != timedelta(0):
        raise ValueError(
            f'backfill chunk must be a positive whole number of days: {chunk!r}'
        )
    if not _is_utc_midnight(span.start) or not _is_utc_midnight(span.end):
        raise ValueError(
            'backfill chunks require whole-UTC-day span bounds (midnight UTC): '
            f'start={span.start!r}, end={span.end!r}'
        )
    chunks: list[tuple[datetime, datetime]] = []
    start = span.start
    while start < span.end:
        end = min(start + chunk, span.end)
        chunks.append((start, end))
        start = end
    return chunks


def plan_backfill_units(
    provider: Provider,
    endpoint: str,
    span: DateWindow,
    chunk: timedelta,
) -> list[WorkUnitSpec]:
    """Decompose a backfill into one work unit per whole-UTC-day chunk.

    The caller-side decomposition the work-unit store leaves to its driver
    (DESIGN §5): tile the span into whole-UTC-day chunks, one unit per chunk. A
    unit fans the whole roster at execution, so it carries no member
    (``partition_key=None``); the unit loop drives each chunk through the
    endpoint's already-resolved driver. So one planner serves fan-out and
    non-fan-out watermark endpoints alike; the fan-out distinction is the
    driver's, not the unit's. Pure: it returns the specs; enqueuing them
    idempotently is the caller's, kept separate so the plan can be inspected and
    the enqueue stays the store's one write.

    Args:
        provider: The provider being backfilled.
        endpoint: The endpoint being backfilled.
        span: The full backfill range, half-open and midnight-UTC on both bounds
            (the runner's residual-window resolution builds it from the resume
            precedence against the trailing edge).
        chunk: The width of each date chunk; a positive whole number of days.

    Returns:
        One ``WorkUnitSpec`` per chunk, in chronological order, ready to enqueue.

    Raises:
        ValueError: When the span bounds are not midnight UTC or ``chunk`` is not a
            positive whole number of days (from :func:`_date_chunks`).
    """
    return [
        WorkUnitSpec(
            provider=provider,
            endpoint=endpoint,
            partition_key=None,
            chunk_start=chunk_start,
            chunk_end=chunk_end,
        )
        for chunk_start, chunk_end in _date_chunks(span, chunk)
    ]
