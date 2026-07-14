# src/fleetpull/incremental/resolution.py
"""Pure window-resolution helpers: the stateless pieces that compute a run's window.

The three pure functions a watermark run's window is built from, composed by the
(stateful) orchestrator -- which does the SQLite reads and the clock read and feeds
the values in. Each is single-concern and individually testable; none touches a
clock, a cursor store, a run ledger, or any I/O.

    - ``resolve_trailing_edge`` -- the window's end: the current UTC day floored to
      midnight, held back by the configured cutoff.
    - ``resolve_resume_start`` -- the window's start, by the resume precedence
      (DESIGN section 4): the watermark-derived start if one exists, else the
      coverage frontier, else the cold-start default -- the chosen arm floored
      to the UTC midnight of its date.
    - ``window_or_none`` -- the assembly: a ``DateWindow`` when ``start < end``, else
      ``None`` (caught up -- no work this run, not an error).

Together they are the watermark resume mechanism (DESIGN section 4): the resolver
day-aligns both bounds -- the start floored to its UTC midnight, the trailing edge
floored and held back -- and returns ``None`` for a caught-up window
(``start >= end``) rather than raising, so a watermark sitting inside the
still-arriving day is a "no work this run" verdict, not an error. Day alignment
is the floored-window invariant the date-partitioned writers document: requests
and partitions are day-granular, so every covered date must be refetched in
full for wholesale partition replacement and the prune to be safe.

The start candidates are pre-resolved datetimes, not cursors: the orchestrator
extracts the watermark moment and subtracts the lookback (arm 1's one line) before
calling, so this module stays pure datetime math with no cursor dependency. The
cutoff is expected to be a whole-day ``timedelta`` (its only source is
``timedelta(days=cutoff_days)``), which keeps the trailing edge date-aligned -- the
partition-wholeness invariant storage relies on. UTC validity is not checked here;
the bounds cross the codec boundary when a spec-builder serializes them, and that
boundary raises on naive/non-UTC, exactly as for ``DateWindow``.
"""

from datetime import UTC, datetime, time, timedelta

from fleetpull.incremental.window import DateWindow

__all__: list[str] = [
    'resolve_resume_start',
    'resolve_trailing_edge',
    'window_or_none',
]


def _utc_midnight(moment: datetime) -> datetime:
    """The UTC midnight beginning ``moment``'s UTC date (the day-alignment floor)."""
    return datetime.combine(moment.date(), time.min, tzinfo=UTC)


def resolve_trailing_edge(now: datetime, cutoff: timedelta) -> datetime:
    """
    Compute the window's exclusive end: today's UTC midnight, less the cutoff.

    Floors ``now`` to the start of its UTC day (the most recent midnight at or before
    ``now``) so the still-arriving current day is never inside the window, then holds
    the edge back by ``cutoff`` for providers whose data settles later. With
    ``cutoff`` zero the end is today's midnight, covering through yesterday -- the
    last complete day.

    Args:
        now: The current instant, timezone-aware UTC (the caller reads it from its
            ``Clock``); its UTC date is the day floored to.
        cutoff: Trailing-edge holdback, a whole-day ``timedelta`` (zero or more);
            whole days keep the returned edge midnight-aligned.

    Returns:
        The exclusive end, a UTC-midnight datetime with ``tzinfo`` ``datetime.UTC``.

    Side Effects:
        None -- pure function.
    """
    return _utc_midnight(now) - cutoff


def resolve_resume_start(
    watermark_start: datetime | None,
    frontier: datetime | None,
    default_start: datetime,
) -> datetime:
    """
    Pick the window's inclusive start by the resume precedence, floored to
    the UTC midnight of its date.

    First present wins (DESIGN section 4): the watermark-derived start when a
    committed watermark exists, else the coverage frontier (the furthest forward a
    succeeded run has covered, even empty), else the cold-start default anchor.
    ``default_start`` is never ``None``, so a start is always produced.

    The chosen start is floored to its UTC midnight because day-granular
    endpoints request and replace whole days: the effective window must be
    day-aligned on both bounds so every covered date is refetched in full --
    the floored-window invariant the date-partitioned writers' wholesale
    replacement and prune are safe under. Unfloored, a watermark's
    ``23:59:59`` less the lookback covers its boundary date by a seconds-wide
    sliver: the day-granular request fetches the whole day, the window filter
    keeps only the sliver, and replacement/prune then destroy the boundary
    partition (the live severely truncated boundary-partition defect). Floored, lookback reads as
    "re-cover N whole days before the watermark's day". Arms 2 and 3 are
    midnight-aligned by construction, so the floor is idempotent there and
    load-bearing on arm 1. This rule is for snapshot/point-event endpoints
    (each record a single point in time); duration/span endpoints (e.g. HOS
    periods crossing midnight) need their own boundary policy when they
    arrive and must not blindly reuse it.

    Args:
        watermark_start: The watermark moment less the lookback (arm 1), or ``None``
            when no watermark is committed. Pre-computed by the caller.
        frontier: The coverage frontier (arm 2), or ``None`` when no succeeded run
            has covered anything yet.
        default_start: The cold-start anchor (arm 3); always present.

    Returns:
        The chosen start floored to the UTC midnight beginning its date,
        timezone-aware UTC.

    Side Effects:
        None -- pure function.
    """
    if watermark_start is not None:
        return _utc_midnight(watermark_start)
    if frontier is not None:
        return _utc_midnight(frontier)
    return _utc_midnight(default_start)


def window_or_none(start: datetime, end: datetime) -> DateWindow | None:
    """
    Assemble the window, or ``None`` when there is no work this run.

    Returns ``DateWindow(start, end)`` when ``start < end``. When ``start >= end`` the
    resume point has reached or passed the trailing edge -- caught up -- and the
    result is ``None``: a verdict ("nothing to fetch"), not an error. An inverted
    window is therefore never constructed through this path, so ``DateWindow``'s own
    ``start < end`` invariant is a backstop for a direct construction bug, never
    tripped here.

    Args:
        start: The window's inclusive start, timezone-aware UTC.
        end: The window's exclusive end, timezone-aware UTC.

    Returns:
        The ``DateWindow`` when ``start < end``, else ``None``.

    Side Effects:
        None -- pure function.
    """
    if start < end:
        return DateWindow(start=start, end=end)
    return None
