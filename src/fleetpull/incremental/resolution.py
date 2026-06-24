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
      coverage frontier, else the cold-start default.
    - ``window_or_none`` -- the assembly: a ``DateWindow`` when ``start < end``, else
      ``None`` (caught up -- no work this run, not an error).

Together they are the watermark resume mechanism (DESIGN section 4): the resolver
floors-and-holds-back the trailing edge and returns ``None`` for a caught-up window
(``start >= end``) rather than raising, so a watermark sitting inside the
still-arriving day is a "no work this run" verdict, not an error.

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
    today_midnight = datetime.combine(now.date(), time.min, tzinfo=UTC)
    return today_midnight - cutoff


def resolve_resume_start(
    watermark_start: datetime | None,
    frontier: datetime | None,
    default_start: datetime,
) -> datetime:
    """
    Pick the window's inclusive start by the resume precedence.

    First present wins (DESIGN section 4): the watermark-derived start when a
    committed watermark exists, else the coverage frontier (the furthest forward a
    succeeded run has covered, even empty), else the cold-start default anchor.
    ``default_start`` is never ``None``, so a start is always produced.

    Args:
        watermark_start: The watermark moment less the lookback (arm 1), or ``None``
            when no watermark is committed. Pre-computed by the caller.
        frontier: The coverage frontier (arm 2), or ``None`` when no succeeded run
            has covered anything yet.
        default_start: The cold-start anchor (arm 3); always present.

    Returns:
        The chosen start, timezone-aware UTC.

    Side Effects:
        None -- pure function.
    """
    if watermark_start is not None:
        return watermark_start
    if frontier is not None:
        return frontier
    return default_start


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
