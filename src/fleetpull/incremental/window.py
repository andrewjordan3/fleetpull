# src/fleetpull/incremental/window.py
"""The half-open watermark resume window — the canonical internal fetch window.

A pure, stdlib-only leaf beside the cursors (DESIGN §4). ``DateWindow`` is the
*resume value* a watermark fetch is built from, distinct from the stored
``DateWatermark`` cursor: the cursor records the maximum event timestamp seen, and
the window is what the resume resolver derives from it (``watermark - lookback`` up
to the trailing edge). A fetch is built from the window, never from the watermark
directly.

The window is half-open, ``[start, end)`` — start inclusive, end exclusive — and
that is the canonical internal form: the half-open boundary is what lets storage's
delete-by-window predicate (``>= start & < end``) and the start-anchored
append-filter share one rule, so a cross-boundary event is never double-counted at
a window edge (DESIGN §4). Unlike the pure ``cursor.py`` carriers, ``DateWindow``
enforces one structural invariant on construction — ``start < end`` — because that
ordering has no downstream enforcement; UTC validity, which does (the codec
boundary), is deferred there exactly as ``DateWatermark`` defers it.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta

__all__: list[str] = ['DateWindow']


@dataclass(frozen=True, slots=True)
class DateWindow:
    """
    The half-open ``[start, end)`` watermark resume window.

    The resume value the spec-builder turns into a request, distinct from the
    stored ``DateWatermark`` cursor — a fetch is built from this window, not from
    the watermark. Half-open by definition: ``start`` inclusive, ``end`` exclusive.
    The half-open boundary is the canonical internal convention that lets the
    delete-by-window predicate and the start-anchored append-filter share one rule
    (DESIGN §4); there is deliberately no ``contains`` / ``__contains__`` —
    membership is storage's vectorized Polars ``>= start & < end``, never scalar
    row-level.

    Construction enforces the one structural invariant, ``start < end`` (strict,
    well-ordered, mirroring the run ledger's window). It lives here on the type,
    not deferred like ``DateWatermark``'s UTC check, because the ordering has no
    downstream boundary to catch it — an inverted or empty window is a loud-failure
    bug, not a value to carry; the resume resolver returns ``None`` for a caught-up
    ``start >= end`` rather than constructing one, so this invariant now backstops a
    direct construction bug. UTC validity is not checked here; it crosses the codec
    boundary when a spec-builder serializes the bounds, and that boundary raises on
    naive/non-UTC, exactly as for ``DateWatermark``.

    Attributes:
        start: The window's inclusive start, timezone-aware UTC.
        end: The window's exclusive end, timezone-aware UTC; must be after
            ``start``.
    """

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        """
        Enforce the ``start < end`` ordering invariant on construction.

        Raises:
            ValueError: ``start`` is not strictly before ``end`` (an inverted or
                empty window); the message names both bounds.
            TypeError: ``start`` and ``end`` mix naive and aware datetimes —
                surfaced from the stdlib comparison, a loud failure (UTC validity
                otherwise defers to the codec boundary).

        Side Effects:
            None — reads ``start`` / ``end`` and may raise.
        """
        if self.start >= self.end:
            raise ValueError(
                f'DateWindow requires start < end; got start={self.start!r}, '
                f'end={self.end!r}'
            )

    @property
    def last_covered_date(self) -> date:
        """
        The last calendar date the half-open window covers.

        The ``(end - 1µs).date()`` derivation, stated once: the exclusive
        ``end`` itself is not covered, so a window ending exactly at UTC
        midnight does not cover that date while a mid-day ``end`` does. The
        one-microsecond step back is exact because every datetime in
        fleetpull is microsecond-precision UTC end to end (DESIGN §3/§4).

        Returns:
            The UTC calendar date of the last covered instant.
        """
        return (self.end - timedelta(microseconds=1)).date()
