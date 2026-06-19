# src/fleetpull/storage/merge.py
"""Merge functions: the per-``SyncMode`` write semantics, plus the cross-cutting
exact-duplicate dedup.

The ``SyncMode`` axis. Each merge takes the existing on-disk frame (or ``None``)
and this run's new frame and returns the frame to persist for one write unit;
they are pure DataFrame transforms, dispatched by the ``SyncMode`` marker in
``persist``. Only ``merge_snapshot`` exists now; ``merge_watermark`` (delete-by-
window-then-append) and ``merge_feed`` (append) arrive with their consumers.

``drop_exact_duplicates`` is the write-time exact dedup (DESIGN §6): applied to
the merged result of every mode (default on; its config off-switch is a later
concern), it clears byte-identical rows -- feed's pagination / refetch duplicates
most importantly, and a cheap safety net elsewhere. It runs on the merged result,
not just the incoming frame, so a feed crash-refetch that re-appends an already-
stored row still collapses.
"""

from collections.abc import Callable

import polars as pl

from fleetpull.incremental import DateWindow

__all__: list[str] = ['MergeFn', 'drop_exact_duplicates', 'in_window', 'merge_snapshot']

# A merge function: (existing-or-None, new) -> the frame to persist for a unit.
type MergeFn = Callable[[pl.DataFrame | None, pl.DataFrame], pl.DataFrame]


def merge_snapshot(existing: pl.DataFrame | None, new: pl.DataFrame) -> pl.DataFrame:
    """Full-replace: the new frame is the result; existing is discarded.

    Args:
        existing: The prior on-disk frame; accepted to satisfy ``MergeFn`` and
            intentionally unused -- a snapshot replaces wholesale.
        new: This run's freshly fetched frame.

    Returns:
        ``new``, unchanged.
    """
    return new


def drop_exact_duplicates(frame: pl.DataFrame) -> pl.DataFrame:
    """Drop byte-identical duplicate rows, preserving first-occurrence order.

    Exactness over all columns: two rows collapse only if every value matches.
    Same-key-different-payload rows are deliberately kept -- collapsing those is
    semantic dedup, out of scope (DESIGN §6).

    Args:
        frame: The frame to dedup.

    Returns:
        The frame with exact-duplicate rows removed, order preserved.
    """
    return frame.unique(maintain_order=True)


def in_window(event_time_column: str, window: DateWindow) -> pl.Expr:
    """The half-open ``[start, end)`` window-membership predicate for a column.

    Returns the boolean Polars expression true for rows whose
    ``event_time_column`` falls in ``window`` -- ``>= window.start`` and
    ``< window.end``, the half-open boundary made literal. It is a *predicate*,
    not a filter: ``merge_watermark`` will apply it in both polarities from the
    one rule -- ``frame.filter(~in_window(col, w))`` to delete a window's rows
    from the existing on-disk frame, ``frame.filter(in_window(col, w))`` to keep
    only the in-window rows of a fresh fetch -- so the boundary is defined in
    exactly one place and "removal" stays the caller's concern (DESIGN §4).

    The full ``[start, end)`` predicate, not merely ``>= start``: in steady state
    ``end`` is ``now`` and ``< end`` binds nothing, but a historical backfill
    chunk has a real ``end``, and ``< end`` is what stops an event on a chunk
    boundary from being claimed by both the chunk ending at it and the one
    starting at it. ``window`` already guarantees ``start < end`` at construction,
    so the predicate never sees an inverted range.

    Args:
        event_time_column: Name of the UTC datetime column to test.
        window: The half-open ``[start, end)`` resume window.

    Returns:
        A boolean ``pl.Expr`` true for in-window rows; apply it (or its negation)
        with ``DataFrame.filter``.

    Side Effects:
        None -- builds an expression; evaluates nothing.
    """
    return (pl.col(event_time_column) >= window.start) & (
        pl.col(event_time_column) < window.end
    )
