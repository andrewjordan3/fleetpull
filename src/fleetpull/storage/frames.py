# src/fleetpull/storage/frames.py
"""Frame operations the write path composes: the exact-duplicate dedup and the
half-open window-membership predicate.

Two pure DataFrame helpers. ``drop_exact_duplicates`` is the write-time exact
dedup (DESIGN §6): the replace-partition and single-file writers run it on
their finalized frames, clearing byte-identical rows as a cheap safety net.
The feed append cell deliberately does NOT (DESIGN §4's stored-as-emitted
contract and §14's append-only invariant): crash-window and re-emission
duplicates are the log's honest content, reconciled by the consumer. ``in_window`` is the half-open ``[start, end)``
membership predicate that defines the window boundary in exactly one place: its
consumer today is the orchestrator's batch shaping, which keeps a fetch's
in-window rows before they reach a writer; the future single-file window-clearing
cells will apply it in both polarities (delete the existing window's rows, keep
the fresh fetch's in-window rows) from the same rule.
"""

import polars as pl

from fleetpull.incremental import DateWindow

__all__: list[str] = ['drop_exact_duplicates', 'in_window']


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
    not a filter, so the boundary is defined in exactly one place and "removal"
    stays the caller's concern (DESIGN §4). Its consumer today is the
    orchestrator's batch shaping, which keeps a fetch's in-window rows with
    ``frame.filter(in_window(col, w))``; the future single-file window-clearing
    cells will add the other polarity, ``frame.filter(~in_window(col, w))``, to
    delete a window's rows from the existing on-disk frame.

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
