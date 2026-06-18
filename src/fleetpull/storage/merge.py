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

__all__: list[str] = ['MergeFn', 'drop_exact_duplicates', 'merge_snapshot']

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
