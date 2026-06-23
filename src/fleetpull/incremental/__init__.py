# src/fleetpull/incremental/__init__.py
"""Per-endpoint incremental resume state: the cursors, the resume window, and the pure functions that resolve it."""

from fleetpull.incremental.cursor import (
    DateWatermark,
    FeedToken,
    IncrementalCursor,
)
from fleetpull.incremental.resolution import (
    resolve_resume_start,
    resolve_trailing_edge,
    window_or_none,
)
from fleetpull.incremental.resume import compute_resume
from fleetpull.incremental.window import DateWindow

__all__: list[str] = [
    'DateWatermark',
    'DateWindow',
    'FeedToken',
    'IncrementalCursor',
    'compute_resume',
    'resolve_resume_start',
    'resolve_trailing_edge',
    'window_or_none',
]
