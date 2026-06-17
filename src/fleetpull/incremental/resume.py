# src/fleetpull/incremental/resume.py
"""The pure watermark-resume function: a stored watermark to the window to fetch.

Maps a stored ``DateWatermark`` (or its absence) to the ``DateWindow`` a watermark
fetch is built from (DESIGN §4): ``DateWatermark`` becomes
``DateWindow(watermark - lookback, now)`` and ``None`` becomes ``None``. This is
the only module in ``incremental/`` that imports both siblings (``cursor`` and
``window``), which keeps the ``DateWindow`` carrier free of any cursor dependency.

Watermark-only by design. The feed arm carries no lookback and no window — its
resume value is the stored ``FeedToken`` used directly by the caller — so it needs
no function and is deliberately not handled here. The parameter type is
``DateWatermark | None``, not the full ``IncrementalCursor``: a ``FeedToken``
reaching this function is a static type error, not a runtime branch.

Pure — no ``Clock``, no codec, no I/O. ``now`` is injected by the caller (which
reads it from the injected ``Clock``); this function never reads time itself. A
``None`` return means only "no committed watermark"; resolving the start from
coverage or the configured default is the caller's job (the resume precedence,
DESIGN §4). An inverted window from a corrupt (future) watermark surfaces as the
``ValueError`` ``DateWindow`` raises — this function adds no guard and lets it
propagate.
"""

from datetime import datetime, timedelta

from fleetpull.incremental.cursor import DateWatermark
from fleetpull.incremental.window import DateWindow

__all__: list[str] = ['compute_resume']


def compute_resume(
    watermark: DateWatermark | None, lookback: timedelta, now: datetime
) -> DateWindow | None:
    """
    Map a stored watermark to the half-open resume window to fetch.

    A ``DateWatermark`` becomes ``DateWindow(watermark - lookback, now)`` — the
    half-open window from ``lookback`` before the last-seen event up to ``now``.
    ``None`` (no committed watermark) returns ``None``. Watermark-only: a
    ``FeedToken`` has no window and is excluded at the type boundary, not handled
    here.

    Args:
        watermark: The stored watermark cursor, or ``None`` when none is committed.
        lookback: How far before the watermark to re-fetch (the late-arrival safety
            margin); may be zero.
        now: The window's exclusive end, injected by the caller from its ``Clock``.

    Returns:
        The resume ``DateWindow``, or ``None`` when ``watermark`` is ``None``.

    Raises:
        ValueError: The computed window is inverted or empty
            (``watermark - lookback >= now``, i.e. a future watermark) —
            propagated unchanged from ``DateWindow``.

    Side Effects:
        None — pure function.
    """
    match watermark:
        case DateWatermark():
            return DateWindow(start=watermark.watermark - lookback, end=now)
        case None:
            return None
