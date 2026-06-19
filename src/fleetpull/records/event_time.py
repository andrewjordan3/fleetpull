# src/fleetpull/records/event_time.py
"""Event-time observation over a records frame: the latest event timestamp.

The watermark candidate a successful watermark fetch produces -- the maximum
value of the endpoint's event-time column in the fetched records (DESIGN §5,
observed-data-only). After a fetch is persisted, the orchestrator reads this
maximum and, if it advances the stored watermark, commits it; the monotonic guard
and the wrapping into a ``DateWatermark`` are the orchestrator's and the state
layer's, not this function's -- it returns the raw ``datetime`` so it stays
ignorant of the cursor type.

A pure leaf over the records frame -- Polars and stdlib ``datetime`` only, imports
nothing internal. It reads a finished records frame (records' output) rather than
building one, which is why it sits beside the construction modules rather than in
``incremental`` (a deliberately stdlib-only, Polars-free leaf) or ``state`` (which
the DESIGN keeps free of frame knowledge -- the caller hands it the computed
value).
"""

from datetime import datetime

import polars as pl

__all__: list[str] = ['latest_event_time']


def latest_event_time(frame: pl.DataFrame, event_time_column: str) -> datetime | None:
    """The maximum value of a records frame's event-time column.

    Args:
        frame: The fetched, validated records frame. ``event_time_column`` must be
            a UTC datetime column.
        event_time_column: Name of the UTC datetime column to take the maximum of
            (e.g. ``'located_at'``).

    Returns:
        The latest (maximum) timestamp in ``event_time_column``, or ``None`` when
        the frame is empty (no observed events, hence no watermark advance).

    Raises:
        polars.exceptions.ColumnNotFoundError: ``event_time_column`` is absent
            from ``frame`` -- a caller bug, surfaced unguarded by Polars.
        TypeError: ``event_time_column``'s maximum is present but not a
            ``datetime`` (e.g. the column is a date or numeric), which would make
            an invalid watermark -- raised loud rather than returned.

    Side Effects:
        None -- pure function.
    """
    maximum = frame.get_column(event_time_column).max()
    if maximum is None:
        return None
    if not isinstance(maximum, datetime):
        raise TypeError(
            f'expected a datetime maximum for column {event_time_column!r}, '
            f'got {type(maximum).__name__}'
        )
    return maximum
