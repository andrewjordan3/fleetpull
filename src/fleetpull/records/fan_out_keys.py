# src/fleetpull/records/fan_out_keys.py
"""Fan-out key extraction over a records frame: a column's distinct values as strings.

The fan-out keys a feeder listing produces -- the distinct values of one column of the
feeder's validated frame, stringified for use as URL-path values and roster members.
The orchestrator reads this off the listed-and-validated feeder frame at roster-refresh
time and hands the keys to ``reconcile``; mapping a key to anything else (a VIN) is
never needed -- the value is opaque.

A pure leaf over the records frame -- Polars and stdlib only, imports nothing internal,
the same placement rationale as ``latest_event_time`` beside it: it reads a finished
records frame rather than building one, so it sits with the records-layer extractors,
not in ``state`` (kept free of frame knowledge) or a Polars-free leaf.
"""

import polars as pl

__all__: list[str] = ['extract_fan_out_keys']


def extract_fan_out_keys(frame: pl.DataFrame, column: str) -> list[str]:
    """The distinct values of a frame column, as strings, for fan-out keys.

    Reads ``column``, drops nulls (a null is not a fan-out key -- the feeder's key field
    is required upstream, so this is defensive), takes the distinct values in
    first-occurrence order, and stringifies each (a numeric id becomes its decimal
    string, ready for a URL-path value and a roster member).

    Args:
        frame: The fetched, validated feeder frame.
        column: Name of the column whose distinct values are the keys (e.g.
            ``'vehicle_id'``).

    Returns:
        The distinct non-null values of ``column`` as strings, in first-occurrence
        order; empty when the frame is empty.

    Raises:
        polars.exceptions.ColumnNotFoundError: ``column`` is absent from ``frame`` -- a
            caller bug, surfaced unguarded by Polars.

    Side Effects:
        None -- pure function.
    """
    values = frame.get_column(column).drop_nulls().unique(maintain_order=True)
    return [str(value) for value in values.to_list()]
