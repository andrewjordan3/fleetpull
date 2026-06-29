# src/fleetpull/records/roster_members.py
"""Roster-member extraction over a records frame: a column's distinct values as
strings.

The roster members a feeder listing produces -- the distinct values of one column of
the feeder's validated frame, stringified for use as roster members and URL-path
fan-out values. The refresh path reads this off the listed-and-validated feeder frame
and hands the members to ``reconcile``; mapping a member to anything else (a VIN) is
never needed -- the value is opaque.

A pure leaf over the records frame -- Polars and stdlib only, imports nothing internal,
the placement rationale of ``latest_event_time`` beside it: it reads a finished records
frame rather than building one, so it sits with the records-layer extractors. It raises
``ValueError`` on a missing column or a null member -- both bad-input/wiring failures
the exception hierarchy leaves to stdlib, and the pure leaf has no provider context for
a typed operational error. The refresh coordinator, which holds that context, is where
a null member would surface as a provider-contract failure if that is wanted.
"""

import polars as pl

__all__: list[str] = ['extract_roster_members']


def extract_roster_members(frame: pl.DataFrame, column: str) -> set[str]:
    """The distinct values of a frame column, as strings, for roster members.

    Reads ``column`` and returns its distinct values stringified (a numeric id becomes
    its decimal string, ready for a roster member and a URL-path value). A roster is an
    unordered set, so the result is a ``set`` and extraction order is not preserved.

    Args:
        frame: The fetched, validated feeder frame.
        column: Name of the column whose distinct values are the members (e.g.
            ``'vehicle_id'``).

    Returns:
        The distinct values of ``column`` as a set of strings; an empty set when the
        frame is empty.

    Raises:
        ValueError: ``column`` is absent from ``frame`` (a wiring bug -- the roster
            definition names a column the feeder frame lacks), or ``column`` holds a
            null (a null is not a roster member). Both are left to stdlib rather than
            the operational hierarchy, which the pure leaf cannot give provider context.

    Side Effects:
        None -- pure function.
    """
    if column not in frame.columns:
        raise ValueError(f'roster source column {column!r} is not in the frame')
    series = frame.get_column(column)
    if series.null_count() > 0:
        raise ValueError(
            f'roster source column {column!r} holds a null; a null is not a '
            'roster member'
        )
    return {str(value) for value in series.unique().to_list()}
