# src/fleetpull/records/roster_members.py
"""Roster-member extraction over a records frame: a column's distinct values as
strings.

The roster members a feeder listing produces -- the distinct values of one column of
the feeder's validated frame, stringified for use as roster members and URL-path
fan-out values. The refresh path reads this off the listed-and-validated feeder frame
and hands the members to ``reconcile``; mapping a member to anything else (a VIN) is
never needed -- the value is opaque.

A pure-ish leaf over the records frame -- Polars, stdlib logging, nothing internal;
it reads a finished records frame rather than building one, so it sits with the
records-layer extractors. A missing column raises ``ValueError`` -- a wiring bug
(the roster definition names a column the feeder frame lacks) the exception
hierarchy leaves to stdlib. Null and empty-string values are *filtered, loudly*
rather than raised: both are unfetchable members by construction (a null carries
no id; an empty string renders an unbuildable URL path), and excluding one garbage
record beats converting it into an outage for every other member's fan-out. The
filter logs a warning with the column and counts, so a provider emitting garbage
ids is visible without being fatal.
"""

import logging

import polars as pl

__all__: list[str] = ['extract_roster_members']

logger = logging.getLogger(__name__)


def extract_roster_members(frame: pl.DataFrame, column: str) -> set[str]:
    """The distinct fetchable values of a frame column, as strings.

    Reads ``column`` and returns its distinct values stringified (a numeric id
    becomes its decimal string, ready for a roster member and a URL-path value).
    A roster is an unordered set, so the result is a ``set`` and extraction order
    is not preserved. Null and empty-string values are filtered with a warning --
    unfetchable by construction, excluded rather than fatal -- so the returned
    members are exactly the fan-out-usable ones.

    Args:
        frame: The fetched, validated feeder frame.
        column: Name of the column whose distinct values are the members (e.g.
            ``'vehicle_id'``).

    Returns:
        The distinct non-null, non-empty values of ``column`` as a set of
        strings; an empty set when the frame is empty or holds no usable value.

    Raises:
        ValueError: ``column`` is absent from ``frame`` -- a wiring bug (the
            roster definition names a column the feeder frame lacks), left to
            stdlib rather than the operational hierarchy, which this leaf cannot
            give provider context.

    Side Effects:
        Logs a warning when null or empty-string values were filtered.
    """
    if column not in frame.columns:
        raise ValueError(f'roster source column {column!r} is not in the frame')
    series = frame.get_column(column)
    null_row_count = series.null_count()
    stringified = series.drop_nulls().cast(pl.String)
    empty_row_count = int((stringified.str.len_chars() == 0).sum())
    if null_row_count or empty_row_count:
        logger.warning(
            'filtered unfetchable members from roster source column %r: '
            '%d null and %d empty-string value(s) excluded',
            column,
            null_row_count,
            empty_row_count,
        )
    return {value for value in stringified.unique().to_list() if value != ''}
