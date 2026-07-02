# src/fleetpull/timing/canon.py
"""The canonical-UTC surface: normalize at ingress, require in the interior.

The canonical interior temporal form is exactly one: a timezone-aware
``datetime`` whose ``tzinfo is datetime.UTC`` -- identity, not offset-equality.
``datetime.date`` serves calendar concepts (timezone-free by nature); strings
exist only at wire/storage edges via the codec. Two verbs enforce the form:

    - **Ingress normalizes** (``ensure_utc``): any function bringing a temporal
      value into the domain -- from a string, from a Polars frame, from config
      -- converts it to canonical form, rejecting only the genuinely ambiguous
      (a naive value is never assumed UTC). ``from_iso8601`` is the string
      twin of this datetime-object verb.
    - **Interior and egress require** (``require_utc``): strict identity
      checks, never loosened. A strict failure in the interior means an
      ingress was missed -- the fix is adding the missing ``ensure_utc``
      boundary, never weakening the guard.

Identity rather than offset-equality is deliberate: a zero-offset foreign
tzinfo (Polars materializes ``zoneinfo.ZoneInfo('UTC')``; pydantic-core tags
its own ``TzInfo``) is the fingerprint of a value that entered the domain
without normalizing. An offset-equality check would accept it and mask the
missed ingress; the identity check finds it -- it is what caught the live
watermark-serialization crash.

Bad input raises stdlib ``ValueError``, never a ``FleetpullError`` -- a bad
temporal value is a caller bug or malformed input, and keeping the raise
stdlib is what lets ``timing`` stay a leaf below ``exceptions``, importing
nothing internal (the codec stance).
"""

from datetime import UTC, datetime

__all__: list[str] = [
    'ensure_utc',
    'require_utc',
]


def ensure_utc(moment: datetime) -> datetime:
    """
    Normalize an aware datetime to canonical UTC; reject a naive one.

    The ingress verb: converts via ``astimezone(UTC)`` so the result's tzinfo
    *is* ``datetime.UTC`` regardless of the source tag (a foreign
    ``ZoneInfo('UTC')``, a fixed offset). A naive datetime is ambiguous and is
    rejected, never assumed UTC -- the ``from_iso8601`` stance.

    Args:
        moment: The aware datetime to normalize.

    Returns:
        The same instant with ``tzinfo`` ``datetime.UTC``.

    Raises:
        ValueError: If ``moment`` is naive.
    """
    if moment.tzinfo is None:
        raise ValueError(
            'datetime must be timezone-aware to normalize; got a naive value '
            '(never assumed UTC)'
        )
    return moment.astimezone(UTC)


def require_utc(moment: datetime) -> datetime:
    """
    Validate that ``moment`` is canonical UTC; return it unchanged.

    The guard verb: the check is identity against ``datetime.UTC``. The whole
    codebase produces UTC datetimes via ``datetime.UTC`` / ``tz=UTC``, so a
    different tzinfo -- even a zero-offset one -- signals a value that did not
    pass an ingress (``ensure_utc`` / ``from_iso8601``) and is rejected rather
    than silently coerced.

    Args:
        moment: The datetime to validate.

    Returns:
        ``moment`` unchanged, once validated.

    Raises:
        ValueError: If ``moment`` is naive, or its tzinfo is not
            ``datetime.UTC``.
    """
    if moment.tzinfo is None:
        raise ValueError('datetime must be timezone-aware (UTC); got a naive value')
    if moment.tzinfo is not UTC:
        raise ValueError(
            f'datetime must use datetime.UTC; got tzinfo={moment.tzinfo!r}'
        )
    return moment
