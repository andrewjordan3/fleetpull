# src/fleetpull/timing/codec.py
"""Pure conversions between UTC datetimes and their wire/storage string forms.

A leaf: stdlib plus the sibling ``canon`` module, nothing internal beyond
this package. Converts our own outbound and persisted values — request
parameters and DateWatermark state — between ``datetime`` and string. It
never parses a provider response body; response timestamps are cast
vectorized in the records layer.

UTC discipline, enforced both directions (the canonical-UTC doctrine,
``canon.py``):
    - Encoders reject a naive or non-UTC datetime via ``require_utc`` — a
      non-UTC value reaching here missed an ingress and is a bug, failed
      loud.
    - ``from_iso8601`` normalizes any offset to UTC and rejects a naive
      result — an ISO string with no offset is ambiguous, never assumed UTC.
      It is the string-ingress twin of ``canon.ensure_utc``.

Because every internal timestamp is UTC end to end, DST and ambiguous-local
bugs are structurally impossible.

Bad input raises stdlib ``ValueError`` — a bad value is a caller bug or
malformed input, never a ``FleetpullError``. The consuming boundary
translates when it wants a typed failure (the config loader maps a user's
bad date to ``ConfigurationError``). Keeping the raise stdlib is what lets
``timing`` import nothing internal.

The epoch encoders (``to_unix_seconds`` / ``to_unix_millis``) are
deliberately absent: their only consumers are Samsara/Motive endpoint params
whose exact format is unverified, settled at the endpoint layer against the
predecessor rather than guessed here. The module grows a function when such
an endpoint lands.
"""

from datetime import UTC, datetime

from fleetpull.timing.canon import require_utc

__all__: list[str] = [
    'from_iso8601',
    'to_iso8601',
    'to_utc_date_string',
]


def to_iso8601(moment: datetime) -> str:
    """
    Render a UTC datetime as a seconds-precision ISO-8601 'Z' string.

    The form GeoTab's ``fromDate`` and DateWatermark persistence use, e.g.
    ``'2026-06-01T00:00:00Z'``. Sub-second precision is dropped: window
    parameters are second-granular and persisted watermarks read cleaner
    without it.

    Args:
        moment: A timezone-aware UTC datetime.

    Returns:
        The ISO-8601 string with a trailing ``Z``.

    Raises:
        ValueError: If ``moment`` is naive or not UTC.
    """
    validated_moment: datetime = require_utc(moment)
    # isoformat emits '+00:00' for a UTC datetime; swap for the 'Z' form
    # providers expect. The offset is the only '+00:00' a validated-UTC ISO
    # string can contain, so the suffix strip is unambiguous.
    iso_with_offset: str = validated_moment.isoformat(timespec='seconds')
    return iso_with_offset.removesuffix('+00:00') + 'Z'


def to_utc_date_string(moment: datetime) -> str:
    """
    Render the UTC calendar date of a datetime as ``'YYYY-MM-DD'``.

    Used for hive partition keys (``date=YYYY-MM-DD``) and date-only request
    parameters. The date is the UTC date, since ``moment`` is UTC.

    Args:
        moment: A timezone-aware UTC datetime.

    Returns:
        The UTC date in ISO ``'YYYY-MM-DD'`` form.

    Raises:
        ValueError: If ``moment`` is naive or not UTC.
    """
    validated_moment: datetime = require_utc(moment)
    return validated_moment.date().isoformat()


def from_iso8601(text: str) -> datetime:
    """
    Parse an ISO-8601 datetime string into a timezone-aware UTC datetime.

    Accepts any offset form ``datetime.fromisoformat`` handles on 3.12 (``Z``,
    ``+HH:MM``, fractional seconds) and normalizes it to UTC. A string with no
    offset — including a date-only string — is ambiguous and rejected; this
    module never assumes UTC for an unzoned value.

    Consumers: reading a persisted DateWatermark back, and parsing a
    user-supplied date from YAML config. The config path is untrusted, so a
    malformed value raises here and the config loader translates it.

    Args:
        text: An ISO-8601 datetime string carrying a UTC offset.

    Returns:
        The instant as a timezone-aware UTC datetime (tzinfo is
        ``datetime.UTC``).

    Raises:
        ValueError: If ``text`` is not parseable ISO-8601, or carries no
            offset (naive).
    """
    parsed_moment: datetime = datetime.fromisoformat(text)
    if parsed_moment.tzinfo is None:
        raise ValueError(
            f'ISO-8601 datetime must carry a UTC offset; got naive {text!r}'
        )
    return parsed_moment.astimezone(UTC)
