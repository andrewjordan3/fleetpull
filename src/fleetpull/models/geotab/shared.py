# src/fleetpull/models/geotab/shared.py
"""Shared GeoTab boundary-model machinery: TimeSpan parsing, reference coercion.

GeoTab serializes every duration as a .NET TimeSpan string
(``[d.]hh:mm:ss[.f{1,7}]`` -- captured 2026-07-13: ``"00:05:01"``,
``"00:03:42.3630000"``, ``"4.16:41:16"``, ``"21:04:17"``), and reference
fields may arrive as either a bare known-id sentinel string
(``"UnknownDriverId"``) or an object (``{"id": ..., "isDriver": true}``).
Both shapes are structural wire facts shared across GeoTab entities
(``Trip`` today; ``ExceptionEvent``'s ``driver``/``diagnostic`` next),
so their coercions live here beside each other, consumed through
``Annotated`` field aliases -- never as per-model parsing logic.

``GeotabTimeSpan`` deliberately bakes nullability into the alias
(``Annotated[timedelta | None, ...]`` rather than
``Annotated[timedelta, ...] | None``): Pydantic lifts ``Annotated``
metadata into ``FieldInfo`` only when it is the annotation's top level,
so this form is the one where the records field walk sees the bare
``timedelta | None`` leaf it derives a ``Duration`` column from --
union-nesting the ``Annotated`` would hide the leaf inside metadata the
walk rejects. Every union-of-observed-fields model wants the
nullability anyway.
"""

import re
from datetime import timedelta
from typing import Annotated, Final

from pydantic import BeforeValidator

from fleetpull.vocabulary import JsonValue

__all__: list[str] = [
    'GeotabTimeSpan',
    'bare_id_to_reference',
    'parse_timespan',
]

# The .NET TimeSpan grammar: optional day prefix, exactly-two-digit
# fields, 1-7 fractional digits (100 ns ticks). Range checks (hh <= 23,
# mm/ss <= 59) are numeric, below -- a regex range would misread 29.
_TIMESPAN_PATTERN: Final[re.Pattern[str]] = re.compile(
    r'^(?:(?P<days>\d+)\.)?'
    r'(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d{2})'
    r'(?:\.(?P<ticks>\d{1,7}))?$'
)

_MAX_HOURS: Final[int] = 23
_MAX_MINUTES: Final[int] = 59
_MAX_SECONDS: Final[int] = 59

# One microsecond is ten 100 ns ticks; a 7-digit fraction is a full
# tick count, shorter fractions right-pad to it.
_TICK_DIGITS: Final[int] = 7
_TICKS_PER_MICROSECOND: Final[int] = 10


def parse_timespan(value: str) -> timedelta:
    """Parse a .NET TimeSpan string into a ``timedelta``.

    Accepts exactly the grammar ``[d.]hh:mm:ss[.f{1,7}]`` -- a
    non-negative day count, two-digit hours 00-23, two-digit minutes and
    seconds 00-59, and 1-7 fractional digits of decimal seconds (100 ns
    ticks, truncated to microseconds; every captured seventh digit is
    zero, so no observed value loses precision). Anything else --
    negative spans, malformed shapes, empty strings -- fails loudly: a
    duration this parser has never seen should fail validation, not
    pass mangled.

    Args:
        value: The wire TimeSpan string.

    Returns:
        The equivalent ``timedelta``.

    Raises:
        ValueError: ``value`` does not match the grammar or a field is
            out of range; the message names the offending string.
    """
    match = _TIMESPAN_PATTERN.match(value)
    if match is None:
        raise ValueError(f'not a .NET TimeSpan string: {value!r}')
    hours: int = int(match['hours'])
    minutes: int = int(match['minutes'])
    seconds: int = int(match['seconds'])
    if hours > _MAX_HOURS or minutes > _MAX_MINUTES or seconds > _MAX_SECONDS:
        raise ValueError(f'TimeSpan field out of range: {value!r}')
    ticks_text: str | None = match['ticks']
    microseconds: int = (
        int(ticks_text.ljust(_TICK_DIGITS, '0')) // _TICKS_PER_MICROSECOND
        if ticks_text is not None
        else 0
    )
    return timedelta(
        days=int(match['days'] or 0),
        hours=hours,
        minutes=minutes,
        seconds=seconds,
        microseconds=microseconds,
    )


def _coerce_timespan(value: JsonValue | timedelta) -> timedelta | None:
    """The ``GeotabTimeSpan`` ingress: parse strings, pass parsed values.

    Args:
        value: The raw wire value, or an already-validated value on a
            Pydantic revalidation path.

    Returns:
        ``None`` for ``None`` (the alias is nullable), a ``timedelta``
        passthrough (idempotent validation), or the parsed string.

    Raises:
        ValueError: A string that is not a TimeSpan, or any other type.
    """
    if value is None:
        return None
    if isinstance(value, timedelta):
        return value
    if isinstance(value, str):
        return parse_timespan(value)
    raise ValueError(f'expected a .NET TimeSpan string, got {type(value).__name__}')


# The duration field type every GeoTab model uses. Plain assignment, not
# a `type` statement: Pydantic must evaluate the Annotated form eagerly
# for the metadata lift the module docstring describes.
GeotabTimeSpan = Annotated[timedelta | None, BeforeValidator(_coerce_timespan)]


def bare_id_to_reference(value: JsonValue) -> JsonValue:
    """Lift a bare reference-id string into the object form.

    GeoTab reference fields carry either an object (``{"id": ...}``,
    possibly with siblings) or a bare known-id sentinel string
    (``"UnknownDriverId"``). This coercion is structural and
    sentinel-agnostic: ANY bare string becomes ``{"id": <string>}``,
    the string preserved verbatim, so the sentinel lands as the
    reference's id and the object form passes through untouched.

    Args:
        value: The raw wire value of a reference field.

    Returns:
        ``{'id': value}`` for a bare string; ``value`` unchanged
        otherwise (objects validate against the reference model, and
        anything else fails there, loudly).
    """
    if isinstance(value, str):
        return {'id': value}
    return value
