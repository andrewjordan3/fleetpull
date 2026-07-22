# src/fleetpull/models/geotab/shared.py
"""Shared GeoTab boundary-model machinery: TimeSpan parsing, reference
coercion, and the nested-location model trio.

GeoTab serializes every duration as a .NET TimeSpan string
(``[d.]hh:mm:ss[.f{1,7}]`` -- captured 2026-07-13: ``"00:05:01"``,
``"00:03:42.3630000"``, ``"4.16:41:16"``, ``"21:04:17"``), and reference
fields may arrive as either a bare known-id sentinel string
(``"UnknownDriverId"``) or an object (``{"id": ..., "isDriver": true}``).
Both shapes are structural wire facts shared across many GeoTab entities
(any model with a duration or a reference field), so their coercions
live here beside each other, consumed through ``Annotated`` field
aliases -- never as per-model parsing logic. The consumer set is not
enumerated here: it grows with every ported entity, so the list of
importers (``grep`` for ``bare_id_to_reference`` / ``GeotabTimeSpan``)
is the source of truth, not a snapshot that goes stale.

The nested-location trio (``GeotabAddressedLocation`` wrapping an
optional ``GeotabCoordinate`` and an optional ``GeotabPostalAddress``)
is the third shared shape, consumed on ``DutyStatusLog`` and ``DVIRLog``
-- two consumers at birth, so it lives here (the second-consumer
threshold) rather than per-model. The wrapper carries the DOUBLE-NESTED
``{location: {x, y}}`` COORDINATE arm OR an ``{address:
{formattedAddress}}`` arm: the 2026-07-21 feed-wave-two census (nested
blocks sampled at 200) saw only the coordinate arm, but a 24,860-block
LIVE-PROOF walk (2026-07-21) found the wrapper carries the coordinate
arm on 24,846 blocks and the address arm on 14 (mutually exclusive at
that scale) -- the fourth time an at-scale walk found an arm a bounded
census missed (the ``StatusData.controller`` lesson). Both arms are
optional on the wrapper; a wrapper with neither is unobserved but
representable.

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

from pydantic import BeforeValidator, Field

from fleetpull.model_contract import ResponseModel
from fleetpull.vocabulary import JsonValue

__all__: list[str] = [
    'GeotabAddressedLocation',
    'GeotabCoordinate',
    'GeotabPostalAddress',
    'GeotabTimeSpan',
    'bare_id_to_reference',
    'parse_timespan',
]


class GeotabCoordinate(ResponseModel):
    """The inner coordinate block of a nested GeoTab location.

    GeoTab's ``x`` is LONGITUDE and ``y`` is LATITUDE (the provider's
    map-plane convention, consistent with the shipped ``FillUp``
    location); both arrive as floats (bare-int arms lift losslessly
    under lax coercion). Required within the block: a coordinate block
    without its coordinates is a shape change and must fail loudly.
    """

    x: float
    y: float


class GeotabPostalAddress(ResponseModel):
    """The inner address block of a nested GeoTab location.

    The wrapper's address arm, observed only on ``DutyStatusLog`` in
    the 24,860-block live-proof walk (14 blocks). Only
    ``formattedAddress`` was observed on the block; other GeoTab
    address keys (city, state, ...) are absorbed by ``extra='ignore'``
    until a walk observes them. Required within the block on the same
    loud-failure logic as the coordinates: a present address block
    missing its one observed key is a shape change.
    """

    formatted_address: str = Field(alias='formattedAddress')


class GeotabAddressedLocation(ResponseModel):
    """The nested GeoTab location wrapper: a coordinate arm or an address arm.

    Carries the double-nested ``{location: {x, y}}`` coordinate block
    (``location``) OR the ``{address: {formattedAddress}}`` block
    (``address``) -- both optional, mutually exclusive at the observed
    scale (module docstring: the live-proof walk found 24,846
    coordinate arms and 14 address arms, none carrying both). The
    consuming models (``DutyStatusLog``, ``DVIRLog``) carry the wrapper
    itself as an optional field.
    """

    location: GeotabCoordinate | None = None
    address: GeotabPostalAddress | None = None


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
    hours = int(match['hours'])
    minutes = int(match['minutes'])
    seconds = int(match['seconds'])
    if hours > _MAX_HOURS or minutes > _MAX_MINUTES or seconds > _MAX_SECONDS:
        raise ValueError(f'TimeSpan field out of range: {value!r}')
    ticks_text = match['ticks']
    microseconds = (
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
