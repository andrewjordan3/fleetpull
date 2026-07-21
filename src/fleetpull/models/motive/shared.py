# src/fleetpull/models/motive/shared.py
"""Motive embedded shapes shared across more than one endpoint.

This module holds the per-record building blocks that appear on multiple
Motive responses — ``UserSummary`` and ``EldDeviceInfo`` on the vehicle
and vehicle-location records (and the driving-period and idle-event
records), ``VehicleSummary`` on the driving-period, idle-event, and
vehicle-utilization records — plus ``MotiveWindowStamp``, the
decoder-synthesized window-identity type the utilization rollup pair's
models share. Endpoint-private sub-shapes live in their endpoint module,
not here; a shape is promoted into this module only once a second
endpoint actually uses it.
"""

import re
from datetime import UTC, date, datetime, time
from typing import Annotated, Final

from pydantic import BeforeValidator, Field

from fleetpull.model_contract import ResponseModel, empty_str_to_none
from fleetpull.vocabulary import JsonValue

__all__: list[str] = [
    'EldDeviceInfo',
    'MotiveWindowStamp',
    'UserSummary',
    'VehicleSummary',
]


# Exactly the dashed calendar-date label the builders render; fullmatch
# keeps date.fromisoformat's laxer forms (compact YYYYMMDD, ISO week
# dates) failing loudly as the wiring drift they would be.
_DATE_LABEL_PATTERN: Final[re.Pattern[str]] = re.compile(r'\d{4}-\d{2}-\d{2}')


def _date_label_to_utc_midnight(value: JsonValue | datetime) -> datetime:
    """The ``MotiveWindowStamp`` ingress: lift a date label to an instant.

    The Motive window-report decoder stamps each rollup row with the
    sent spec's ``start_date``/``end_date`` values VERBATIM — day-only
    ``YYYY-MM-DD`` labels, never instants. A date label cannot validate
    into the timezone-aware datetime the event-time machinery requires
    (an unzoned event time is never assumed), so this lift is the
    structural type recovery DESIGN section 9 allows on a mirror: the
    calendar-day label is preserved exactly (the result's ``.date()`` IS
    the label) and UTC midnight is attached as the label's canonical
    instant representation — a labeling convention for partition
    routing, never a timezone conversion of the data. Strict by design:
    the builder only ever renders date labels, so any other string —
    including a full RFC3339 datetime — is a wiring drift that should
    fail validation loudly, not pass mangled.

    Args:
        value: The raw stamp value, or an already-recovered datetime on
            a Pydantic revalidation path.

    Returns:
        The label's UTC-midnight instant (passthrough for an
        already-recovered tz-aware value).

    Raises:
        ValueError: ``value`` is not exactly a dashed ``YYYY-MM-DD``
            label (the laxer ``date.fromisoformat`` forms -- compact
            digits, week dates -- reject too), or is a NAIVE datetime.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError(
                'window stamp datetime is naive -- an unzoned event time '
                'is never assumed'
            )
        return value
    if isinstance(value, str) and _DATE_LABEL_PATTERN.fullmatch(value):
        return datetime.combine(date.fromisoformat(value), time.min, tzinfo=UTC)
    raise ValueError(f'expected a YYYY-MM-DD window-stamp label, got {value!r}')


# The window-stamp field type the utilization rollup models use. Plain
# assignment, not a `type` statement: Pydantic must evaluate the
# Annotated form eagerly for the metadata lift (the GeotabTimeSpan
# precedent).
MotiveWindowStamp = Annotated[datetime, BeforeValidator(_date_label_to_utc_midnight)]


class UserSummary(ResponseModel):
    """Abbreviated user reference embedded in other Motive records.

    The compact user-account shape that appears when a user is referenced
    from another entity: the vehicle record's ``permanent_driver`` /
    ``current_driver``, the driving-period and idle-event ``driver``
    references, the group record's owner ``user``, and the
    driver-idle-rollup ``driver`` reference (the fourth carrying
    surface, captured 2026-07-21: the exact 8-key shape, populated on
    every attributed rollup and null on the unattributed bucket row).
    The full record comes from the users endpoint. Optionality is
    union-lax across the carrying surfaces (a key populated on one
    surface may be null on another -- e.g. ``username`` carries values
    on driver references and was null on all 152 group owners); each
    consumer's docstring pins its own surface's census.

    ``status`` and ``role`` are modeled as free-form ``str`` rather than
    constrained enums: Motive documents both as plain strings, fleetpull
    does not interpret them, and mirroring them as strings keeps the model
    faithful and evolution-safe.

    Attributes:
        user_id: Motive's internal user identifier (wire key ``id``).
        first_name: Driver's first name.
        last_name: Driver's last name.
        username: Login username; null when unset.
        email: Driver's email address; null when unset.
        driver_company_id: Company-assigned driver identifier; null when
            unset.
        status: Free-form account-status string; null when absent.
        role: Free-form user-role string; null when absent.
    """

    user_id: int = Field(alias='id')
    first_name: str
    last_name: str
    username: str | None = None
    email: str | None = None
    driver_company_id: str | None = None
    status: str | None = None
    role: str | None = None


class VehicleSummary(ResponseModel):
    """Abbreviated vehicle reference embedded in other Motive records.

    The compact vehicle shape that appears when a vehicle is referenced
    from another record: the driving-period and idle-event records
    (captured 2026-07-15) and the vehicle-utilization rollup record
    (captured 2026-07-21 -- the same seven wire keys exactly, its third
    carrying surface). The full vehicle record comes from the vehicles
    endpoint. Optionality is union-lax across the carrying surfaces
    (the UserSummary posture): ``vin`` carried values on every event
    record but is null on some utilization rows, so it is nullable
    here; each consumer's docstring pins its own surface's census.

    ``year`` arrives as a quoted integer (``"2022"``); lax coercion types
    it, and the captured ``"0"`` not-configured sentinel mirrors as
    ``0``, never interpreted. The empty-string wire shape (live-observed
    2026-07-16, failing ``int_parsing``) is lifted by a before-validator
    — the type-recovery case DESIGN section 9 allows on a mirror, since
    ``""`` cannot validate as an integer at all. ``make`` and ``model``
    arrive as empty strings where the provider has no value and mirror
    verbatim: empty strings normalize to null at the DataFrame boundary,
    never on a string field of the model.

    Attributes:
        vehicle_id: Motive's internal vehicle identifier (wire key ``id``).
        number: Company-assigned unit number.
        year: Model year; ``0`` is the provider's not-configured
            sentinel; null when the provider sends an empty string or
            nothing.
        make: Manufacturer; the captured empty string mirrors verbatim;
            null when absent.
        model: Model name; the captured empty string mirrors verbatim;
            null when absent.
        vin: Vehicle identification number; null where the provider has
            none (observed on the vehicle-utilization surface).
        metric_units: Whether the vehicle's Motive profile reports metric.
    """

    vehicle_id: int = Field(alias='id')
    number: str
    year: Annotated[int | None, BeforeValidator(empty_str_to_none)] = None
    make: str | None = None
    model: str | None = None
    vin: str | None = None
    metric_units: bool


class EldDeviceInfo(ResponseModel):
    """ELD hardware embedded in other Motive records.

    Identifies the physical telematics device installed in a vehicle. The
    source documents this shape on both the vehicle record and the
    vehicle-location record.

    Attributes:
        device_id: Motive's internal device identifier (wire key ``id``).
        identifier: Device serial / hardware identifier.
        model: Device model name (e.g. the Motive LBB designation).
    """

    device_id: int = Field(alias='id')
    identifier: str
    model: str
