# src/fleetpull/models/motive/shared.py
"""Motive embedded shapes shared across more than one endpoint.

This module holds the per-record building blocks that appear on multiple
Motive responses — ``UserSummary`` and ``EldDeviceInfo`` on the vehicle
and vehicle-location records (and the driving-period and idle-event
records), ``VehicleSummary`` on the driving-period and idle-event records.
Endpoint-private sub-shapes live in their endpoint module, not here; a
shape is promoted into this module only once a second endpoint actually
uses it.
"""

from typing import Annotated

from pydantic import BeforeValidator, Field

from fleetpull.model_contract import ResponseModel, empty_str_to_none

__all__: list[str] = [
    'EldDeviceInfo',
    'UserSummary',
    'VehicleSummary',
]


class UserSummary(ResponseModel):
    """Abbreviated user reference embedded in other Motive records.

    The compact user-account shape that appears when a user is referenced
    from another entity: the vehicle record's ``permanent_driver`` /
    ``current_driver``, the driving-period and idle-event ``driver``
    references, and the group record's owner ``user``. The full record
    comes from the users endpoint. Optionality is union-lax across the
    carrying surfaces (a key populated on one surface may be null on
    another -- e.g. ``username`` carries values on driver references and
    was null on all 152 group owners); each consumer's docstring pins its
    own surface's census.

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
    from an event record (captured 2026-07-15 on the driving-period and
    idle-event records). The full vehicle record comes from the vehicles
    endpoint.

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
        vin: Vehicle identification number.
        metric_units: Whether the vehicle's Motive profile reports metric.
    """

    vehicle_id: int = Field(alias='id')
    number: str
    year: Annotated[int | None, BeforeValidator(empty_str_to_none)] = None
    make: str | None = None
    model: str | None = None
    vin: str
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
