"""Motive embedded shapes shared across more than one endpoint.

This module holds the per-record building blocks that the source documents
as appearing on multiple Motive responses — ``EldDeviceInfo`` and
``DriverSummary`` both appear on the vehicle record and the vehicle-location
record. Endpoint-private sub-shapes live in their endpoint module, not here;
a shape is promoted into this module only once a second endpoint actually
uses it.
"""

from pydantic import Field

from fleetpull.model_contract import ResponseModel

__all__: list[str] = [
    'DriverSummary',
    'EldDeviceInfo',
]


class DriverSummary(ResponseModel):
    """Abbreviated driver reference embedded in other Motive records.

    The compact driver shape that appears when a driver is referenced from
    another entity (e.g. ``permanent_driver`` / ``current_driver`` on the
    vehicle record). The full driver record comes from the users endpoint.

    ``status`` and ``role`` are modeled as free-form ``str`` rather than
    constrained enums: Motive documents both as plain strings, fleetpull
    does not interpret them, and mirroring them as strings keeps the model
    faithful and evolution-safe.

    Attributes:
        driver_id: Motive's internal driver identifier (wire key ``id``).
        first_name: Driver's first name.
        last_name: Driver's last name.
        username: Login username; null when unset.
        email: Driver's email address; null when unset.
        driver_company_id: Company-assigned driver identifier; null when
            unset.
        status: Free-form account-status string; null when absent.
        role: Free-form user-role string; null when absent.
    """

    driver_id: int = Field(alias='id')
    first_name: str
    last_name: str
    username: str | None = None
    email: str | None = None
    driver_company_id: str | None = None
    status: str | None = None
    role: str | None = None


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
