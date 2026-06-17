"""Motive vehicles-endpoint response model (``/v1/vehicles``).

Holds the ``Vehicle`` record and the shapes used only by it —
``AvailabilityDetails`` and the ``VehicleStatus`` / ``AvailabilityStatus``
enums. Cross-endpoint embedded shapes (``DriverSummary``, ``EldDeviceInfo``)
are imported from ``fleetpull.models.motive.shared``.

Pure API mirrors — typed fields and nothing else. No use-case logic, no
derived properties, no normalizing validators: flattening and schema
derivation are the records layer's generic concern (DESIGN §9), and
fleetpull assumes no end use, so a field is mirrored, never interpreted.
The response *wrapper* (the ``{"vehicles": [{"vehicle": {...}}]}`` envelope)
is not modeled here — the endpoints layer's extractor and paginator own it,
so this module mirrors only the inner per-vehicle object.
"""

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import Field

from fleetpull.model_contract import ResponseModel
from fleetpull.models.motive.shared import DriverSummary, EldDeviceInfo

__all__: list[str] = [
    'AvailabilityDetails',
    'AvailabilityStatus',
    'Vehicle',
    'VehicleStatus',
]


class VehicleStatus(StrEnum):
    """Operational status for a Motive vehicle.

    A closed mirror of Motive's documented status vocabulary. Kept as an
    enum (not downgraded to ``str``) because the wire values match the
    member values exactly — no normalizing logic is needed to land them,
    so the enum stays a faithful mirror while adding documentation and
    membership validation. Live responses show ``active`` and
    ``deactivated``; ``inactive`` is documented.
    """

    ACTIVE = 'active'
    INACTIVE = 'inactive'
    DEACTIVATED = 'deactivated'


class AvailabilityStatus(StrEnum):
    """In-service / out-of-service availability for a Motive vehicle.

    Closed mirror of Motive's documented availability vocabulary; an exact
    wire-to-member match, so it stays an enum for the same reason as
    ``VehicleStatus``.
    """

    IN_SERVICE = 'in_service'
    OUT_OF_SERVICE = 'out_of_service'


class AvailabilityDetails(ResponseModel):
    """Availability status block embedded in the vehicle record.

    Tracks whether a vehicle is in or out of service and when the status
    last changed.

    Attributes:
        availability_status: Current in-service / out-of-service value.
        updated_at: Timestamp of the last status change.
        updated_by_user: The user who set the status; null when system-set.
            Typed ``Any`` because Motive returns an unconstrained, rarely
            populated nested object here that fleetpull does not interpret;
            a single justified ``Any`` is preferred over ``dict[str, Any]``
            (type discipline) and over importing a JSON value alias (the
            import boundary forbids that for response models).
    """

    availability_status: AvailabilityStatus
    updated_at: datetime
    updated_by_user: Any = None  # free-form nested object; see docstring


class Vehicle(ResponseModel):
    """Complete vehicle record from Motive's vehicles endpoint.

    A single fleet vehicle with its metadata, current and permanent driver
    references, ELD device, and availability block. A pure mirror: every
    field maps a Motive response field, with no derived or interpreted
    values.

    Attributes:
        vehicle_id: Motive's internal vehicle identifier (wire key ``id``).
        company_id: Parent company identifier.
        number: User-assigned fleet/unit number.
        status: Vehicle operational status.
        ifta: Whether the vehicle is IFTA-reportable.
        vin: Vehicle Identification Number; null when unset.
        make: Manufacturer; null when unset.
        model: Model name; null when unset.
        year: Model year, mirrored as a string (Motive's convention); null
            when unset.
        license_plate_state: Registration state/province; null when unset.
        license_plate_number: License plate number; null when unset.
        license_plate_country_code: Registration country code; null when
            unset. Present in live responses but absent from the predecessor
            model — added here so the mirror is faithful to the wire.
        metric_units: Whether the vehicle displays metric units.
        fuel_type: Primary fuel type, mirrored as a free-form ``str``
            rather than a constrained enum — Motive's casing varies and
            fleetpull does not interpret the value, so a string mirror
            avoids carrying a normalizing validator on an otherwise pure
            model; null when unset.
        prevent_auto_odometer_entry: Whether automatic odometer capture is
            disabled.
        notes: Free-form notes; null when unset.
        incab_alert_live_stream_enable: Motive camera-config field; ``-1``
            is Motive's "not configured" sentinel and the default.
        driver_facing_camera: Camera-config field; ``-1`` sentinel default.
        incab_audio_recording: Camera-config field; ``-1`` sentinel default.
        group_ids: Identifiers of the groups the vehicle belongs to.
        created_at: When the vehicle was added to Motive.
        updated_at: Last modification timestamp.
        permanent_driver: Permanently assigned driver; null when none.
        availability_details: Current availability block; null when absent.
        eld_device: Installed ELD hardware; null when none.
        current_driver: Currently logged-in driver; null when none.
        external_ids: External-system identifier objects. Typed
            ``list[Any]`` because the element is an unconstrained,
            integration-specific object fleetpull does not interpret; a
            justified ``Any`` element is preferred over ``dict[str, Any]``
            and over a JSON value alias, as for
            ``AvailabilityDetails.updated_by_user``.
        carb_ctc_test_enabled: CARB compliance flag; null when absent.
        carb_ctc_emission_status: CARB emission status string; null when
            absent.
        registration_expiry_date: Registration expiry, mirrored as a string
            (Motive's convention); null when absent.
    """

    vehicle_id: int = Field(alias='id')
    company_id: int
    number: str
    status: VehicleStatus
    ifta: bool
    vin: str | None = None
    make: str | None = None
    model: str | None = None
    year: str | None = None
    license_plate_state: str | None = None
    license_plate_number: str | None = None
    license_plate_country_code: str | None = None
    metric_units: bool = False
    fuel_type: str | None = None
    prevent_auto_odometer_entry: bool = False
    notes: str | None = None

    # Motive uses -1 as a "not configured" sentinel for these camera ints.
    incab_alert_live_stream_enable: int = -1
    driver_facing_camera: int = -1
    incab_audio_recording: int = -1

    group_ids: list[int] = Field(default_factory=list)

    created_at: datetime
    updated_at: datetime

    permanent_driver: DriverSummary | None = None
    availability_details: AvailabilityDetails | None = None
    eld_device: EldDeviceInfo | None = None
    current_driver: DriverSummary | None = None

    external_ids: list[Any] = Field(default_factory=list)  # see docstring

    carb_ctc_test_enabled: bool | None = None
    carb_ctc_emission_status: str | None = None
    registration_expiry_date: str | None = None
