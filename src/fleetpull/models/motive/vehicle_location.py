# src/fleetpull/models/motive/vehicle_location.py
"""Motive vehicle-locations-endpoint response model (``/v3/vehicle_locations``).

Holds the ``VehicleLocation`` breadcrumb record and the ``VehicleLocationType``
enum used only by it. Cross-endpoint embedded shapes (``UserSummary``,
``EldDeviceInfo``) are imported from ``fleetpull.models.motive.shared``.

Pure API mirrors — typed fields and nothing else. No use-case logic, no derived
properties, no normalizing validators: flattening and schema derivation are the
records layer's generic concern (DESIGN §9), and fleetpull assumes no end use, so a
field is mirrored, never interpreted. The response *wrapper* (the
``{"vehicle_locations": [{"vehicle_location": {...}}]}`` envelope) is not modeled
here — the endpoints layer's decoder owns it, so this module mirrors only the inner
per-location object.
"""

from datetime import datetime
from enum import StrEnum

from pydantic import Field

from fleetpull.model_contract import ResponseModel
from fleetpull.models.motive.shared import EldDeviceInfo, UserSummary

__all__: list[str] = [
    'VehicleLocation',
    'VehicleLocationType',
]


class VehicleLocationType(StrEnum):
    """Type of a Motive vehicle-location (breadcrumb) record.

    A closed mirror of Motive's documented location-type vocabulary. Kept as an
    enum (not downgraded to ``str``) for the same reason as ``VehicleStatus``: the
    wire values match the member values exactly, so the enum is a faithful mirror
    that adds documentation and membership validation with no normalizing logic.
    The blast radius differs from ``VehicleStatus`` — this endpoint is high-volume
    per-vehicle breadcrumbs, so a type Motive begins emitting that is not a member
    fails validation loudly across every affected row rather than a handful of
    vehicles. That is fail-fast-and-loud as intended (extend the member set,
    re-fetch); it is also the field most likely to want ``str`` instead if a silent
    land-as-data is ever preferred on this surface.
    """

    BREADCRUMB = 'breadcrumb'
    VEHICLE_STOPPED = 'vehicle_stopped'
    VEHICLE_MOVING = 'vehicle_moving'
    IGNITION_ON = 'ignition_on'
    IGNITION_OFF = 'ignition_off'
    ENGINE_START = 'engine_start'
    ENGINE_STOP = 'engine_stop'
    GPS_MOVING = 'gps_moving'
    GPS_STOPPED = 'gps_stopped'


class VehicleLocation(ResponseModel):
    """A single vehicle-location (breadcrumb) record from Motive.

    One point-in-time fix of a vehicle's position and telemetry from the
    ``/v3/vehicle_locations/{vehicle_id}`` endpoint, which returns a vehicle's
    location history over a date range. A pure mirror: every field maps a Motive
    response field, with no derived or interpreted values.

    Attributes:
        location_id: Motive's identifier for this location record, a UUID string
            (wire key ``id``).
        located_at: Timestamp the location was recorded -- the endpoint's event
            time.
        latitude: GPS latitude in decimal degrees (wire key ``lat``).
        longitude: GPS longitude in decimal degrees (wire key ``lon``).
        location_type: The kind of location event (wire key ``type``).
        description: Human-readable place description (e.g. city, state); null
            when absent.
        speed: Vehicle speed in the vehicle's own units; null when absent.
        bearing: Compass heading in degrees; null when absent.
        battery_voltage: Vehicle battery voltage; null when absent.
        odometer: Calculated odometer reading; null when absent.
        true_odometer: ECM-reported odometer; null when absent.
        engine_hours: Calculated engine-hours reading; null when absent.
        true_engine_hours: ECM-reported engine hours; null when absent.
        fuel: Cumulative fuel consumption; null when absent.
        fuel_primary_remaining_percentage: Primary tank level, 0-100; null when
            absent.
        fuel_secondary_remaining_percentage: Secondary tank level, 0-100; null
            when absent.
        veh_range: Estimated remaining range (EV); null for non-EV.
        hvb_state_of_charge: High-voltage-battery charge state (EV); null for
            non-EV.
        hvb_charge_status: High-voltage-battery charge status string (EV); null
            for non-EV.
        hvb_charge_source: High-voltage-battery charge source string (EV); null
            for non-EV.
        hvb_lifetime_energy_output: High-voltage-battery lifetime energy output
            (EV); null for non-EV.
        driver: Driver logged in at capture time; null when none.
        eld_device: ELD device that captured the location; null when none.
    """

    location_id: str = Field(alias='id')
    located_at: datetime
    latitude: float = Field(alias='lat')
    longitude: float = Field(alias='lon')
    location_type: VehicleLocationType = Field(alias='type')
    description: str | None = None

    speed: float | None = None
    bearing: float | None = None

    battery_voltage: float | None = None
    odometer: float | None = None
    true_odometer: float | None = None
    engine_hours: float | None = None
    true_engine_hours: float | None = None

    fuel: float | None = None
    fuel_primary_remaining_percentage: float | None = None
    fuel_secondary_remaining_percentage: float | None = None

    veh_range: float | None = None
    hvb_state_of_charge: float | None = None
    hvb_charge_status: str | None = None
    hvb_charge_source: str | None = None
    hvb_lifetime_energy_output: float | None = None

    driver: UserSummary | None = None
    eld_device: EldDeviceInfo | None = None
