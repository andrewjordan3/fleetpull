# src/fleetpull/models/geotab/device.py
"""GeoTab Device response model (JSON-RPC ``Get`` on ``typeName: Device``).

Written from captured live responses (2026-07-09 probe session), never
from docs. The captured schema is polymorphic -- one entity type spans
at least three shapes: GO7-era hardware, GO9-era hardware (a superset
with ``vinInfo*`` and continuous-connect fields), and trailer entries
(``deviceType: "None"``, ``productId: -1``, ``tmpTrailerId``, no
telematics-parameter fields at all). Two tracked GO9 records also
arrived without ``deviceFlags``/``devicePlans`` entirely, so shape
poverty is itself a shape. The model is therefore the union of observed
fields with every field optional; ``models_to_dataframe`` lands absent
fields as nulls, one typed schema across all shapes.

Excluded fields (``extra='ignore'`` makes exclusion exactly "don't
model it"):

- ``ignoreDownloadsUntil`` -- observed live at ``0001-01-01``, which
  overflows nanosecond-precision timestamp columns (captured
  2026-07-09). No other captured field carries a year-one datetime.
- ``autoGroups``, ``customParameters``, ``customProperties``,
  ``mediaFiles``, ``wifiHotspotLimits`` -- observed only as empty
  lists, so their element shape is uncaptured and cannot be honestly
  typed.
- ``groups``, ``devicePlanBillingInfo`` (lists of objects),
  ``customFeatures``, ``deviceFlags`` (dynamic-keyed / deeply nested
  objects) -- the records layer's schema derivation (DESIGN section 9)
  supports scalars, enums, ``list[scalar]``, and nested models only;
  its override hatch is still deferred, so these shapes have no honest
  column today. Model them when that hatch lands or a consumer forces
  typed access.

Sentinels are stored as-is, never transformed (interpretation is the
consumer's concern; the boundary model's job is shape): ``activeTo``
of ``2050-01-01`` means "still active" (and is ns-safe); the VIN fields
carry ``""`` and a literal ``"?"`` for unknown VINs; ``productId: -1``
marks non-telematics (trailer) entries.

Wire keys are camelCase; fields are snake_case via the ``to_camel``
alias generator rather than 72 hand-written ``Field(alias=...)`` lines
-- a typo'd hand alias would land silently as ``None`` under
``extra='ignore'``, while the generator is mechanically exact (the
model tests validate every captured shape against it).
"""

from datetime import datetime

from pydantic import ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel

__all__: list[str] = ['Device']


class Device(ResponseModel):
    """One GeoTab Device entity: vehicle-installed GO hardware or a trailer.

    A pure mirror of the union of captured fields, everything optional
    (the shape polymorphism above). Field semantics are GeoTab's; no
    value is derived or interpreted here. Groups below follow the
    captured record layout: identity and lifecycle, vehicle identity,
    fleet bookkeeping, firmware/parameter state, telematics
    configuration scalars, and the aux-channel arrays.

    Attributes:
        id: GeoTab's device id -- the seek-paging sort key (hex-suffixed
            string, ascending).
        serial_number: Hardware serial; ``""`` on trailer entries.
        name: The unit's display name.
        device_type: Hardware generation (``GO7``, ``GO9``; trailers
            carry the literal string ``"None"``).
        product_id: Numeric hardware product code; ``-1`` on trailers.
        active_from: Start of the device's active window (UTC).
        active_to: End of the active window; ``2050-01-01`` is GeoTab's
            still-active sentinel, stored as-is.
        vehicle_identification_number: Registered VIN; ``""`` and a
            literal ``"?"`` are captured unknown-VIN sentinels.
        engine_vehicle_identification_number: ECM-reported VIN; same
            sentinels.
        tmp_trailer_id: Trailer-entry identifier; absent on tracked
            vehicles.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    # Identity and lifecycle.
    id: str | None = None
    serial_number: str | None = None
    name: str | None = None
    device_type: str | None = None
    product_id: int | None = None
    active_from: datetime | None = None
    active_to: datetime | None = None
    comment: str | None = None
    tmp_trailer_id: str | None = None

    # Vehicle identity (VIN sentinels: "" and a literal "?").
    vehicle_identification_number: str | None = None
    engine_vehicle_identification_number: str | None = None
    vin_info_make: str | None = None
    vin_info_model: str | None = None
    vin_info_vehicle_type: int | None = None
    vin_info_year: str | None = None
    license_plate: str | None = None
    license_state: str | None = None

    # Fleet bookkeeping.
    device_plans: list[str] | None = None
    time_zone_id: str | None = None
    work_time: str | None = None
    auto_hos: str | None = None
    go_talk_language: str | None = None
    pin_device: bool | None = None

    # Firmware / parameter state.
    major: int | None = None
    minor: int | None = None
    parameter_version: int | None = None
    parameter_version_on_device: int | None = None
    enable_must_reprogram: bool | None = None
    time_to_download: str | None = None

    # Telematics configuration scalars.
    acceleration_warning_threshold: int | None = None
    accelerometer_threshold_warning_factor: int | None = None
    braking_warning_threshold: int | None = None
    cornering_warning_threshold: int | None = None
    communication_threshold_interval_moving: int | None = None
    communication_threshold_interval_stationary: int | None = None
    disable_buzzer: bool | None = None
    disable_sleeper_berth: bool | None = None
    enable_beep_on_dangerous_driving: bool | None = None
    enable_beep_on_idle: bool | None = None
    enable_beep_on_rpm: bool | None = None
    enable_control_external_relay: bool | None = None
    enable_speed_warning: bool | None = None
    engine_hour_offset: int | None = None
    engine_type: str | None = None
    ensure_hot_start: bool | None = None
    external_device_shut_down_delay: int | None = None
    force_active_tracking: bool | None = None
    fuel_tank_capacity: int | None = None
    gps_off_delay: int | None = None
    idle_minutes: int | None = None
    immobilize_arming: int | None = None
    immobilize_unit: bool | None = None
    is_active_tracking_enabled: bool | None = None
    is_continuous_connect_enabled: bool | None = None
    is_driver_seatbelt_warning_on: bool | None = None
    is_iox_connection_enabled: bool | None = None
    is_passenger_seatbelt_warning_on: bool | None = None
    is_reverse_detect_on: bool | None = None
    is_speed_indicator: bool | None = None
    max_seconds_between_logs: int | None = None
    min_accident_speed: int | None = None
    obd_alert_enabled: bool | None = None
    odometer_factor: float | None = None
    odometer_offset: float | None = None
    rpm_value: int | None = None
    seatbelt_warning_speed: int | None = None
    speeding_off: int | None = None
    speeding_on: int | None = None

    # Aux-channel arrays (fixed-width per hardware generation).
    aux_warning_speed: list[int] | None = None
    enable_aux_warning: list[bool] | None = None
    is_aux_ign_trigger: list[bool] | None = None
    is_aux_inverted: list[bool] | None = None
