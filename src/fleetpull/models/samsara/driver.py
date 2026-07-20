# src/fleetpull/models/samsara/driver.py
"""Samsara Driver response model (``GET /fleet/drivers``).

Written from captured live responses (2026-07-20 probe session: the
full two-sweep census over all 832 records -- 460 active plus 372
deactivated, fully disjoint -- plus captured record variants), never
from docs. The census observed no null value anywhere -- Samsara omits
absent keys rather than nulling them (the vehicles posture), while
also using empty strings (``homeTerminalName`` / ``homeTerminalAddress``
are ``""`` on 204 and 268 of the 460 active records), which mirror
verbatim and normalize to null at the DataFrame boundary.

Optionality is conservative: only ``id`` is required. The per-key
presence counts were fully enumerated on the active sweep only (the
attribute docs cite them, out of 460); the deactivated sweep matched
structurally but was not per-key sworn, so unlike the vehicles model
(whose one sweep was the whole population) the always-present-in-capture
keys stay optional here rather than required.

``driverActivationStatus`` is a strict CLOSED enum, proven by the API
itself: any other value -- case variants, comma-joins, repeated keys,
bogus strings -- returns HTTP 400 naming the two admissible values
(captured 2026-07-20), so the two-member ``DriverActivationStatus``
mirror is closed by evidence, not assumption. ``dotNumber`` is a BARE
integer on the wire (not quoted). ``eldExemptReason`` is a free-text
reason string on the wire (282/460 presence, captured 2026-07-20).

Excluded fields (``extra='ignore'`` makes exclusion exactly "don't
model it"):

- ``tags`` -- a list of ``{id, name, parentTagId}`` objects (441/460);
  the records layer's schema derivation supports scalars, enums,
  ``list[scalar]``, and nested models only (the GeoTab Device/User
  exclusion precedent) -- modeled when the list-of-structs derivation
  vertical lands.
- ``eldSettings`` -- ``{rulesets: [{break, cycle, restart, shift}]}``,
  a list-of-objects block (190/460); same exclusion precedent.

``externalIds`` was NEVER observed on any of the 832 swept records --
deliberately not modeled (unobserved, not excluded); revisit only on a
capture that shows one, per the union-of-observed discipline.

Wire keys are camelCase; fields are snake_case via the ``to_camel``
alias generator.
"""

from datetime import datetime
from enum import StrEnum

from pydantic import ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel

__all__: list[str] = [
    'Driver',
    'DriverActivationStatus',
    'DriverCarrierSettings',
    'DriverHosSetting',
    'DriverStaticAssignedVehicleRef',
    'DriverTagRef',
]


class DriverActivationStatus(StrEnum):
    """Activation status for a Samsara driver.

    A closed mirror whose closure is API-enforced: every probed variant
    outside these two values returned HTTP 400 naming exactly this
    vocabulary (captured 2026-07-20), so the enum is evidence-closed.
    Kept as an enum (not downgraded to ``str``) because the wire values
    match the member values exactly -- membership validation for free on
    a faithful mirror (the Motive ``VehicleStatus`` precedent).
    """

    ACTIVE = 'active'
    DEACTIVATED = 'deactivated'


class DriverCarrierSettings(ResponseModel):
    """The ``carrierSettings`` block: carrier identity and home terminal.

    Present on every captured record, with all five keys present in
    every carrying block; the home-terminal pair frequently carries
    empty strings (204/460 and 268/460 active), mirrored verbatim.

    Attributes:
        carrier_name: The carrier's display name.
        dot_number: The carrier's USDOT number -- a BARE integer on the
            wire (captured six-digit), never quoted.
        main_office_address: The carrier's main office address.
        home_terminal_name: The driver's home terminal name; ``""`` on
            204/460 active records.
        home_terminal_address: The home terminal address; ``""`` on
            268/460 active records.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    carrier_name: str
    dot_number: int
    main_office_address: str
    home_terminal_name: str
    home_terminal_address: str


class DriverHosSetting(ResponseModel):
    """The ``hosSetting`` block: hours-of-service configuration flags.

    Attributes:
        heavy_haul_exemption_toggle_enabled: Whether the heavy-haul
            exemption toggle is enabled for the driver.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    heavy_haul_exemption_toggle_enabled: bool


class DriverStaticAssignedVehicleRef(ResponseModel):
    """The statically assigned vehicle reference: id and display name."""

    id: str
    name: str


class DriverTagRef(ResponseModel):
    """A tag reference: the shared ``{id, name, parentTagId}`` shape.

    The one shape both singular tag references (``peerGroupTag``,
    ``vehicleGroupTag``) carry -- all three keys present in every
    captured block. Deliberately NOT reused for the ``tags`` list, which
    stays excluded wholesale (module docstring).

    Attributes:
        id: The tag's id.
        name: The tag's display name.
        parent_tag_id: The parent tag's id.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str
    name: str
    parent_tag_id: str


class Driver(ResponseModel):
    """One Samsara fleet driver, active or deactivated.

    A pure mirror of the captured fields (``tags`` and ``eldSettings``
    excluded, ``externalIds`` unobserved -- module docstring). Field
    semantics are Samsara's; no value is derived or interpreted here.
    The one complete driver dataset is the union of the two activation
    sweeps; ``driver_activation_status`` carries the split.

    Presence counts below are out of the 460 active records; keys with
    no count were present on every one (the deactivated sweep matched
    structurally). Only ``id`` is required (module docstring).

    Attributes:
        id: Samsara's driver id -- a numeric string, mirrored as string.
        name: The driver's display name.
        username: The driver-app login name.
        driver_activation_status: ``active`` / ``deactivated`` -- the
            API-enforced closed enum, and the two-sweep split column.
        timezone: The driver's IANA timezone (e.g.
            ``America/Chicago``).
        created_at_time: Record creation (UTC, millisecond ISO-8601).
        updated_at_time: Last record update (UTC).
        has_vehicle_unpinning_enabled: Vehicle-unpinning flag.
        carrier_settings: Carrier identity and home terminal block.
        hos_setting: Hours-of-service configuration block.
        static_assigned_vehicle: Statically assigned vehicle reference
            (102/460).
        peer_group_tag: Peer-group tag reference (4/460).
        vehicle_group_tag: Vehicle-group tag reference (8/460).
        license_number: Driving license number (172/460).
        license_state: License issuing state (269/460).
        phone: Contact phone number (7/460).
        locale: Display locale (1/460).
        notes: Free-form notes (1/460).
        profile_image_url: Profile image URL (1/460).
        eld_exempt: ELD exemption flag (270/460).
        eld_exempt_reason: Free-text exemption reason (282/460).
        eld_adverse_weather_exemption_enabled: ELD adverse-weather
            exemption flag (191/460).
        eld_big_day_exemption_enabled: ELD big-day exemption flag
            (186/460).
        eld_pc_enabled: ELD personal-conveyance flag (77/460).
        eld_ym_enabled: ELD yard-move flag (100/460).
        waiting_time_duty_status_enabled: Waiting-time duty-status flag
            (8/460).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    # Identity and lifecycle.
    id: str
    name: str | None = None
    username: str | None = None
    driver_activation_status: DriverActivationStatus | None = None
    timezone: str | None = None
    created_at_time: datetime | None = None
    updated_at_time: datetime | None = None
    has_vehicle_unpinning_enabled: bool | None = None

    # Carrier and HOS configuration blocks.
    carrier_settings: DriverCarrierSettings | None = None
    hos_setting: DriverHosSetting | None = None

    # Assignment and grouping references.
    static_assigned_vehicle: DriverStaticAssignedVehicleRef | None = None
    peer_group_tag: DriverTagRef | None = None
    vehicle_group_tag: DriverTagRef | None = None

    # Person and contact.
    license_number: str | None = None
    license_state: str | None = None
    phone: str | None = None
    locale: str | None = None
    notes: str | None = None
    profile_image_url: str | None = None

    # ELD and duty-status flags.
    eld_exempt: bool | None = None
    eld_exempt_reason: str | None = None
    eld_adverse_weather_exemption_enabled: bool | None = None
    eld_big_day_exemption_enabled: bool | None = None
    eld_pc_enabled: bool | None = None
    eld_ym_enabled: bool | None = None
    waiting_time_duty_status_enabled: bool | None = None
