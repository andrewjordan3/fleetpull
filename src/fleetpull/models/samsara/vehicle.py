# src/fleetpull/models/samsara/vehicle.py
"""Samsara Vehicle response model (``GET /fleet/vehicles``).

Written from captured live responses (2026-07-17 probe session: a
full-population sweep of all 608 records plus six captured record
variants), never from docs. The sweep observed no null value and no
type variance on any of the 20 observed keys -- Samsara omits absent
keys rather than nulling them (the GeoTab posture), while also using
empty strings (``notes: ""`` on every captured record), which mirror
verbatim and normalize to null at the DataFrame boundary. Fields the
sweep proved on every record are required; the partial-presence fields
are optional (presence counts in the attribute docs, out of 608). The
minimal captured shape is the bare 7-key form -- units with no gateway
carry serial-shaped default names.

``year`` arrives as a quoted integer (``"2013"``); lax coercion types
it. No empty-string ``year`` was observed in 608 records -- if one ever
arrives, the loud ``int_parsing`` failure names it (the Motive ``year``
history, replayed on purpose).

Excluded fields (``extra='ignore'`` makes exclusion exactly "don't
model it"):

- ``tags`` -- a list of ``{id, name, parentTagId}`` objects (549/608,
  ``parentTagId`` itself partial within elements); the records layer's
  schema derivation supports scalars, enums, ``list[scalar]``, and
  nested models only (the Device/User exclusion precedent) -- modeled
  when the list-of-structs derivation vertical lands.

``externalIds`` is an OPEN, user-definable map whose keys are dotted
namespace names -- not a struct. The two Samsara-managed keys observed
in every carrying record are modeled as aliased fields
(``samsara.serial``, ``samsara.vin``, each mirroring its top-level
sibling exactly in capture); user-defined keys are absorbed by
``extra='ignore'`` until a capture shows one, per the union-of-observed
discipline.

Wire keys are camelCase; fields are snake_case via the ``to_camel``
alias generator (the dotted ``externalIds`` keys carry explicit
aliases, above the generator's reach).
"""

from datetime import datetime

from pydantic import ConfigDict, Field
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel

__all__: list[str] = [
    'Vehicle',
    'VehicleExternalIds',
    'VehicleGatewayRef',
    'VehicleStaticAssignedDriverRef',
]


class VehicleExternalIds(ResponseModel):
    """The ``externalIds`` block: an open map of namespaced external ids.

    Only the two Samsara-managed keys are modeled (see the module
    docstring's open-map caveat). Each key is independently optional --
    the sweep observed carriers with a serial and no VIN (595 blocks:
    ``samsara.serial`` on 509, ``samsara.vin`` on 576).

    Attributes:
        samsara_serial: The gateway serial, undashed (wire key
            ``samsara.serial``; equal to the record's top-level
            ``serial`` in every capture).
        samsara_vin: The VIN (wire key ``samsara.vin``; equal to the
            record's top-level ``vin`` in every capture).
    """

    samsara_serial: str | None = Field(default=None, alias='samsara.serial')
    samsara_vin: str | None = Field(default=None, alias='samsara.vin')


class VehicleGatewayRef(ResponseModel):
    """The installed telematics gateway: serial and hardware model.

    Attributes:
        serial: The gateway serial in Samsara's dashed 4-3-3 rendering
            (the record's top-level ``serial`` carries the undashed
            form of the same value in every capture).
        model: The gateway hardware model (``"VG34"`` captured).
    """

    serial: str
    model: str


class VehicleStaticAssignedDriverRef(ResponseModel):
    """The statically assigned driver reference: id and display name."""

    id: str
    name: str


class Vehicle(ResponseModel):
    """One Samsara fleet vehicle.

    A pure mirror of the captured fields (``tags`` excluded, module
    docstring). Field semantics are Samsara's; no value is derived or
    interpreted here.

    Attributes:
        id: Samsara's vehicle id -- a numeric string, mirrored as
            string.
        name: The unit's display name; units with no gateway carry
            serial-shaped default names (captured).
        notes: Free-form notes; ``""`` on every captured record,
            mirrored verbatim.
        harsh_acceleration_setting_type: Settings vocabulary
            (``"automatic"`` captured), mirrored, never interpreted.
        vehicle_regulation_mode: ``"regulated"`` / ``"unregulated"``
            captured, mirrored, never interpreted.
        created_at_time: Record creation (UTC).
        updated_at_time: Last record update (UTC).
        camera_serial: Dash-cam serial, dashed rendering (404/608).
        external_ids: The open external-id map's modeled slice
            (595/608).
        gateway: The installed gateway reference (509/608; absent on
            unplugged units -- the minimal shape).
        serial: The gateway serial, undashed (509/608; the dashed twin
            lives on ``gateway.serial``).
        esn: Electronic serial number; both captured shapes are
            alphanumeric strings (255/608).
        license_plate: Registration plate (410/608).
        make: Manufacturer (576/608).
        model: Vehicle model name (576/608).
        vin: Vehicle identification number (576/608).
        year: Model year; a quoted integer on the wire, typed by lax
            coercion (576/608).
        static_assigned_driver: Statically assigned driver reference
            (198/608).
        aux_input_type1: Auxiliary input assignment (12/608;
            ``"powerTakeOff"`` captured), mirrored, never interpreted.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    # Identity and lifecycle.
    id: str
    name: str
    notes: str
    harsh_acceleration_setting_type: str
    vehicle_regulation_mode: str
    created_at_time: datetime
    updated_at_time: datetime

    # Installed hardware.
    camera_serial: str | None = None
    external_ids: VehicleExternalIds | None = None
    gateway: VehicleGatewayRef | None = None
    serial: str | None = None
    esn: str | None = None

    # Vehicle identity.
    license_plate: str | None = None
    make: str | None = None
    model: str | None = None
    vin: str | None = None
    year: int | None = None

    # Assignment.
    static_assigned_driver: VehicleStaticAssignedDriverRef | None = None
    aux_input_type1: str | None = None
