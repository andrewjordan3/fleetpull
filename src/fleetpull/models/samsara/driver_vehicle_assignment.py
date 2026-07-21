# src/fleetpull/models/samsara/driver_vehicle_assignment.py
"""Samsara DriverVehicleAssignment response model
(``GET /fleet/driver-vehicle-assignments``).

Written from captured live responses (2026-07-20 probe session: a full
24-hour walk of the fleet under BOTH ``filterBy`` values -- 216 records
each, proven identical as tuple sets, so the two sweeps are one
dataset), never from docs.

The record census is total: EVERY key was present on 216/216 records --
``startTime``/``endTime`` (RFC3339 strs, recovered tz-aware UTC; no
empty or missing ``endTime`` was observed -- assignments were only
ever observed complete), ``assignedAtTime`` (present on every record
but the EMPTY STRING on all of them -- the 2026-07-21 live proof
failed datetime parsing on record 0, and a 6,921-row week-wide value
census found '' on every single row: the Samsara empty-string posture,
mirrored verbatim as ``str``; a populated value's wire format is
UNOBSERVED, so no datetime recovery is presumed -- revisit on a
capture that shows one), ``assignmentType``
(str), ``isPassenger`` (bool), ``driver {id: str, name: str}``, and
``vehicle {id: str, name: str, externalIds}``.

``assignmentType``'s 24h census observed ``{'static': 158, 'HOS': 58}``
and the 2026-07-21 week-wide live proof added ``driverApp`` (25 of
8,042 rows) -- an OPEN vocabulary, not API-enforced on output (the
eldExemptReason lesson; contrast ``filterBy``'s INPUT vocabulary, which
IS 400-enforced), so the field stays a plain ``str`` with the observed
values documented here, never an enum -- exactly the posture that let
the third value land without a failure.

``vehicle.externalIds`` carries the LITERAL DOTTED wire keys
``samsara.serial`` and ``samsara.vin`` (both str, 216/216) on a NESTED
object, mirrored via explicit ``Field`` aliases -- the Samsara
``VehicleExternalIds`` precedent. Note the contrast with the stats
triple's ``vehicleSerial``/``vehicleVin``: those are flat keys the
series-unnesting DECODER synthesizes; these are the wire's own dotted
keys on the record, mirrored verbatim.

Requiredness posture: 216/216 across one day's two-sweep walk is NOT a
whole-population-over-time oath (the drivers conservative posture would
leave everything optional), but the structural core is required anyway
by structural judgment -- an assignment without its parties (``driver``,
``vehicle``, and their ``id``\\s: a party ref without an id references
nothing) or its bounds (``startTime``/``endTime``) is structurally
meaningless, so a future record omitting them should fail loudly, never
land an all-null row. Everything else (``assignedAtTime``,
``assignmentType``, ``isPassenger``, the ref ``name``\\s, and
``externalIds`` with its keys) stays optional per the conservative
posture. This is the asset_locations judgment, recorded here and in
DESIGN section 8.

Wire keys are camelCase; fields are snake_case via the ``to_camel``
alias generator, except the dotted external-id keys, which take
explicit aliases.
"""

from datetime import datetime

from pydantic import ConfigDict, Field
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel

__all__: list[str] = [
    'AssignmentDriverRef',
    'AssignmentVehicleExternalIds',
    'AssignmentVehicleRef',
    'DriverVehicleAssignment',
]


class AssignmentDriverRef(ResponseModel):
    """The ``driver`` block: the assignment's driver party.

    Both keys were 216/216 in census; only ``id`` is required by
    structural judgment (module docstring) -- a party ref without an id
    references nothing, while ``name`` stays optional per the
    conservative posture.

    Attributes:
        id: Samsara's driver id -- a string, mirrored as string.
        name: The driver's display name.
    """

    id: str
    name: str | None = None


class AssignmentVehicleExternalIds(ResponseModel):
    """The vehicle ref's ``externalIds`` block: namespaced external ids.

    The wire keys are the LITERAL DOTTED ``samsara.serial`` and
    ``samsara.vin`` (both str, 216/216 in census), mirrored via explicit
    aliases on this NESTED object -- the ``VehicleExternalIds``
    precedent, and the contrast with the stats triple's
    decoder-synthesized flat keys (module docstring). Each key is
    independently optional (the conservative posture; the vehicles
    surface proves ``externalIds`` variance exists in this fleet).

    Attributes:
        samsara_serial: The gateway serial (wire key ``samsara.serial``).
        samsara_vin: The VIN (wire key ``samsara.vin``).
    """

    samsara_serial: str | None = Field(default=None, alias='samsara.serial')
    samsara_vin: str | None = Field(default=None, alias='samsara.vin')


class AssignmentVehicleRef(ResponseModel):
    """The ``vehicle`` block: the assignment's vehicle party.

    All three keys were 216/216 in census; only ``id`` is required by
    structural judgment (module docstring), while ``name`` and
    ``external_ids`` stay optional per the conservative posture.

    Attributes:
        id: Samsara's vehicle id -- a string, mirrored as string.
        name: The vehicle's display name.
        external_ids: The dotted-key external-id block (wire key
            ``externalIds``).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str
    name: str | None = None
    external_ids: AssignmentVehicleExternalIds | None = None


class DriverVehicleAssignment(ResponseModel):
    """One driver-vehicle assignment interval.

    A pure mirror of the captured record. Field semantics and units are
    Samsara's; no value is derived or interpreted here. The structural
    core (``driver``, ``vehicle``, ``start_time``, ``end_time``) is
    required by structural judgment; the rest is optional per the
    conservative posture (module docstring).

    Attributes:
        driver: The driver party of the assignment.
        vehicle: The vehicle party of the assignment.
        start_time: The assignment interval's start (RFC3339, recovered
            tz-aware UTC) -- the event-time column: retrieval is
            overlap-anchored, and ownership anchors here via the
            runner's post-fetch window filter.
        end_time: The assignment interval's end (RFC3339, recovered
            tz-aware UTC); never observed empty or missing --
            assignments were only ever observed complete.
        assigned_at_time: The instant the assignment was made --
            observed as the EMPTY STRING on every one of 6,921
            week-censused rows (live-proven 2026-07-21), mirrored
            verbatim; a populated value has never been observed.
        assignment_type: How the assignment arose. Observed values
            ``static``, ``HOS``, and ``driverApp`` (the third surfaced
            only at week scale) -- an open vocabulary, so a plain
            ``str`` (module docstring).
        is_passenger: Whether the driver rode as a passenger.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    driver: AssignmentDriverRef
    vehicle: AssignmentVehicleRef
    start_time: datetime
    end_time: datetime
    assigned_at_time: str | None = None
    assignment_type: str | None = None
    is_passenger: bool | None = None
