# src/fleetpull/models/samsara/trip.py
"""Samsara Trip response model (``GET /v1/fleet/trips`` -- the legacy v1
surface; every modern candidate path 404s).

Written from captured live responses (2026-07-20 probe session: a
725-trip census across 60 vehicles), never from docs. The census
observed ZERO nulls anywhere: every key below except the two address
blocks was present on every record, so those fields are required; the
address blocks (``startAddress`` 177/725, ``endAddress`` 185/725) are
present only when the trip endpoint matched a defined address/geofence
and are optional.

``startMs``/``endMs`` are epoch-MILLISECOND ints on the wire. The
mirror RECOVERS them as tz-aware UTC datetimes via a mode='before'
validator -- type recovery is structural and belongs on the mirror (the
``GeotabTimeSpan`` precedent), not interpretation. The field names
(``start_time``/``end_time``) drop the wire's unit suffix because the
recovered type carries the unit; naming is the model's to own. ``endMs``
was present on every observed trip including the two most recent --
in-progress trips were never observed and appear to materialize on
completion; the watermark lookback absorbs late materialization
(accepted residual, DESIGN §8), so ``end_time`` stays required and a
trip ever arriving without it fails validation loudly.

Everything else mirrors verbatim: the unit-suffixed int family
(``distanceMeters``, ``fuelConsumedMl``, ``tollMeters``) and the
odometer pair keep provider units untouched; ``driverId`` is an int
whose ``0`` is the UNASSIGNED sentinel (110/725) -- mirrored verbatim,
never nulled or interpreted. ``assetIds``/``codriverIds`` are typed
``list[int]`` -- the int-id family, in the ``list[scalar]`` form the
records layer's schema derivation represents (DESIGN §9; the §9
pipeline has no tuple form). The 725-trip census observed both EMPTY on
every record; a larger live pull (2026-07-22) then returned ``assetIds``
populated on a substantial minority of trips (its elements are attached
assets -- trailers/equipment -- not the trip's own vehicle), while
``codriverIds`` stayed empty. The ``list[int]`` typing anticipated
exactly that, so a non-empty capture lands as data, not a crash -- the
census sized the shape, it never bounded the population.

The wire record does NOT echo the requested ``vehicleId``: per-vehicle
attribution is the request parameter (the roster fan-out member), not a
wire field. So ``vehicle_id`` is the one SYNTHESIZED field on this
otherwise-pure mirror -- ``SamsaraTripsPageDecoder`` stamps it off the
sent spec before validation, mirrored as a string to match
``Vehicle.id`` for a direct join to the vehicles listing. The response
*wrapper* (the ``{"trips": [...]}`` envelope, unpaginated) is the
endpoints layer's decoder concern; this module mirrors only the inner
per-trip object, plus that one synthesized identity.

Wire keys are camelCase; fields are snake_case via the ``to_camel``
alias generator (``startMs``/``endMs`` carry explicit aliases, since
their field names diverge by design).
"""

from datetime import UTC, datetime, timedelta
from typing import Annotated, Final

from pydantic import BeforeValidator, ConfigDict, Field
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel
from fleetpull.vocabulary import JsonValue

__all__: list[str] = [
    'Trip',
    'TripAddress',
    'TripCoordinates',
]

_EPOCH_START: Final[datetime] = datetime(1970, 1, 1, tzinfo=UTC)


def _epoch_milliseconds_to_datetime(value: JsonValue | datetime) -> datetime:
    """The epoch-ms ingress: recover a tz-aware UTC datetime from a wire int.

    Strict by design: the census observed BARE ints only, so anything
    else -- a quoted number, a float, a bool -- is a wire drift that
    should fail validation loudly, not pass mangled. Exact integer
    arithmetic (no float epoch math), so millisecond precision is
    preserved verbatim.

    Args:
        value: The raw wire value, or an already-recovered datetime on a
            Pydantic revalidation path.

    Returns:
        The instant as a timezone-aware UTC datetime (passthrough for an
        already-recovered value).

    Raises:
        ValueError: ``value`` is not a bare int (bools excluded).
    """
    if isinstance(value, datetime):
        return value
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f'expected a bare epoch-millisecond int, got {type(value).__name__}'
        )
    return _EPOCH_START + timedelta(milliseconds=value)


# The epoch-ms datetime field type. Plain assignment, not a `type`
# statement: Pydantic must evaluate the Annotated form eagerly so the
# metadata lifts into FieldInfo and the records field walk sees the bare
# `datetime` leaf (the GeotabTimeSpan stance).
_EpochMillisecondsDatetime = Annotated[
    datetime, BeforeValidator(_epoch_milliseconds_to_datetime)
]


class TripCoordinates(ResponseModel):
    """A ``{latitude, longitude}`` coordinate pair, decimal-degree floats.

    The one shape both trip endpoints' coordinate blocks
    (``startCoordinates``, ``endCoordinates``) carry -- both keys
    present in every captured block.
    """

    latitude: float
    longitude: float


class TripAddress(ResponseModel):
    """A matched address/geofence block: the ``{address, id, name}`` shape.

    Present only when a trip endpoint matched a defined address/geofence
    (``startAddress`` 177/725, ``endAddress`` 185/725); all three keys
    present in every carrying block.

    Attributes:
        address: The defined address's street address string.
        id: Samsara's address id -- a BARE int on the wire, unlike the
            string ids of the vehicles/drivers surfaces.
        name: The defined address's display name.
    """

    address: str
    id: int
    name: str


class Trip(ResponseModel):
    """One Samsara vehicle trip, overlap-retrieved per (vehicle, window).

    A near-pure mirror of the captured fields (module docstring): field
    semantics and units are Samsara's, no value derived or interpreted
    -- the one exception is ``vehicle_id``, synthesized because the wire
    record does not echo the requested ``vehicleId`` (stamped off the
    sent spec by ``SamsaraTripsPageDecoder``).

    Attributes:
        vehicle_id: The fan-out vehicle this trip belongs to, stamped
            from the request's ``vehicleId`` -- the wire record never
            echoes it; a numeric string, matching ``Vehicle.id``.
        start_time: Trip start, recovered from the wire's epoch-ms
            ``startMs`` -- the endpoint's event time (start-anchored
            ownership, DESIGN §4).
        end_time: Trip end, recovered from the wire's epoch-ms
            ``endMs``; present on every observed trip (in-progress trips
            appear to materialize on completion).
        distance_meters: Trip distance in meters, a bare int.
        fuel_consumed_ml: Fuel consumed in milliliters, a bare int.
        toll_meters: Distance on toll roads in meters, a bare int.
        start_odometer: Odometer at trip start -- provider units
            mirrored verbatim, a bare int.
        end_odometer: Odometer at trip end -- provider units mirrored
            verbatim, a bare int.
        driver_id: The assigned driver's id; ``0`` is the UNASSIGNED
            sentinel (110/725), mirrored verbatim.
        start_location: Reverse-geocoded start location string.
        end_location: Reverse-geocoded end location string.
        start_coordinates: The start ``{latitude, longitude}`` block.
        end_coordinates: The end ``{latitude, longitude}`` block.
        asset_ids: Attached asset ids (trailers/equipment, not the
            trip's own vehicle); empty across the 725-trip census, then
            populated on a minority of trips in a larger live pull.
        codriver_ids: Co-driver ids; empty across the 725-trip census
            and the larger live pull alike.
        start_address: The matched start address/geofence block
            (177/725); null when no defined address matched.
        end_address: The matched end address/geofence block (185/725);
            null when no defined address matched.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    # The fan-out vehicle, synthesized: the wire record does not echo the
    # requested vehicleId, so SamsaraTripsPageDecoder stamps it off the
    # sent spec before validation (module docstring). Mirrored as a
    # string, matching Vehicle.id, so it joins the vehicles listing
    # directly; the to_camel generator aliases vehicle_id -> vehicleId,
    # which is the decoder's stamp key.
    vehicle_id: str

    # The trip interval (epoch-ms recovered, module docstring).
    start_time: _EpochMillisecondsDatetime = Field(alias='startMs')
    end_time: _EpochMillisecondsDatetime = Field(alias='endMs')

    # Distances, fuel, and odometers (provider units, bare ints).
    distance_meters: int
    fuel_consumed_ml: int
    toll_meters: int
    start_odometer: int
    end_odometer: int

    # Assignment (0 = unassigned, mirrored verbatim).
    driver_id: int

    # Endpoints: geocoded strings and coordinate blocks.
    start_location: str
    end_location: str
    start_coordinates: TripCoordinates
    end_coordinates: TripCoordinates

    # The int-id lists (only empties observed).
    asset_ids: list[int]
    codriver_ids: list[int]

    # The partial address/geofence matches.
    start_address: TripAddress | None = None
    end_address: TripAddress | None = None
