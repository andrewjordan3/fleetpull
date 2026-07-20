# src/fleetpull/models/samsara/gps_reading.py
"""Samsara GpsReading response model
(``GET /fleet/vehicles/stats/history``, ``types=gps``, post-decoder
reading grain).

Written from captured live responses (2026-07-20 probe session: a
2,512-reading sample over 8 cursor pages of a 24-hour window whose
full walk spanned 569 vehicles -- every vehicle returned per the
requested type carried data; no empty-array padding was observed),
never from docs. The model mirrors the FLAT record
``SamsaraVehicleSeriesPageDecoder`` emits, one row per reading -- the
grain the records pipeline represents (scalars, not list-of-objects;
DESIGN section 9) -- and its two field families have different
provenance:

- ``vehicle_id`` / ``vehicle_name`` / ``vehicle_serial`` /
  ``vehicle_vin`` are DECODER-SYNTHESIZED: the unnesting lifts them
  from the per-vehicle ``id``/``name`` keys and the ``externalIds``
  object's literal dotted ``samsara.serial``/``samsara.vin`` wire keys
  onto every reading. ``vehicle_id`` and ``vehicle_name`` were 74/74
  on the censused mixed-type page and are required; ``vehicle_serial``
  and ``vehicle_vin`` were also 74/74 there, but one page is not a
  whole-population oath (an unplugged or serial-less unit could omit
  its ``externalIds`` block -- the vehicles surface shows exactly that
  variance), so both stay OPTIONAL -- the drivers conservative posture.
- The reading keys are WIRE-VERBATIM. Sampled presence out of 2,512:
  ``time``, ``latitude``, ``longitude``, ``headingDegrees``,
  ``speedMilesPerHour``, ``isEcuSpeed``, and ``reverseGeo``
  (``{formattedLocation}``, the key always present in every carrying
  block) rode every reading and are required; ``address``
  (``{id, name}``, the defined-address-book reference) rode 401/2,512
  and is optional.

``speedMilesPerHour`` is MIXED int|float on the wire -- modeled
``float``, lax coercion lifting the int shape (the idling_events
``fuelConsumedMilliliters`` precedent). ``headingDegrees`` is a bare
int on every sampled reading. ``time`` is an RFC3339 string recovered
as a tz-aware UTC datetime by Pydantic's standard parse. Readings fall
strictly inside the requested ``[startTime, endTime)`` window
(probe-proven), so ``time`` is the endpoint's event-time column with
retrieval and routing coinciding natively.

The reverse-geocoded ``formattedLocation`` string is PII-adjacent --
capture fixtures are fully synthetic (the trips precedent).

Wire keys are camelCase; fields are snake_case via the ``to_camel``
alias generator.
"""

from datetime import datetime

from pydantic import ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel

__all__: list[str] = [
    'GpsReading',
    'GpsReadingAddressRef',
    'GpsReadingReverseGeo',
]


class GpsReadingReverseGeo(ResponseModel):
    """The ``reverseGeo`` block: the reading's reverse-geocoded location.

    Present on every sampled reading (2,512/2,512), with
    ``formattedLocation`` present in every carrying block.

    Attributes:
        formatted_location: The reverse-geocoded location string,
            mirrored verbatim (PII-adjacent; fixtures are synthetic).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    formatted_location: str


class GpsReadingAddressRef(ResponseModel):
    """The ``address`` block: a matched address-book reference (401/2,512).

    Present when the reading fell inside a defined address/geofence --
    the addresses surface is the book it references.

    Attributes:
        id: The defined address's id -- a string on the wire.
        name: The defined address's display name.
    """

    id: str
    name: str


class GpsReading(ResponseModel):
    """One GPS reading of one vehicle, at the reading grain.

    A pure mirror of the flat post-decoder record (module docstring:
    the identity fields are decoder-synthesized, the reading fields
    wire-verbatim). Field semantics and units are Samsara's; no value
    is derived or interpreted here.

    Attributes:
        vehicle_id: The vehicle's Samsara id -- a numeric string,
            decoder-synthesized from the vehicle record's ``id``.
        vehicle_name: The vehicle's display name, decoder-synthesized
            from the vehicle record's ``name``.
        vehicle_serial: The gateway serial, decoder-synthesized from
            ``externalIds['samsara.serial']`` (74/74 on the censused
            page; optional -- module docstring).
        vehicle_vin: The VIN, decoder-synthesized from
            ``externalIds['samsara.vin']`` (74/74 on the censused page;
            optional -- module docstring).
        time: The reading instant (RFC3339, recovered tz-aware UTC) --
            the event-time column; readings fall strictly inside the
            requested window.
        latitude: Reading latitude, decimal degrees.
        longitude: Reading longitude, decimal degrees.
        heading_degrees: Heading in degrees, a bare int on every
            sampled reading.
        speed_miles_per_hour: Speed in miles per hour -- MIXED
            int|float on the wire, modeled float (module docstring).
        is_ecu_speed: Whether the speed came from the ECU.
        reverse_geo: The reverse-geocoded location block (2,512/2,512).
        address: The matched address-book reference (401/2,512).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    # Decoder-synthesized vehicle identity.
    vehicle_id: str
    vehicle_name: str
    vehicle_serial: str | None = None
    vehicle_vin: str | None = None

    # Wire-verbatim reading payload.
    time: datetime
    latitude: float
    longitude: float
    heading_degrees: int
    speed_miles_per_hour: float
    is_ecu_speed: bool
    reverse_geo: GpsReadingReverseGeo

    # The partial block (absence-shaped).
    address: GpsReadingAddressRef | None = None
