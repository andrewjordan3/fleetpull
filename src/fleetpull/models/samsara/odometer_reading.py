# src/fleetpull/models/samsara/odometer_reading.py
"""Samsara OdometerReading response model
(``GET /fleet/vehicles/stats/history``, ``types=obdOdometerMeters``,
post-decoder reading grain).

Written from captured live responses (2026-07-20 probe session: a
9,480-reading census over a 24-hour window, 135 vehicles -- every
vehicle returned per the requested type carried data; no empty-array
padding was observed), never from docs. The model mirrors the FLAT
record ``SamsaraVehicleSeriesPageDecoder`` emits, one row per reading
-- the grain the records pipeline represents (scalars, not
list-of-objects; DESIGN section 9) -- and its two field families have
different provenance:

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
- ``time`` / ``value`` are WIRE-VERBATIM reading keys: the series
  census observed exactly ``{time, value}`` on every one of the 9,480
  readings, so both are required.

``value`` is the OBD odometer in METERS -- a bare int on every observed
reading (range 3,552,000..1,012,456,215 in census), mirrored verbatim
under the model's unitless wire name; the unit lives here and in the
series' stat-type name (``obdOdometerMeters``), never converted.

``time`` is an RFC3339 string recovered as a tz-aware UTC datetime by
Pydantic's standard parse. Readings fall strictly inside the requested
``[startTime, endTime)`` window (probe-proven), so ``time`` is the
endpoint's event-time column with retrieval and routing coinciding
natively.

Wire keys are camelCase; fields are snake_case via the ``to_camel``
alias generator.
"""

from datetime import datetime

from pydantic import ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel

__all__: list[str] = ['OdometerReading']


class OdometerReading(ResponseModel):
    """One OBD odometer reading of one vehicle, at the reading grain.

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
        value: The OBD odometer in meters -- a bare int on every
            observed reading, mirrored verbatim (module docstring).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    # Decoder-synthesized vehicle identity.
    vehicle_id: str
    vehicle_name: str
    vehicle_serial: str | None = None
    vehicle_vin: str | None = None

    # Wire-verbatim reading payload.
    time: datetime
    value: int
