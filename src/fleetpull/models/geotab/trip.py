# src/fleetpull/models/geotab/trip.py
"""GeoTab Trip response model (JSON-RPC ``Get`` on ``typeName: Trip``).

Written from captured live responses (2026-07-13 probe session), never
from docs. A Trip is one engine-on movement interval plus its trailing
stop window; the model is the union of observed fields with every field
optional, a pure mirror -- no value is derived or interpreted here.

Interval semantics (12 of 12 captured records):
``driving_duration = stop - start``; ``stop_duration = next_trip_start
- stop``; ``idling_duration`` measures engine-on time WITHIN the
post-trip stop window, never within the drive. The zero-distance
degenerate shape has ``start == stop``, ``driving_duration`` of zero,
and NO ``averageSpeed`` key at all -- absence is a shape, landing as a
null.

Units (delta-arithmetic confirmed against the captures):

- distances (``distance``, ``after_hours_distance``, ``work_distance``)
  are kilometers;
- speeds (``average_speed``, ``maximum_speed``) are km/h;
- ``odometer`` is METERS (confirmed by delta arithmetic against a
  trip's own km distance);
- ``engine_hours`` is SECONDS despite the name -- a captured 26.1M
  "hours" is 7,251 real engine-hours. The value is stored verbatim;
  renaming or converting it would break the pure-mirror rule, so the
  trap is documented here and at every mention instead.

Durations arrive as .NET TimeSpan strings, parsed at the boundary
through ``GeotabTimeSpan`` (``models/geotab/shared.py``); day-prefixed
spans (``"4.16:41:16"``) occur whenever a stop window crosses days.

The ``driver`` reference arrives as either an object
(``{"id": ..., "isDriver": true}``) or the bare known-id sentinel
string ``"UnknownDriverId"``; the shared ``bare_id_to_reference``
coercion lifts any bare string to ``{"id": <string>}`` verbatim, so the
sentinel lands as ``driver__id`` and ``driver__is_driver`` stays null
on sentinel rows. ``maximum_speed`` and the ``speed_range1/2/3`` trio
are modeled ``float`` although every captured value is integral: they
are physical measurements like ``average_speed``, and JSON numbers do
not distinguish ``94`` from ``94.0`` (``speed_range*`` semantics are
unconfirmed -- captured all-zero).
"""

from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel
from fleetpull.models.geotab.shared import GeotabTimeSpan, bare_id_to_reference

__all__: list[str] = ['Trip', 'TripDeviceRef', 'TripDriverRef', 'TripStopPoint']


class TripDeviceRef(ResponseModel):
    """The trip's device reference: the id alone."""

    model_config = ConfigDict(alias_generator=to_camel)

    id: str | None = None


class TripDriverRef(ResponseModel):
    """The trip's driver reference.

    Arrives as an object or the bare ``"UnknownDriverId"`` sentinel
    string; the ``Trip.driver`` field's coercion lifts the bare form to
    ``{"id": <string>}``, so ``is_driver`` is null exactly on sentinel
    rows.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str | None = None
    is_driver: bool | None = None


class TripStopPoint(ResponseModel):
    """The trip's stop coordinate: ``x`` longitude, ``y`` latitude."""

    model_config = ConfigDict(alias_generator=to_camel)

    x: float | None = None
    y: float | None = None


class Trip(ResponseModel):
    """One GeoTab Trip: a movement interval and its trailing stop window.

    A pure mirror of the union of captured fields, everything optional.
    Groups below follow the captured record layout: identity, the
    interval, durations, distances and speeds, counters, and flags. The
    unit traps (``engine_hours`` is seconds, ``odometer`` meters) are
    documented in the module docstring.

    Attributes:
        id: GeoTab's trip id -- the seek-paging sort key.
        version: The record's version token (ids and versions share one
            counter space).
        device: The vehicle reference.
        driver: The driver reference; the bare ``"UnknownDriverId"``
            sentinel lands as ``driver.id`` verbatim.
        start: Trip start (UTC) -- the endpoint's event time.
        stop: Trip end (UTC); equals ``start`` on the zero-distance
            degenerate shape.
        next_trip_start: The following trip's start; bounds this trip's
            stop window.
        engine_hours: Cumulative engine SECONDS despite the name (the
            module-docstring trap), stored verbatim.
        odometer: Cumulative odometer in METERS, stored verbatim.
        average_speed: Mean speed in km/h; the key is absent on
            zero-distance trips (lands null).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    # Identity.
    id: str | None = None
    version: str | None = None
    device: TripDeviceRef | None = None
    driver: Annotated[TripDriverRef | None, BeforeValidator(bare_id_to_reference)] = (
        None
    )

    # The interval.
    start: datetime | None = None
    stop: datetime | None = None
    next_trip_start: datetime | None = None

    # Durations (.NET TimeSpan strings on the wire).
    driving_duration: GeotabTimeSpan = None
    stop_duration: GeotabTimeSpan = None
    idling_duration: GeotabTimeSpan = None
    after_hours_driving_duration: GeotabTimeSpan = None
    after_hours_stop_duration: GeotabTimeSpan = None
    work_driving_duration: GeotabTimeSpan = None
    work_stop_duration: GeotabTimeSpan = None
    speed_range1_duration: GeotabTimeSpan = None
    speed_range2_duration: GeotabTimeSpan = None
    speed_range3_duration: GeotabTimeSpan = None

    # Distances (km) and speeds (km/h).
    distance: float | None = None
    after_hours_distance: float | None = None
    work_distance: float | None = None
    average_speed: float | None = None
    maximum_speed: float | None = None
    speed_range1: float | None = None
    speed_range2: float | None = None
    speed_range3: float | None = None

    # Cumulative counters (the unit traps: seconds and meters).
    engine_hours: float | None = None
    odometer: float | None = None

    # Flags and the stop coordinate.
    after_hours_start: bool | None = None
    after_hours_end: bool | None = None
    is_seat_belt_off: bool | None = None
    stop_point: TripStopPoint | None = None
