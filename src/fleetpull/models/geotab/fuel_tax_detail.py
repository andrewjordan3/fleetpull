# src/fleetpull/models/geotab/fuel_tax_detail.py
"""GeoTab FuelTaxDetail response model (``GetFeed`` on ``typeName: FuelTaxDetail``).

Written from the 2026-07-21 live probe session, never from docs. A
FuelTaxDetail is one provider-calculated IFTA jurisdiction segment — a
vehicle's continuous travel within one jurisdiction, bracketed by its
enter/exit instants and odometer readings — a CALCULATED feed, stored as
emitted (DESIGN §4). Its version identity is a LIST: ``versions``
carries 16-hex component version tokens (mirrored ``list[str]``, a
§9-supported list-of-scalar) rather than the single ``version`` its
feed siblings carry — the consumer's ``(id, max version)`` reconcile
reads the re-emitted row's whole token list as the fresher edition.

THE ESTIMATES-ONLY-TENANT CAVEAT (DESIGN §8): the probed tenant has NO
fuel-transaction (fuel-card) integration, so every fuel value on this
surface is provider-derived from telemetry — estimates, not
transactions. The census cannot speak for integrated tenants.

Requiredness posture: the census is a uniform whole-page total — every
key present on all sampled records (100-300 per key across the probed
pages) — so every field is required, with the arms exactly as observed:

- ``driver`` arrives as either the object reference or the bare
  ``"UnknownDriverId"`` sentinel string; the shared
  ``bare_id_to_reference`` coercion (the shipped Trip mechanism) lifts
  the bare form to ``{"id": <string>}``, so ``is_driver`` is null
  exactly on sentinel rows.
- The hourly arrays (``hourlyGpsOdometer``, ``hourlyLatitude``, and
  kin) may be EMPTY lists — present on every record, sometimes with no
  elements (``hasHourlyData`` false) — mirrored as list-of-scalar
  fields, never demoted to nullable.

``enterTime`` (the event time — the segment materializes where it
begins) and ``exitTime`` are recovered tz-aware by validation, the
GeoTab sibling idiom. Mixed int-or-float wire numerics
(``enterOdometer``, ``exitOdometer``, the ``hourlyOdometer`` elements)
are modeled ``float``. ``authority`` and ``jurisdiction`` are
census-scoped open vocabularies — plain strs, never enums.
"""

from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel
from fleetpull.models.geotab.shared import bare_id_to_reference

__all__: list[str] = [
    'FuelTaxDetail',
    'FuelTaxDetailDeviceRef',
    'FuelTaxDetailDriverRef',
]


class FuelTaxDetailDeviceRef(ResponseModel):
    """The segment's device reference: the id alone, on every record."""

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class FuelTaxDetailDriverRef(ResponseModel):
    """The segment's driver reference.

    Arrives as an object or the bare ``"UnknownDriverId"`` sentinel
    string; the ``FuelTaxDetail.driver`` field's coercion lifts the
    bare form to ``{"id": <string>}``, so ``is_driver`` is null exactly
    on sentinel rows.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str
    is_driver: bool | None = None


class FuelTaxDetail(ResponseModel):
    """One GeoTab IFTA jurisdiction segment.

    A pure mirror of the whole-page census: every modeled key present on
    every record, so everything is required; the observed arms (the
    ``driver`` string-or-object sentinel, the possibly-empty hourly
    arrays) are the module docstring's.

    Attributes:
        authority: The taxing authority token (census-open plain str).
        device: The vehicle unit's reference.
        driver: The driver reference; the bare ``"UnknownDriverId"``
            sentinel lands as ``driver.id`` verbatim.
        enter_gps_odometer: The GPS-derived odometer at segment entry.
        enter_latitude: Latitude at segment entry.
        enter_longitude: Longitude at segment entry.
        enter_odometer: The odometer at segment entry.
        enter_time: Segment entry (UTC) — the endpoint's event time.
        exit_gps_odometer: The GPS-derived odometer at segment exit.
        exit_latitude: Latitude at segment exit.
        exit_longitude: Longitude at segment exit.
        exit_odometer: The odometer at segment exit.
        exit_time: Segment exit (UTC).
        has_hourly_data: Whether the hourly arrays carry elements.
        hourly_gps_odometer: Per-hour GPS-derived odometer readings
            (may be empty).
        hourly_is_odometer_interpolated: Per-hour interpolation flags
            (may be empty).
        hourly_latitude: Per-hour latitudes (may be empty).
        hourly_longitude: Per-hour longitudes (may be empty).
        hourly_odometer: Per-hour odometer readings (may be empty).
        id: GeoTab's record id.
        is_cluster_odometer: Whether the odometer is cluster-sourced.
        is_enter_odometer_interpolated: Whether the entry odometer is
            interpolated.
        is_exit_odometer_interpolated: Whether the exit odometer is
            interpolated.
        is_negligible: The provider's negligible-segment flag.
        jurisdiction: The jurisdiction token (census-open plain str).
        versions: The 16-hex component version tokens — this type's
            list-shaped version identity (the module docstring).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    authority: str
    device: FuelTaxDetailDeviceRef
    driver: Annotated[FuelTaxDetailDriverRef, BeforeValidator(bare_id_to_reference)]
    enter_gps_odometer: float
    enter_latitude: float
    enter_longitude: float
    enter_odometer: float
    enter_time: datetime
    exit_gps_odometer: float
    exit_latitude: float
    exit_longitude: float
    exit_odometer: float
    exit_time: datetime
    has_hourly_data: bool
    hourly_gps_odometer: list[float]
    hourly_is_odometer_interpolated: list[bool]
    hourly_latitude: list[float]
    hourly_longitude: list[float]
    hourly_odometer: list[float]
    id: str
    is_cluster_odometer: bool
    is_enter_odometer_interpolated: bool
    is_exit_odometer_interpolated: bool
    is_negligible: bool
    jurisdiction: str
    versions: list[str]
