# src/fleetpull/models/geotab/fill_up.py
"""GeoTab FillUp response model (``GetFeed`` on ``typeName: FillUp``).

Written from the 2026-07-21 live probe session, never from docs. A
FillUp is one provider-detected fuel-stop event — a CALCULATED feed:
past records re-emit under a higher ``version`` on reprocessing, stored
as emitted and reconciled by ``(id, max version)`` (DESIGN §4).

THE ESTIMATES-ONLY-TENANT CAVEAT (DESIGN §8): the probed tenant has NO
fuel-transaction (fuel-card) integration, so every fuel value on this
surface is provider-derived from telemetry — estimates, not
transactions. The census cannot speak for integrated tenants: ``cost``
was 0.0 on ALL records, ``fuelTransactions`` was an EMPTY list on ALL
records (excluded below as value-unobservable — on tenants with
fuel-card integration it populates with a shape never captured; it
joins the model when a capture types it), and ``productType`` was
``'Unknown'`` throughout.

Requiredness posture: the census is a uniform whole-page total —
100/100 records carried every modeled key — so every field is required,
with the sentinel arms exactly as observed. The census is a
TENANT-SCOPED observation. The observed arms:

- ``driver`` arrives as either the object reference (``{"id": ...,
  "isDriver": true}``, 87/100) or the bare ``"UnknownDriverId"``
  sentinel string; the shared ``bare_id_to_reference`` coercion (the
  shipped Trip mechanism) lifts the bare form to ``{"id": <string>}``,
  so the sentinel lands as ``driver.id`` and ``is_driver`` is null
  exactly on sentinel rows.
- ``derivedVolume`` carries an observed ``-1.0`` sentinel (the
  provider's could-not-derive marker) beside real volumes — mirrored
  VERBATIM, never nulled: reshaping a sentinel would be interpretation.
- ``confidence`` is a comma-joined detection-method token list carried
  as ONE plain string (e.g. ``'FuelLevel, TripStop'``) — splitting it
  would presume a use case; the observed vocabulary is census-open.
- ``tankCapacity.source`` observed vocabulary: ``EstimateFuelLevel`` /
  ``DiagnosticTankCapacity`` / ``Unknown`` — census-open plain str.

Mixed int-or-float wire numerics (``derivedVolume``, ``distance``,
``odometer``, ``tankCapacity.volume``, ``volume``) are modeled
``float``. ``dateTime`` (the event time) and each extrema point's
``dateTime`` are recovered tz-aware by validation, the GeoTab sibling
idiom.
"""

from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel
from fleetpull.models.geotab.shared import bare_id_to_reference

__all__: list[str] = [
    'FillUp',
    'FillUpDeviceRef',
    'FillUpDriverRef',
    'FillUpLocation',
    'FillUpTankCapacity',
    'FillUpTankLevelExtrema',
    'FillUpTankLevelPoint',
]


class FillUpDeviceRef(ResponseModel):
    """The fill-up's device reference: the id alone, on every record."""

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class FillUpDriverRef(ResponseModel):
    """The fill-up's driver reference.

    Arrives as an object or the bare ``"UnknownDriverId"`` sentinel
    string; the ``FillUp.driver`` field's coercion lifts the bare form
    to ``{"id": <string>}``, so ``is_driver`` is null exactly on
    sentinel rows.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str
    is_driver: bool | None = None


class FillUpLocation(ResponseModel):
    """The fill-up's coordinate: ``x`` longitude, ``y`` latitude."""

    model_config = ConfigDict(alias_generator=to_camel)

    x: float
    y: float


class FillUpTankCapacity(ResponseModel):
    """The provider's tank-capacity estimate and its derivation source.

    Attributes:
        source: How the capacity was derived — observed vocabulary
            ``EstimateFuelLevel`` / ``DiagnosticTankCapacity`` /
            ``Unknown``, census-open plain str.
        volume: The estimated capacity in liters (mixed int-or-float on
            the wire, modeled float).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    source: str
    volume: float


class FillUpTankLevelPoint(ResponseModel):
    """One tank-level extremum: its source, instant, and level reading."""

    model_config = ConfigDict(alias_generator=to_camel)

    source: str
    date_time: datetime
    data: float


class FillUpTankLevelExtrema(ResponseModel):
    """The tank-level extrema pair bracketing the detected fill-up."""

    model_config = ConfigDict(alias_generator=to_camel)

    maxima_point: FillUpTankLevelPoint
    minima_point: FillUpTankLevelPoint


class FillUp(ResponseModel):
    """One GeoTab provider-detected fuel-stop event.

    A pure mirror of the 100/100 whole-page census: every modeled key
    present on every record, so everything is required; the sentinel
    arms (``driver`` string-or-object, the ``-1.0`` ``derived_volume``)
    are exactly as observed. The estimates-only-tenant caveat is the
    module docstring's.

    Attributes:
        confidence: The comma-joined detection-method token list, one
            plain string (census-open).
        cost: The transaction cost — 0.0 on ALL census records (no fuel
            transaction integration on the probed tenant).
        currency_code: The cost's currency code (census-open plain str).
        date_time: The detected fill-up's UTC instant — the endpoint's
            event time.
        derived_volume: The provider-derived fill volume in liters;
            ``-1.0`` is the observed could-not-derive sentinel, mirrored
            verbatim.
        device: The vehicle unit's reference.
        distance: Distance since the prior fill-up, km.
        driver: The driver reference; the bare ``"UnknownDriverId"``
            sentinel lands as ``driver.id`` verbatim.
        id: GeoTab's record id.
        location: The detected stop's coordinate.
        odometer: The odometer reading at the fill-up.
        product_type: The fuel product type — ``'Unknown'`` on all
            census records (census-open plain str).
        tank_capacity: The tank-capacity estimate and its source.
        tank_level_extrema: The level extrema bracketing the fill-up.
        total_fuel_used: Cumulative fuel used at the fill-up, liters.
        version: The record's version token — the calculated-feed
            reconcile key beside ``id``.
        volume: The fill volume in liters (unit reasoning, recorded: the sibling wire key totalIdlingFuelUsedL carries the L suffix, and the probed value range -- median 299, max 848 -- is truck-tank plausible in liters and absurd in gallons).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    confidence: str
    cost: float
    currency_code: str
    date_time: datetime
    derived_volume: float
    device: FillUpDeviceRef
    distance: float
    driver: Annotated[FillUpDriverRef, BeforeValidator(bare_id_to_reference)]
    id: str
    location: FillUpLocation
    odometer: float
    product_type: str
    tank_capacity: FillUpTankCapacity
    tank_level_extrema: FillUpTankLevelExtrema
    total_fuel_used: float
    version: str
    volume: float
