# src/fleetpull/models/samsara/vehicle_fuel_energy_report.py
"""Samsara VehicleFuelEnergyReport response model
(``GET /fleet/reports/vehicles/fuel-energy``, post-decoder
window-stamped grain).

Written from captured live responses (2026-07-20/21 probe session: a
71/71 total census on the 1-day walk, structurally identical on the
2-day 267-report walk), never from docs. The model mirrors the record
``SamsaraWindowReportPageDecoder`` emits, and its two field families
have different provenance:

- ``window_start`` / ``window_end`` are DECODER-SYNTHESIZED
  (``windowStartDate``/``windowEndDate``): report rows carry NO
  event-time key of any kind, so the decoder stamps each row with the
  window the SENT spec asked for, copied verbatim from its
  ``startDate``/``endDate`` params. Contrast the stats triple's
  synthesized identity keys, which are lifted from the RECORD's own
  nested vehicle block -- these come from the request, because the
  request window IS the row's time identity.
- Everything else is WIRE-VERBATIM: the metric core, the cost block,
  and the ``vehicle`` ref, camelCase keys mirrored via aliases.

**NON-ADDITIVITY -- the rollup grain is the request window, proven
twice (2026-07-21).** Widening a 1-day window to 2 days GREW per-vehicle
metrics (36 of 47 vehicles shared between the 1-day walk and the
2-day window's first page grew), and summing two adjacent day
rollups reproduced the two-day rollup on only 178 of 267 vehicles
(89/267 MISMATCHED across distance, engine run time, fuel, and energy).
Each row is the provider's answer for exactly its window, nothing else:
day rows MUST NOT be summed to reproduce a wider window's rollup. This
is why the binding declares ``fixed_unit_days=1`` -- the unit width is
part of the row's meaning and never floats with configuration.

Requiredness posture: the window stamps, the ``vehicle`` ref, and its
``id`` are required STRUCTURALLY -- a rollup row without its window or
its entity is meaningless, so a future record omitting them must fail
loudly, never land an all-null row. The metric core (all eight metrics
plus ``estFuelEnergyCost``) is required on the WHOLE-WALK posture: the
census was total on every walked report (71/71 and, structurally, all
267 of the 2-day walk), and a rollup surface that computes its metrics
per window has no absence mechanism to be conservative about -- a
missing metric would be a contract change worth a loud failure. The
ref's ``name``/``energyType``/``externalIds`` stay optional per the
conservative posture (one fleet's walk is not a whole-population oath).

``efficiencyMpge``, ``estCarbonEmissionsKg``, and the cost ``amount``
are MIXED int|float on the wire -- modeled ``float``, lax coercion
lifting the int shape. ``energyType`` (observed only ``'fuel'``) and
``currencyCode`` (observed only ``'USD'``, 100-report samples) are
census-open plain ``str``\\s, never enums. ``vehicle.externalIds``
carries the LITERAL DOTTED wire keys ``samsara.serial``/``samsara.vin``
(both str, 71/71) on the NESTED ref, mirrored via explicit aliases with
single-key independence -- the assignments precedent.

Wire keys are camelCase; fields are snake_case via the ``to_camel``
alias generator, except the decoder-synthesized window stamps and the
dotted external-id keys, which take explicit aliases.
"""

from datetime import datetime

from pydantic import ConfigDict, Field
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel

__all__: list[str] = [
    'VehicleFuelEnergyCost',
    'VehicleFuelEnergyExternalIds',
    'VehicleFuelEnergyReport',
    'VehicleFuelEnergyVehicleRef',
]


class VehicleFuelEnergyCost(ResponseModel):
    """The ``estFuelEnergyCost`` block: the window's estimated cost.

    Both keys were 71/71 in census and required per the whole-walk
    posture (module docstring).

    Attributes:
        amount: The monetary amount -- MIXED int|float on the wire,
            modeled float.
        currency_code: The currency code -- observed only ``'USD'`` on
            a 100-report sample, census-open, so a plain ``str``.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    amount: float
    currency_code: str


class VehicleFuelEnergyExternalIds(ResponseModel):
    """The vehicle ref's ``externalIds`` block: namespaced external ids.

    The wire keys are the LITERAL DOTTED ``samsara.serial`` and
    ``samsara.vin`` (both str, 71/71 in census), mirrored via explicit
    aliases on this NESTED object -- the assignments precedent. Each key
    is independently optional (the conservative posture; the vehicles
    surface proves ``externalIds`` variance exists in this fleet).

    Attributes:
        samsara_serial: The gateway serial (wire key ``samsara.serial``).
        samsara_vin: The VIN (wire key ``samsara.vin``).
    """

    samsara_serial: str | None = Field(default=None, alias='samsara.serial')
    samsara_vin: str | None = Field(default=None, alias='samsara.vin')


class VehicleFuelEnergyVehicleRef(ResponseModel):
    """The ``vehicle`` block: the rollup's vehicle entity.

    All four keys were 71/71 in census; only ``id`` is required by
    structural judgment (a ref without an id references nothing), while
    the rest stays optional per the conservative posture.

    Attributes:
        id: Samsara's vehicle id -- a string, mirrored as string.
        name: The vehicle's display name.
        energy_type: The vehicle's energy type -- observed only
            ``'fuel'`` on a 100-report sample, census-open, so a plain
            ``str``, never an enum.
        external_ids: The dotted-key external-id block (wire key
            ``externalIds``).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str
    name: str | None = None
    energy_type: str | None = None
    external_ids: VehicleFuelEnergyExternalIds | None = None


class VehicleFuelEnergyReport(ResponseModel):
    """One vehicle's fuel-energy rollup over exactly one request window.

    A pure mirror of the window-stamped post-decoder record (module
    docstring: the window stamps are decoder-synthesized from the sent
    spec; everything else is wire-verbatim). Field semantics and units
    are Samsara's; no value is derived or interpreted here. Day rows
    MUST NOT be summed to reproduce a wider window's rollup (89/267
    mismatched -- module docstring).

    Attributes:
        window_start: The request window's start (decoder-synthesized
            ``windowStartDate``, verbatim from the sent ``startDate``)
            -- the event-time column: the row's time identity is the
            window that produced it.
        window_end: The request window's end (decoder-synthesized
            ``windowEndDate``, verbatim from the sent ``endDate``).
        vehicle: The rollup's vehicle entity.
        distance_traveled_meters: Distance traveled in the window, in
            meters, a bare int.
        efficiency_mpge: Efficiency in MPGe -- MIXED int|float on the
            wire, modeled float.
        energy_used_kwh: Energy used in the window, in kWh, a bare int.
        engine_idle_time_duration_ms: Engine idle time in the window,
            in milliseconds, a bare int.
        engine_run_time_duration_ms: Engine run time in the window, in
            milliseconds, a bare int.
        est_carbon_emissions_kg: Estimated carbon emissions in the
            window, in kilograms -- MIXED int|float on the wire,
            modeled float.
        fuel_consumed_ml: Fuel consumed in the window, in milliliters,
            a bare int.
        est_fuel_energy_cost: The window's estimated fuel/energy cost
            block.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    # Decoder-synthesized window identity (the sent spec's own window).
    window_start: datetime = Field(alias='windowStartDate')
    window_end: datetime = Field(alias='windowEndDate')

    # The rollup's entity.
    vehicle: VehicleFuelEnergyVehicleRef

    # The wire-verbatim metric core (whole-walk required; provider
    # units mirrored verbatim).
    distance_traveled_meters: int
    efficiency_mpge: float
    energy_used_kwh: int
    engine_idle_time_duration_ms: int
    engine_run_time_duration_ms: int
    est_carbon_emissions_kg: float
    fuel_consumed_ml: int
    est_fuel_energy_cost: VehicleFuelEnergyCost
