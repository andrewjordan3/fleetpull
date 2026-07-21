# src/fleetpull/models/samsara/driver_fuel_energy_report.py
"""Samsara DriverFuelEnergyReport response model
(``GET /fleet/reports/drivers/fuel-energy``, post-decoder
window-stamped grain).

Written from captured live responses (2026-07-20/21 probe session: a
47/47 total census on the 1-day walk), never from docs. The vehicle
arm's shape with the entity swapped: the same metric core plus
``estFuelEnergyCost``, attributed to a ``driver {id, name}`` ref
instead of the vehicle block -- and NO ``externalIds`` anywhere on this
arm (never observed, so unmodeled as unobserved, never excluded).

The model mirrors the record ``SamsaraWindowReportPageDecoder`` emits:
``window_start`` / ``window_end`` are DECODER-SYNTHESIZED
(``windowStartDate``/``windowEndDate``) -- report rows carry NO
event-time key of any kind, so the decoder stamps each row with the
window the SENT spec asked for, copied verbatim from its
``startDate``/``endDate`` params (the request-sourced contrast with the
stats triple's record-sourced identity keys); everything else is
wire-verbatim.

**NON-ADDITIVITY -- the rollup grain is the request window
(2026-07-21).** Proven on this surface family twice: widening a 1-day
window to 2 days GREW per-entity metrics, and summing two adjacent day
rollups mismatched the two-day rollup on 89 of 267 vehicle reports.
Each row is the provider's answer for exactly its window, nothing else:
day rows MUST NOT be summed to reproduce a wider window's rollup --
which is why the binding declares ``fixed_unit_days=1``.

Requiredness posture: the window stamps, the ``driver`` ref, and its
``id`` are required STRUCTURALLY (a rollup row without its window or
its entity is meaningless); the metric core is required on the
WHOLE-WALK posture (47/47 total census -- a per-window rollup surface
has no absence mechanism to be conservative about, so a missing metric
is a contract change worth a loud failure). The ref's ``name`` stays
optional per the conservative posture.

``efficiencyMpge``, ``estCarbonEmissionsKg``, and the cost ``amount``
are MIXED int|float on the wire -- modeled ``float``; ``currencyCode``
(observed only ``'USD'`` on a 100-report sample) is census-open, a
plain ``str``.

Wire keys are camelCase; fields are snake_case via the ``to_camel``
alias generator, except the decoder-synthesized window stamps, which
take explicit aliases.
"""

from datetime import datetime

from pydantic import ConfigDict, Field
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel

__all__: list[str] = [
    'DriverFuelEnergyCost',
    'DriverFuelEnergyDriverRef',
    'DriverFuelEnergyReport',
]


class DriverFuelEnergyCost(ResponseModel):
    """The ``estFuelEnergyCost`` block: the window's estimated cost.

    Both keys were 47/47 in census and required per the whole-walk
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


class DriverFuelEnergyDriverRef(ResponseModel):
    """The ``driver`` block: the rollup's driver entity.

    Both keys were 47/47 in census; only ``id`` is required by
    structural judgment (a ref without an id references nothing), while
    ``name`` stays optional per the conservative posture. No
    ``externalIds`` was ever observed on this arm -- unmodeled as
    unobserved.

    Attributes:
        id: Samsara's driver id -- a string, mirrored as string.
        name: The driver's display name.
    """

    id: str
    name: str | None = None


class DriverFuelEnergyReport(ResponseModel):
    """One driver's fuel-energy rollup over exactly one request window.

    A pure mirror of the window-stamped post-decoder record (module
    docstring: the window stamps are decoder-synthesized from the sent
    spec; everything else is wire-verbatim). Field semantics and units
    are Samsara's; no value is derived or interpreted here. Day rows
    MUST NOT be summed to reproduce a wider window's rollup (module
    docstring).

    Attributes:
        window_start: The request window's start (decoder-synthesized
            ``windowStartDate``, verbatim from the sent ``startDate``)
            -- the event-time column: the row's time identity is the
            window that produced it.
        window_end: The request window's end (decoder-synthesized
            ``windowEndDate``, verbatim from the sent ``endDate``).
        driver: The rollup's driver entity.
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
    driver: DriverFuelEnergyDriverRef

    # The wire-verbatim metric core (whole-walk required; provider
    # units mirrored verbatim).
    distance_traveled_meters: int
    efficiency_mpge: float
    energy_used_kwh: int
    engine_idle_time_duration_ms: int
    engine_run_time_duration_ms: int
    est_carbon_emissions_kg: float
    fuel_consumed_ml: int
    est_fuel_energy_cost: DriverFuelEnergyCost
