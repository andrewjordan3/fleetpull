"""The committed Samsara vehicle_fuel_energy_reports capture set
(2026-07-20/21 probe session).

Four FULLY SYNTHETIC vehicle fuel-energy report records shaped by the
live census of ``GET /fleet/reports/vehicles/fuel-energy`` (71/71 total
on the 1-day walk, structurally identical on the 2-day 267-report
walk), arranged as a two-page cursor walk mirroring the server's OWN
~100-report paging shape (the ``limit`` param is proven ignored --
limit=512/513/10 on the same 2-day window all returned identical
3-page/267-report paging, 513 NOT rejected; the committed pages carry a
representative few records, the assignments precedent).

The variant coverage: a metric-rich row with FLOAT-valued
``efficiencyMpge``/``estCarbonEmissionsKg``/cost ``amount``; an
INT-valued ``efficiencyMpge`` row (the wire's mixed int|float shape,
proving the model's float coercion, with an int cost ``amount``
beside it); an electric-leaning row with nonzero ``energyUsedKwh``;
and a zero-activity row (every metric 0 -- a parked vehicle's window
answer). Every vehicle block carries ``energyType: "fuel"`` (the only
observed value, census-open) and the ``externalIds`` object with the
LITERAL DOTTED wire keys ``samsara.serial``/``samsara.vin``.

Report rows carry NO event-time key of any kind -- the row's time
identity is the request window itself, which the decoder stamps on as
``windowStartDate``/``windowEndDate`` from the SENT spec's
``startDate``/``endDate`` params (these surfaces' param NAMES, unlike
every sibling's startTime/endTime; RFC3339 datetimes accepted despite
the names). The raw pages below are PRE-STAMP wire truth; the stamped
records beside them are what the model tests consume, stamped exactly
as the decoder stamps (verbatim copies of the notional
[2026-01-02, 2026-01-03) sent window).

No record values here are scrubbed live values -- every id, name,
serial, VIN, and metric is synthetic outright. What IS verbatim wire
truth: the ``data``-as-OBJECT envelope nesting the list under
``vehicleReports``, the ``pagination {endCursor, hasNextPage}`` block,
the camelCase key set, the dotted external-id keys, and the terminal
``hasNextPage: false`` beside an empty-string ``endCursor``.

Consumed by the VehicleFuelEnergyReport model tests and the
vehicle_fuel_energy_reports endpoint tests -- kept as a helper module
under ``tests/`` so consumers share one capture set (the
``samsara_vehicles_capture`` precedent). The raw JSON literals are the
captures; the parsed objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# The notional sent window the stamped records carry -- verbatim what
# the builder renders for a [2026-01-02, 2026-01-03) one-day unit (the
# declared fixed_unit_days=1 width).
WINDOW_START_DATE: str = '2026-01-02T00:00:00Z'
WINDOW_END_DATE: str = '2026-01-03T00:00:00Z'

# A continuation page: the float-metric-rich row and the INT-valued
# efficiencyMpge row (the wire's mixed int|float shape, with an int
# cost amount beside it). The endCursor is an opaque synthetic token.
VEHICLE_FUEL_ENERGY_PAGE_RESPONSE_JSON: str = r"""
{
    "data": {
        "vehicleReports": [
            {
                "vehicle": {
                    "id": "281474981110001",
                    "name": "SYNTH-TRUCK-001",
                    "energyType": "fuel",
                    "externalIds": {
                        "samsara.serial": "SYNTH-SER-001",
                        "samsara.vin": "SYNTHVIN000000001"
                    }
                },
                "distanceTraveledMeters": 482301,
                "efficiencyMpge": 7.42,
                "energyUsedKwh": 0,
                "engineIdleTimeDurationMs": 1860000,
                "engineRunTimeDurationMs": 21540000,
                "estCarbonEmissionsKg": 152.7,
                "estFuelEnergyCost": {
                    "amount": 214.53,
                    "currencyCode": "USD"
                },
                "fuelConsumedMl": 168220
            },
            {
                "vehicle": {
                    "id": "281474981110002",
                    "name": "SYNTH-TRUCK-002",
                    "energyType": "fuel",
                    "externalIds": {
                        "samsara.serial": "SYNTH-SER-002",
                        "samsara.vin": "SYNTHVIN000000002"
                    }
                },
                "distanceTraveledMeters": 96500,
                "efficiencyMpge": 8,
                "energyUsedKwh": 0,
                "engineIdleTimeDurationMs": 600000,
                "engineRunTimeDurationMs": 4500000,
                "estCarbonEmissionsKg": 31,
                "estFuelEnergyCost": {
                    "amount": 42,
                    "currencyCode": "USD"
                },
                "fuelConsumedMl": 30310
            }
        ]
    },
    "pagination": {
        "endCursor": "c3ludGgtZnVlbC1lbmVyZ3ktY3Vyc29yLTAwMQ",
        "hasNextPage": true
    }
}"""

VEHICLE_FUEL_ENERGY_PAGE_RESPONSE: dict[str, JsonValue] = json.loads(
    VEHICLE_FUEL_ENERGY_PAGE_RESPONSE_JSON
)

# The terminal page shape -- hasNextPage false beside an empty-string
# endCursor (the provider-wide cursor contract) -- carrying the
# electric-leaning row (nonzero energyUsedKwh) and the zero-activity
# row (a parked vehicle's per-window answer).
VEHICLE_FUEL_ENERGY_TERMINAL_RESPONSE_JSON: str = r"""
{
    "data": {
        "vehicleReports": [
            {
                "vehicle": {
                    "id": "281474981110003",
                    "name": "SYNTH-TRUCK-003",
                    "energyType": "fuel",
                    "externalIds": {
                        "samsara.serial": "SYNTH-SER-003",
                        "samsara.vin": "SYNTHVIN000000003"
                    }
                },
                "distanceTraveledMeters": 120480,
                "efficiencyMpge": 21.9,
                "energyUsedKwh": 87,
                "engineIdleTimeDurationMs": 240000,
                "engineRunTimeDurationMs": 6120000,
                "estCarbonEmissionsKg": 12.4,
                "estFuelEnergyCost": {
                    "amount": 18.06,
                    "currencyCode": "USD"
                },
                "fuelConsumedMl": 9040
            },
            {
                "vehicle": {
                    "id": "281474981110004",
                    "name": "SYNTH-TRUCK-004",
                    "energyType": "fuel",
                    "externalIds": {
                        "samsara.serial": "SYNTH-SER-004",
                        "samsara.vin": "SYNTHVIN000000004"
                    }
                },
                "distanceTraveledMeters": 0,
                "efficiencyMpge": 0,
                "energyUsedKwh": 0,
                "engineIdleTimeDurationMs": 0,
                "engineRunTimeDurationMs": 0,
                "estCarbonEmissionsKg": 0,
                "estFuelEnergyCost": {
                    "amount": 0,
                    "currencyCode": "USD"
                },
                "fuelConsumedMl": 0
            }
        ]
    },
    "pagination": {
        "endCursor": "",
        "hasNextPage": false
    }
}"""

VEHICLE_FUEL_ENERGY_TERMINAL_RESPONSE: dict[str, JsonValue] = json.loads(
    VEHICLE_FUEL_ENERGY_TERMINAL_RESPONSE_JSON
)


def _envelope_reports(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    container = envelope['data']
    assert isinstance(container, dict)
    result = container['vehicleReports']
    assert isinstance(result, list)
    reports: list[JsonObject] = []
    for report in result:
        assert isinstance(report, dict)
        reports.append(report)
    return reports


def _stamped(report: JsonObject) -> JsonObject:
    """One report stamped exactly as the decoder stamps: the sent
    window's params copied verbatim onto the record."""
    return {
        **report,
        'windowStartDate': WINDOW_START_DATE,
        'windowEndDate': WINDOW_END_DATE,
    }


# All four committed records in capture order, WINDOW-STAMPED as the
# decoder emits them (the model's input grain): the float-rich row, the
# int-efficiencyMpge row, the electric-leaning row, the zero-activity
# row.
VEHICLE_FUEL_ENERGY_REPORT_RECORDS: list[JsonObject] = [
    _stamped(report)
    for report in _envelope_reports(VEHICLE_FUEL_ENERGY_PAGE_RESPONSE)
    + _envelope_reports(VEHICLE_FUEL_ENERGY_TERMINAL_RESPONSE)
]
