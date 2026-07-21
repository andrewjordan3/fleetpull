"""The committed Samsara driver_fuel_energy_reports capture set
(2026-07-20/21 probe session).

Three FULLY SYNTHETIC driver fuel-energy report records shaped by the
live census of ``GET /fleet/reports/drivers/fuel-energy`` (47/47 total
on the 1-day walk), arranged as a two-page cursor walk mirroring the
server's OWN ~100-report paging shape (a 1-day driver window showed
``hasNextPage: true`` at 100 reports; the ``limit`` param is proven
ignored on this surface family -- the committed pages carry a
representative few records, the assignments precedent).

The driver arm is the vehicle arm's metric core attributed to a
``driver {id, name}`` block instead of the vehicle block -- and NO
``externalIds`` appears ANYWHERE on this arm (never observed across
the whole 47/47 census; the fixture preserves that absence exactly).
The variant coverage: a metric-rich row with FLOAT-valued
``efficiencyMpge``/``estCarbonEmissionsKg``/cost ``amount``, an
INT-valued ``efficiencyMpge`` row (the wire's mixed int|float shape,
proving the model's float coercion), and a zero-activity row.

Report rows carry NO event-time key of any kind -- the row's time
identity is the request window itself, which the decoder stamps on as
``windowStartDate``/``windowEndDate`` from the SENT spec's
``startDate``/``endDate`` params. The raw pages below are PRE-STAMP
wire truth; the stamped records beside them are what the model tests
consume, stamped exactly as the decoder stamps (verbatim copies of the
notional [2026-01-02, 2026-01-03) sent window).

No record values here are scrubbed live values -- every id, name, and
metric is synthetic outright. What IS verbatim wire truth: the
``data``-as-OBJECT envelope nesting the list under ``driverReports``,
the ``pagination {endCursor, hasNextPage}`` block, the camelCase key
set, and the terminal ``hasNextPage: false`` beside an empty-string
``endCursor``.

Consumed by the DriverFuelEnergyReport model tests and the
driver_fuel_energy_reports endpoint tests -- kept as a helper module
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
# efficiencyMpge row. The endCursor is an opaque synthetic token.
DRIVER_FUEL_ENERGY_PAGE_RESPONSE_JSON: str = r"""
{
    "data": {
        "driverReports": [
            {
                "driver": {
                    "id": "51000001",
                    "name": "Synthetic Driver One"
                },
                "distanceTraveledMeters": 388404,
                "efficiencyMpge": 6.85,
                "energyUsedKwh": 0,
                "engineIdleTimeDurationMs": 2280000,
                "engineRunTimeDurationMs": 18660000,
                "estCarbonEmissionsKg": 131.9,
                "estFuelEnergyCost": {
                    "amount": 186.9,
                    "currencyCode": "USD"
                },
                "fuelConsumedMl": 146540
            },
            {
                "driver": {
                    "id": "51000002",
                    "name": "Synthetic Driver Two"
                },
                "distanceTraveledMeters": 51200,
                "efficiencyMpge": 9,
                "energyUsedKwh": 0,
                "engineIdleTimeDurationMs": 300000,
                "engineRunTimeDurationMs": 2400000,
                "estCarbonEmissionsKg": 15,
                "estFuelEnergyCost": {
                    "amount": 20,
                    "currencyCode": "USD"
                },
                "fuelConsumedMl": 14330
            }
        ]
    },
    "pagination": {
        "endCursor": "c3ludGgtZHJpdmVyLWZ1ZWwtY3Vyc29yLTAwMQ",
        "hasNextPage": true
    }
}"""

DRIVER_FUEL_ENERGY_PAGE_RESPONSE: dict[str, JsonValue] = json.loads(
    DRIVER_FUEL_ENERGY_PAGE_RESPONSE_JSON
)

# The terminal page shape -- hasNextPage false beside an empty-string
# endCursor (the provider-wide cursor contract) -- carrying the
# zero-activity row (a non-driving driver's per-window answer).
DRIVER_FUEL_ENERGY_TERMINAL_RESPONSE_JSON: str = r"""
{
    "data": {
        "driverReports": [
            {
                "driver": {
                    "id": "51000003",
                    "name": "Synthetic Driver Three"
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

DRIVER_FUEL_ENERGY_TERMINAL_RESPONSE: dict[str, JsonValue] = json.loads(
    DRIVER_FUEL_ENERGY_TERMINAL_RESPONSE_JSON
)


def _envelope_reports(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    container = envelope['data']
    assert isinstance(container, dict)
    result = container['driverReports']
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


# All three committed records in capture order, WINDOW-STAMPED as the
# decoder emits them (the model's input grain): the float-rich row, the
# int-efficiencyMpge row, the zero-activity row.
DRIVER_FUEL_ENERGY_REPORT_RECORDS: list[JsonObject] = [
    _stamped(report)
    for report in _envelope_reports(DRIVER_FUEL_ENERGY_PAGE_RESPONSE)
    + _envelope_reports(DRIVER_FUEL_ENERGY_TERMINAL_RESPONSE)
]
