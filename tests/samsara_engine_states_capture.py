"""The committed Samsara engine_states capture set (2026-07-20 probe session).

Three FULLY SYNTHETIC vehicle records shaped by the live census of
``GET /fleet/vehicles/stats/history`` with ``types=engineStates`` (a
1,045-reading, 138-vehicle 24-hour walk; per-vehicle keys exactly
``id``/``name``/``externalIds``/``engineStates``, series keys exactly
``{time, value}``, value vocabulary exactly ``On``/``Off``/``Idle``),
arranged as a two-page cursor walk whose pages carry DISJOINT vehicle
ids -- the probe-proven vehicle-axis cursor (three consecutive live
pages showed zero vehicle-id overlap). The variants exercise every
decoder and model arm: a multi-reading vehicle covering all three
observed engine-state values, a single-reading vehicle with
``externalIds`` ABSENT (a synthetic variant -- the censused page
carried the block 74/74, but one page is not a whole-population oath
and the vehicles surface shows exactly this variance; downstream it
proves the serial/vin omit-absent posture), and a terminal-page
single-reading carrier.

No record values here are scrubbed live values -- every id, name,
serial, VIN-shaped string, timestamp, and state sequence is synthetic
outright. What IS verbatim wire truth: the ``data`` + ``pagination
{endCursor, hasNextPage}`` envelope, the per-vehicle key set with the
literal DOTTED ``externalIds`` keys (``samsara.serial`` /
``samsara.vin``), the millisecond RFC3339 ``time`` shape, the exact
``{time, value}`` series keys, the ``On``/``Off``/``Idle`` value
vocabulary (census-closed only, NOT API-enforced on output), and the
terminal ``hasNextPage: false`` beside an empty-string ``endCursor``.

Consumed by the EngineState model tests (which unnest through
the production series decoder -- the model mirrors the flat
post-decoder record) and the engine_states endpoint tests -- kept as a
helper module under ``tests/`` so consumers share one capture set (the
``samsara_vehicles_capture`` precedent). The raw JSON literals are the
captures; the parsed objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# A continuation page: the multi-reading carrier (all three observed
# engine-state values) beside the externalIds-ABSENT single-reading
# vehicle. Reading times sit strictly inside a 12:00-13:00Z window --
# the probe's [startTime, endTime) anchoring evidence (min observed
# 12:00:03.062Z, max 12:59:56.881Z).
ENGINE_STATES_PAGE_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "id": "281474980000001",
            "name": "Truck 101",
            "externalIds": {
                "samsara.serial": "GSYNTH00000A",
                "samsara.vin": "SYNTH000000000001"
            },
            "engineStates": [
                {
                    "time": "2026-01-01T12:00:03.062Z",
                    "value": "On"
                },
                {
                    "time": "2026-01-01T12:20:15.500Z",
                    "value": "Idle"
                },
                {
                    "time": "2026-01-01T12:59:56.881Z",
                    "value": "Off"
                }
            ]
        },
        {
            "id": "281474980000002",
            "name": "Truck 102",
            "engineStates": [
                {
                    "time": "2026-01-01T12:05:00.000Z",
                    "value": "On"
                }
            ]
        }
    ],
    "pagination": {
        "endCursor": "00000000-0000-0000-0000-000000000041",
        "hasNextPage": true
    }
}"""

ENGINE_STATES_PAGE_RESPONSE: dict[str, JsonValue] = json.loads(
    ENGINE_STATES_PAGE_RESPONSE_JSON
)

# The terminal page shape -- hasNextPage false beside an empty-string
# endCursor (the provider-wide cursor contract) -- carrying a vehicle
# id DISJOINT from page one's (the vehicle-axis cursor, proven live).
ENGINE_STATES_TERMINAL_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "id": "281474980000003",
            "name": "Truck 103",
            "externalIds": {
                "samsara.serial": "GSYNTH00000C",
                "samsara.vin": "SYNTH000000000003"
            },
            "engineStates": [
                {
                    "time": "2026-01-01T12:30:45.250Z",
                    "value": "Off"
                }
            ]
        }
    ],
    "pagination": {
        "endCursor": "",
        "hasNextPage": false
    }
}"""

ENGINE_STATES_TERMINAL_RESPONSE: dict[str, JsonValue] = json.loads(
    ENGINE_STATES_TERMINAL_RESPONSE_JSON
)


def _envelope_records(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    result = envelope['data']
    assert isinstance(result, list)
    records: list[JsonObject] = []
    for record in result:
        assert isinstance(record, dict)
        records.append(record)
    return records


# All three committed vehicle records, in capture order: multi-reading
# carrier, externalIds-absent single-reading, terminal-page carrier.
ENGINE_STATES_VEHICLE_RECORDS: list[JsonObject] = _envelope_records(
    ENGINE_STATES_PAGE_RESPONSE
) + _envelope_records(ENGINE_STATES_TERMINAL_RESPONSE)
