"""The committed Samsara idling_events capture set (2026-07-20 probe session).

Three FULLY SYNTHETIC event records shaped by the live census (2,200
events over an 11-page cursor walk at limit=200, zero real nulls --
absence-shaped optionality) inside the captured modern envelope of
``GET /idling/events`` (``data`` list + ``pagination {endCursor,
hasNextPage}``): the maximal variant (every observed key -- ``operator``
1546/2200, ``address`` with ``addressTypes: ["yard"]`` 552/2200,
``airTemperatureMillicelsius`` 1833/2200), the minimal variant (the
always-present key set only), and the mixed-float variant
(``fuelConsumedMilliliters`` arrived as BOTH int and float across the
census -- this record carries the float shape; the other two carry the
int shape). Beside the page pair sit the two captured HTTP 400 JSON
bodies: the per-endpoint limit-tier rejection (limit=512 against THIS
endpoint's 200 maximum -- the first captured instance of Samsara's
per-endpoint limit tiers) and the sub-3-months range-cap rejection (91
days accepted; 180 days returns this).

Unlike the vehicles/drivers capture sets, no record values here are
scrubbed live values -- every uuid, coordinate, asset/operator/address
id, timestamp, duration, consumption, and cost is synthetic outright.
What IS verbatim wire truth: the envelope shape, the camelCase key set,
the RFC3339 ``startTime`` string shape, the NO-end-key interval shape
(start plus ``durationMilliseconds``), the bare-int
``asset.id``/``operator.id`` beside the STRING ``address.id``, the
``ptoState: "inactive"`` value (the only value observed in 2,200
records), the string-money ``{amount, currency}`` blocks, the
int|float mixing on ``fuelConsumedMilliliters``, the terminal
``hasNextPage: false`` beside an empty-string ``endCursor`` -- and both
400 message texts, verbatim JSON (requestIds synthetic).

Consumed by the IdlingEvent model tests and the idling_events endpoint
tests -- kept as a helper module under ``tests/`` so consumers share
one capture set (the ``samsara_vehicles_capture`` precedent). The raw
JSON literals are the captures; the parsed objects beside them are what
tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# Captured: a continuation page (2026-07-20; production limit 200, two
# records committed -- the maximal and minimal variants). The cursor
# walk was proven live: 11 pages at limit=200, 2,200/2,200 unique, a
# fresh endCursor per page, the standard terminal.
IDLING_EVENTS_PAGE_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "eventUuid": "00000000-0000-4000-8000-000000000001",
            "startTime": "2026-01-01T12:34:56Z",
            "durationMilliseconds": 930000,
            "asset": {
                "id": 90000001
            },
            "operator": {
                "id": 91000001
            },
            "latitude": 40.2001,
            "longitude": -100.2001,
            "ptoState": "inactive",
            "fuelConsumedMilliliters": 2500,
            "fuelCost": {
                "amount": "1.87",
                "currency": "usd"
            },
            "gaseousFuelConsumedGrams": 0,
            "gaseousFuelCost": {
                "amount": "0.00",
                "currency": "usd"
            },
            "airTemperatureMillicelsius": 23500,
            "address": {
                "id": "88000001",
                "addressTypes": [
                    "yard"
                ]
            }
        },
        {
            "eventUuid": "00000000-0000-4000-8000-000000000002",
            "startTime": "2026-01-01T15:00:00Z",
            "durationMilliseconds": 300000,
            "asset": {
                "id": 90000002
            },
            "latitude": 40.2051,
            "longitude": -100.2051,
            "ptoState": "inactive",
            "fuelConsumedMilliliters": 800,
            "fuelCost": {
                "amount": "0.60",
                "currency": "usd"
            },
            "gaseousFuelConsumedGrams": 0,
            "gaseousFuelCost": {
                "amount": "0.00",
                "currency": "usd"
            }
        }
    ],
    "pagination": {
        "endCursor": "00000000-0000-0000-0000-000000000031",
        "hasNextPage": true
    }
}"""

IDLING_EVENTS_PAGE_RESPONSE: dict[str, JsonValue] = json.loads(
    IDLING_EVENTS_PAGE_RESPONSE_JSON
)

# Captured: the terminal page shape (2026-07-20) -- hasNextPage false
# beside an empty-string endCursor, the provider-wide cursor contract
# proven per-type on the 11-page walk -- carrying the mixed-float
# variant: fuelConsumedMilliliters arrived as BOTH int and float across
# the census, so the model types it float and this record pins the
# float wire shape.
IDLING_EVENTS_TERMINAL_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "eventUuid": "00000000-0000-4000-8000-000000000003",
            "startTime": "2026-01-02T08:15:30Z",
            "durationMilliseconds": 5400000,
            "asset": {
                "id": 90000003
            },
            "operator": {
                "id": 91000002
            },
            "latitude": 40.2101,
            "longitude": -100.2101,
            "ptoState": "inactive",
            "fuelConsumedMilliliters": 123.5,
            "fuelCost": {
                "amount": "0.09",
                "currency": "usd"
            },
            "gaseousFuelConsumedGrams": 0,
            "gaseousFuelCost": {
                "amount": "0.00",
                "currency": "usd"
            },
            "airTemperatureMillicelsius": 21000
        }
    ],
    "pagination": {
        "endCursor": "",
        "hasNextPage": false
    }
}"""

IDLING_EVENTS_TERMINAL_RESPONSE: dict[str, JsonValue] = json.loads(
    IDLING_EVENTS_TERMINAL_RESPONSE_JSON
)

# Captured: the HTTP 400 body for limit=512 (2026-07-20) -- the message
# text is verbatim wire vocabulary; the requestId is synthetic. The
# per-endpoint limit maximum is 200, NOT the 512 of vehicles/drivers:
# the first captured instance of Samsara's per-endpoint limit tiers --
# never assume a sibling's limit.
IDLING_EVENTS_LIMIT_ERROR_RESPONSE_JSON: str = r"""
{
    "message": "limit must be lesser or equal than 200 but got value 512",
    "requestId": "req-synthetic-000000000002"
}"""

IDLING_EVENTS_LIMIT_ERROR_RESPONSE: dict[str, JsonValue] = json.loads(
    IDLING_EVENTS_LIMIT_ERROR_RESPONSE_JSON
)

# Captured: the HTTP 400 body for a window wider than the sub-3-months
# range cap (2026-07-20; a 91-day window was accepted, 180 days returns
# this) -- the message text is verbatim; the requestId is synthetic.
# Loud JSON, never a silent truncation -- and NOT the text/plain
# rpc-error posture of the legacy v1 trips surface.
IDLING_EVENTS_RANGE_CAP_ERROR_RESPONSE_JSON: str = r"""
{
    "message": "Total duration must be less than 3 months.",
    "requestId": "req-synthetic-000000000003"
}"""

IDLING_EVENTS_RANGE_CAP_ERROR_RESPONSE: dict[str, JsonValue] = json.loads(
    IDLING_EVENTS_RANGE_CAP_ERROR_RESPONSE_JSON
)


def _envelope_records(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    result = envelope['data']
    assert isinstance(result, list)
    records: list[JsonObject] = []
    for record in result:
        assert isinstance(record, dict)
        records.append(record)
    return records


# All three committed records, in capture order: maximal, minimal,
# mixed-float.
IDLING_EVENT_RECORDS: list[JsonObject] = _envelope_records(
    IDLING_EVENTS_PAGE_RESPONSE
) + _envelope_records(IDLING_EVENTS_TERMINAL_RESPONSE)
