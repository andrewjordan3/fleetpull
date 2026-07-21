"""The synthetic GeoTab fuel_and_energy_used feed fixture set (2026-07-21 shapes).

FULLY SYNTHETIC — no capture, no credentials: every value is invented in
the probe-settled shapes (the 2026-07-21 feed wave census, DESIGN §8:
2,000/2,000 records carried all seven keys, on the ESTIMATES-ONLY
tenant — every fuel value provider-derived from telemetry). The
envelopes are the verified GETFEED shape — ``result: {data, toVersion}``
— as an ADVANCE page (full at the fixtures' page size of 2) and a
TERMINAL page (short), with 16-hex-lowercase version tokens per the
machinery's synthetic-token convention; the page size is 2 where
production declares 50,000 (the trips-capture ``resultsLimit: 3``
precedent).

Variant coverage promised to consumers: BOTH observed ``confidence``
tokens (``'None'`` — the 1,994/2,000 shape — and the rare
``'FuelUsedInconsistent'``), and the int and float arms of both mixed
numerics (``totalFuelUsed``, ``totalIdlingFuelUsedL``).

Shared by the FuelAndEnergyUsed model tests and the
fuel_and_energy_used endpoint drive-through (the
``geotab_trips_capture`` precedent). The JSON literals are the
fixtures; the parsed objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue
from tests.geotab_feed_pages import feed_records

# Synthetic: the advance page — 2 records (full at the fixtures' page
# size), both confidence tokens, both numeric arms, two event dates.
FUEL_AND_ENERGY_USED_FEED_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "confidence": "None",
                "dateTime": "2026-07-14T05:47:27.000Z",
                "device": {
                    "id": "b8D9"
                },
                "id": "b17d301",
                "totalFuelUsed": 12.4,
                "totalIdlingFuelUsedL": 1,
                "version": "00000000000017d1"
            },
            {
                "confidence": "FuelUsedInconsistent",
                "dateTime": "2026-07-15T06:12:09.000Z",
                "device": {
                    "id": "b8F4"
                },
                "id": "b17d302",
                "totalFuelUsed": 8,
                "totalIdlingFuelUsedL": 0.6,
                "version": "00000000000017d2"
            }
        ],
        "toVersion": "00000000000017d2"
    },
    "jsonrpc": "2.0"
}"""

FUEL_AND_ENERGY_USED_FEED_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    FUEL_AND_ENERGY_USED_FEED_PAGE_1_RESPONSE_JSON
)

# Synthetic: the terminal page — 1 record (short at the fixtures' page
# size), the dominant 'None' confidence shape.
FUEL_AND_ENERGY_USED_FEED_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "confidence": "None",
                "dateTime": "2026-07-15T18:03:55.000Z",
                "device": {
                    "id": "b8D9"
                },
                "id": "b17d303",
                "totalFuelUsed": 21.75,
                "totalIdlingFuelUsedL": 2.3,
                "version": "00000000000017d3"
            }
        ],
        "toVersion": "00000000000017d3"
    },
    "jsonrpc": "2.0"
}"""

FUEL_AND_ENERGY_USED_FEED_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    FUEL_AND_ENERGY_USED_FEED_PAGE_2_RESPONSE_JSON
)


# The three feed records, in stream order.
FUEL_AND_ENERGY_USED_RECORDS: list[JsonObject] = [
    *feed_records(FUEL_AND_ENERGY_USED_FEED_PAGE_1_RESPONSE),
    *feed_records(FUEL_AND_ENERGY_USED_FEED_PAGE_2_RESPONSE),
]

# The designated full-shape record (page 1, first record): every modeled
# field present -- the mechanical alias-trap test iterates the model's
# fields against it.
FUEL_AND_ENERGY_USED_FULL_RECORD: JsonObject = FUEL_AND_ENERGY_USED_RECORDS[0]
