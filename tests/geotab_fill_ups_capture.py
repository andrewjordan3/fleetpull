"""The synthetic GeoTab fill_ups feed fixture set (2026-07-21 shapes).

FULLY SYNTHETIC — no capture, no credentials: every value is invented in
the probe-settled shapes (the 2026-07-21 feed wave census, DESIGN §8:
100/100 records carried every modeled key, on the ESTIMATES-ONLY tenant
— no fuel-transaction integration, so ``cost`` is 0.0 and
``fuelTransactions`` is an empty list on every record). The envelopes
are the verified GETFEED shape — ``result: {data, toVersion}`` — as an
ADVANCE page (full at the fixtures' page size of 2) and a TERMINAL page
(short), with 16-hex-lowercase version tokens per the machinery's
synthetic-token convention; the page size is 2 where production
declares 10,000 (the trips-capture ``resultsLimit: 3`` precedent).

Variant coverage promised to consumers: BOTH driver arms (the object
reference on records 1 and 3, the bare ``"UnknownDriverId"`` sentinel
on record 2), the observed ``-1.0`` ``derivedVolume`` sentinel (record
2) beside real volumes, the int and float arms of the mixed numerics,
the comma-joined and single-token ``confidence`` shapes, all three
observed ``tankCapacity.source`` tokens, the extrema point triples on
every record, and ``fuelTransactions: []`` riding raw on every record
(unmodeled — the model ignores it).

Shared by the FillUp model tests and the fill_ups endpoint
drive-through (the ``geotab_trips_capture`` precedent). The JSON
literals are the fixtures; the parsed objects beside them are what
tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue
from tests.geotab_feed_pages import feed_records

# Synthetic: the advance page — 2 records (full at the fixtures' page
# size). Record 1 is the full object-driver shape with float arms;
# record 2 is the sentinel record: UnknownDriverId, derivedVolume -1.0,
# int arms, single-token confidence, tankCapacity source 'Unknown'.
FILL_UPS_FEED_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "confidence": "FuelLevel, TripStop",
                "cost": 0.0,
                "currencyCode": "USD",
                "dateTime": "2026-07-14T14:32:10.000Z",
                "derivedVolume": 143.2,
                "device": {
                    "id": "b8D9"
                },
                "distance": 412.7,
                "driver": {
                    "id": "b4B82",
                    "isDriver": true
                },
                "fuelTransactions": [],
                "id": "b16c201",
                "location": {
                    "x": -100.0011,
                    "y": 40.0011
                },
                "odometer": 118203.4,
                "productType": "Unknown",
                "tankCapacity": {
                    "source": "DiagnosticTankCapacity",
                    "volume": 220.5
                },
                "tankLevelExtrema": {
                    "maximaPoint": {
                        "source": "EstimateFuelLevel",
                        "dateTime": "2026-07-14T14:40:00.000Z",
                        "data": 0.95
                    },
                    "minimaPoint": {
                        "source": "EstimateFuelLevel",
                        "dateTime": "2026-07-14T14:20:00.000Z",
                        "data": 0.31
                    }
                },
                "totalFuelUsed": 5231.8,
                "version": "00000000000016c1",
                "volume": 141.9
            },
            {
                "confidence": "FuelLevel",
                "cost": 0.0,
                "currencyCode": "USD",
                "dateTime": "2026-07-15T09:05:44.000Z",
                "derivedVolume": -1.0,
                "device": {
                    "id": "b8F4"
                },
                "distance": 388,
                "driver": "UnknownDriverId",
                "fuelTransactions": [],
                "id": "b16c202",
                "location": {
                    "x": -100.0012,
                    "y": 40.0012
                },
                "odometer": 411906,
                "productType": "Unknown",
                "tankCapacity": {
                    "source": "Unknown",
                    "volume": 200
                },
                "tankLevelExtrema": {
                    "maximaPoint": {
                        "source": "EstimateFuelLevel",
                        "dateTime": "2026-07-15T09:10:00.000Z",
                        "data": 0.88
                    },
                    "minimaPoint": {
                        "source": "EstimateFuelLevel",
                        "dateTime": "2026-07-15T08:55:00.000Z",
                        "data": 0.42
                    }
                },
                "totalFuelUsed": 3120.0,
                "version": "00000000000016c2",
                "volume": 96
            }
        ],
        "toVersion": "00000000000016c2"
    },
    "jsonrpc": "2.0"
}"""

FILL_UPS_FEED_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    FILL_UPS_FEED_PAGE_1_RESPONSE_JSON
)

# Synthetic: the terminal page — 1 record (short at the fixtures' page
# size), the third tankCapacity source token.
FILL_UPS_FEED_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "confidence": "FuelLevel, TripStop",
                "cost": 0.0,
                "currencyCode": "USD",
                "dateTime": "2026-07-15T17:50:03.000Z",
                "derivedVolume": 88.4,
                "device": {
                    "id": "b8D9"
                },
                "distance": 245.1,
                "driver": {
                    "id": "b4B67",
                    "isDriver": true
                },
                "fuelTransactions": [],
                "id": "b16c203",
                "location": {
                    "x": -100.0013,
                    "y": 40.0013
                },
                "odometer": 118615.9,
                "productType": "Unknown",
                "tankCapacity": {
                    "source": "EstimateFuelLevel",
                    "volume": 220.5
                },
                "tankLevelExtrema": {
                    "maximaPoint": {
                        "source": "EstimateFuelLevel",
                        "dateTime": "2026-07-15T17:55:00.000Z",
                        "data": 0.91
                    },
                    "minimaPoint": {
                        "source": "EstimateFuelLevel",
                        "dateTime": "2026-07-15T17:40:00.000Z",
                        "data": 0.5
                    }
                },
                "totalFuelUsed": 5320.2,
                "version": "00000000000016c3",
                "volume": 87.6
            }
        ],
        "toVersion": "00000000000016c3"
    },
    "jsonrpc": "2.0"
}"""

FILL_UPS_FEED_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    FILL_UPS_FEED_PAGE_2_RESPONSE_JSON
)


# The three feed records, in stream order.
FILL_UP_RECORDS: list[JsonObject] = [
    *feed_records(FILL_UPS_FEED_PAGE_1_RESPONSE),
    *feed_records(FILL_UPS_FEED_PAGE_2_RESPONSE),
]

# The designated full-shape record (page 1, first record): every modeled
# field present with the object-form driver -- the mechanical alias-trap
# test iterates the model's fields against it.
FILL_UP_FULL_RECORD: JsonObject = FILL_UP_RECORDS[0]

# The designated sentinel record (page 1, second record): the bare
# UnknownDriverId driver arm and the -1.0 derivedVolume sentinel.
FILL_UP_SENTINEL_RECORD: JsonObject = FILL_UP_RECORDS[1]
