"""The synthetic GeoTab fuel_tax_details feed fixture set (2026-07-21 shapes).

FULLY SYNTHETIC — no capture, no credentials: every value is invented in
the probe-settled shapes (the 2026-07-21 feed wave census, DESIGN §8:
every key present on all sampled records, on the ESTIMATES-ONLY
tenant). The envelopes are the verified GETFEED shape —
``result: {data, toVersion}`` — as an ADVANCE page (full at the
fixtures' page size of 2) and a TERMINAL page (short), with
16-hex-lowercase version tokens per the machinery's synthetic-token
convention; the page size is 2 where production declares 50,000 (the
trips-capture ``resultsLimit: 3`` precedent).

Variant coverage promised to consumers: BOTH driver arms (the object
reference on records 1 and 3, the bare ``"UnknownDriverId"`` sentinel
on record 2), POPULATED hourly arrays beside ``hasHourlyData: true``
(record 1, with the int arm inside ``hourlyOdometer``) and EMPTY hourly
arrays beside ``hasHourlyData: false`` (record 2 — present, zero
elements), the int and float arms of ``enterOdometer``/``exitOdometer``,
and ``versions`` as a list of 16-hex component tokens on every record.

Shared by the FuelTaxDetail model tests and the fuel_tax_details
endpoint drive-through (the ``geotab_trips_capture`` precedent). The
JSON literals are the fixtures; the parsed objects beside them are what
tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue
from tests.geotab_feed_pages import feed_records

# Synthetic: the advance page — 2 records (full at the fixtures' page
# size). Record 1: populated hourly arrays, object driver; record 2:
# EMPTY hourly arrays, UnknownDriverId, int odometer arms.
FUEL_TAX_DETAILS_FEED_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "authority": "KS",
                "device": {
                    "id": "b8D9"
                },
                "driver": {
                    "id": "b4B82",
                    "isDriver": true
                },
                "enterGpsOdometer": 118100.2,
                "enterLatitude": 40.0021,
                "enterLongitude": -100.0021,
                "enterOdometer": 118102.7,
                "enterTime": "2026-07-14T06:00:00.000Z",
                "exitGpsOdometer": 118250.9,
                "exitLatitude": 40.0022,
                "exitLongitude": -100.0022,
                "exitOdometer": 118253.1,
                "exitTime": "2026-07-14T08:30:00.000Z",
                "hasHourlyData": true,
                "hourlyGpsOdometer": [118100.2, 118170.5, 118250.9],
                "hourlyIsOdometerInterpolated": [false, true, false],
                "hourlyLatitude": [40.0021, 40.00215, 40.0022],
                "hourlyLongitude": [-100.0021, -100.00215, -100.0022],
                "hourlyOdometer": [118102.7, 118171, 118253.1],
                "id": "b18e401",
                "isClusterOdometer": false,
                "isEnterOdometerInterpolated": false,
                "isExitOdometerInterpolated": true,
                "isNegligible": false,
                "jurisdiction": "KS",
                "versions": ["00000000000018e1", "00000000000018e2"]
            },
            {
                "authority": "MO",
                "device": {
                    "id": "b8F4"
                },
                "driver": "UnknownDriverId",
                "enterGpsOdometer": 411900.5,
                "enterLatitude": 40.0031,
                "enterLongitude": -100.0031,
                "enterOdometer": 411902,
                "enterTime": "2026-07-15T12:10:00.000Z",
                "exitGpsOdometer": 411903.2,
                "exitLatitude": 40.0032,
                "exitLongitude": -100.0032,
                "exitOdometer": 411904,
                "exitTime": "2026-07-15T12:25:00.000Z",
                "hasHourlyData": false,
                "hourlyGpsOdometer": [],
                "hourlyIsOdometerInterpolated": [],
                "hourlyLatitude": [],
                "hourlyLongitude": [],
                "hourlyOdometer": [],
                "id": "b18e402",
                "isClusterOdometer": true,
                "isEnterOdometerInterpolated": true,
                "isExitOdometerInterpolated": true,
                "isNegligible": true,
                "jurisdiction": "MO",
                "versions": ["00000000000018e3"]
            }
        ],
        "toVersion": "00000000000018e3"
    },
    "jsonrpc": "2.0"
}"""

FUEL_TAX_DETAILS_FEED_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    FUEL_TAX_DETAILS_FEED_PAGE_1_RESPONSE_JSON
)

# Synthetic: the terminal page — 1 record (short at the fixtures' page
# size), a third jurisdiction.
FUEL_TAX_DETAILS_FEED_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "authority": "IA",
                "device": {
                    "id": "b8D9"
                },
                "driver": {
                    "id": "b4B67",
                    "isDriver": true
                },
                "enterGpsOdometer": 118260.0,
                "enterLatitude": 40.0041,
                "enterLongitude": -100.0041,
                "enterOdometer": 118262.4,
                "enterTime": "2026-07-15T15:00:00.000Z",
                "exitGpsOdometer": 118310.6,
                "exitLatitude": 40.0042,
                "exitLongitude": -100.0042,
                "exitOdometer": 118312.9,
                "exitTime": "2026-07-15T16:05:00.000Z",
                "hasHourlyData": true,
                "hourlyGpsOdometer": [118260.0, 118310.6],
                "hourlyIsOdometerInterpolated": [false, false],
                "hourlyLatitude": [40.0041, 40.0042],
                "hourlyLongitude": [-100.0041, -100.0042],
                "hourlyOdometer": [118262.4, 118312.9],
                "id": "b18e403",
                "isClusterOdometer": false,
                "isEnterOdometerInterpolated": false,
                "isExitOdometerInterpolated": false,
                "isNegligible": false,
                "jurisdiction": "IA",
                "versions": ["00000000000018e4"]
            }
        ],
        "toVersion": "00000000000018e4"
    },
    "jsonrpc": "2.0"
}"""

FUEL_TAX_DETAILS_FEED_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    FUEL_TAX_DETAILS_FEED_PAGE_2_RESPONSE_JSON
)


# The three feed records, in stream order.
FUEL_TAX_DETAIL_RECORDS: list[JsonObject] = [
    *feed_records(FUEL_TAX_DETAILS_FEED_PAGE_1_RESPONSE),
    *feed_records(FUEL_TAX_DETAILS_FEED_PAGE_2_RESPONSE),
]

# The designated full-shape record (page 1, first record): every modeled
# field present with the object-form driver and populated hourly arrays
# -- the mechanical alias-trap test iterates the model's fields against
# it.
FUEL_TAX_DETAIL_FULL_RECORD: JsonObject = FUEL_TAX_DETAIL_RECORDS[0]

# The designated empty-arrays record (page 1, second record): the bare
# UnknownDriverId driver arm and every hourly array present but empty.
FUEL_TAX_DETAIL_EMPTY_HOURLY_RECORD: JsonObject = FUEL_TAX_DETAIL_RECORDS[1]
