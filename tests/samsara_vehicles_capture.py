"""The committed Samsara vehicles capture set (2026-07-17 probe session).

Six of the 608 swept ``/fleet/vehicles`` records -- the three
minimal-shape variants (the bare 7-key form; unnamed units carrying
serial-shaped default names) and the three rich variants (the
fully-loaded record, the ``staticAssignedDriver`` carrier, the
``auxInputType1`` carrier; both captured ESN shapes) -- plus a
continuation-page envelope and the captured TERMINAL pagination shape:
``hasNextPage: false`` beside an EMPTY-STRING ``endCursor`` (not
absent, not null). Captured from live Samsara and scrubbed per the Data
Hygiene convention before commit (synthetic ids with their prefixes and
ascending order preserved, unit names, serials with the gateway's
dashed 4-3-3 twin and the serial/externalIds equality classes intact,
VINs on the established 4SYNTHV1N arm, plates, ESNs in both captured
shapes, driver, tag ids/names with the shared-parent equality class and
the double-space quirk; makes/models/years, timestamps, empty-string
``notes``, and the settings vocabulary VERBATIM).

Consumed by the Vehicle model tests -- kept as a helper module under
``tests/`` so future consumers share one capture set (the
``geotab_devices_capture`` precedent). The raw JSON literals are the
captures; the parsed objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# Captured: a continuation page (2026-07-17; production limit 512, six
# records committed). The cursor advance was proven live: ids continued
# ascending across the page boundary with no overlap or loss.
VEHICLES_PAGE_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "harshAccelerationSettingType": "automatic",
            "id": "212000000000001",
            "name": "GSYN-DEF-001",
            "notes": "",
            "vehicleRegulationMode": "regulated",
            "createdAtTime": "2019-04-25T22:37:29Z",
            "updatedAtTime": "2019-04-25T22:37:29Z"
        },
        {
            "harshAccelerationSettingType": "automatic",
            "id": "212000000000002",
            "name": "GSYN-DEF-002",
            "notes": "",
            "vehicleRegulationMode": "regulated",
            "createdAtTime": "2019-04-25T22:37:29Z",
            "updatedAtTime": "2019-04-25T22:37:29Z"
        },
        {
            "harshAccelerationSettingType": "automatic",
            "id": "212000000000003",
            "name": "GSYN-DEF-003",
            "notes": "",
            "vehicleRegulationMode": "regulated",
            "createdAtTime": "2019-04-25T22:37:29Z",
            "updatedAtTime": "2019-04-25T22:37:29Z"
        },
        {
            "cameraSerial": "GCAM-AAA-001",
            "externalIds": {
                "samsara.serial": "GSYNAAA001",
                "samsara.vin": "4SYNTHV1N00000023"
            },
            "gateway": {
                "serial": "GSYN-AAA-001",
                "model": "VG34"
            },
            "harshAccelerationSettingType": "automatic",
            "id": "278000000000001",
            "licensePlate": "SYNPL01",
            "make": "FORD",
            "model": "F-550",
            "name": "U-101 (Utility Truck)",
            "notes": "",
            "serial": "GSYNAAA001",
            "tags": [
                {
                    "id": "3000001",
                    "name": "Unit Group - 01",
                    "parentTagId": "3000000"
                }
            ],
            "vin": "4SYNTHV1N00000023",
            "year": "2013",
            "vehicleRegulationMode": "unregulated",
            "createdAtTime": "2019-09-13T22:32:39Z",
            "updatedAtTime": "2019-09-13T22:32:39Z"
        },
        {
            "cameraSerial": "GCAM-AAA-002",
            "externalIds": {
                "samsara.serial": "GSYNAAA002",
                "samsara.vin": "4SYNTHV1N00000024"
            },
            "gateway": {
                "serial": "GSYN-AAA-002",
                "model": "VG34"
            },
            "harshAccelerationSettingType": "automatic",
            "id": "281000000000001",
            "licensePlate": "SYNPL02",
            "make": "KENWORTH",
            "model": "T680",
            "name": "U-102 (Tractor)",
            "notes": "",
            "serial": "GSYNAAA002",
            "staticAssignedDriver": {
                "id": "7000001",
                "name": "Synthetic Driver001"
            },
            "tags": [
                {
                    "id": "2900001",
                    "name": "GRP  Alpha - 23",
                    "parentTagId": "2900000"
                }
            ],
            "vin": "4SYNTHV1N00000024",
            "year": "2015",
            "vehicleRegulationMode": "regulated",
            "createdAtTime": "2021-07-10T20:11:13Z",
            "updatedAtTime": "2022-12-13T20:04:43Z",
            "esn": "Y000001"
        },
        {
            "auxInputType1": "powerTakeOff",
            "cameraSerial": "GCAM-AAA-003",
            "externalIds": {
                "samsara.serial": "GSYNAAA003",
                "samsara.vin": "4SYNTHV1N00000025"
            },
            "gateway": {
                "serial": "GSYN-AAA-003",
                "model": "VG34"
            },
            "harshAccelerationSettingType": "automatic",
            "id": "281000000000002",
            "licensePlate": "SYNPL03",
            "make": "KENWORTH",
            "model": "T8 SERIES",
            "name": "U-103 (Service Unit )",
            "notes": "",
            "serial": "GSYNAAA003",
            "tags": [
                {
                    "id": "2900002",
                    "name": "Region Beta - 08",
                    "parentTagId": "2900000"
                }
            ],
            "vin": "4SYNTHV1N00000025",
            "year": "2019",
            "vehicleRegulationMode": "regulated",
            "createdAtTime": "2021-07-21T01:17:51Z",
            "updatedAtTime": "2025-08-19T18:03:23Z",
            "esn": "80000001"
        }
    ],
    "pagination": {
        "endCursor": "00000000-0000-0000-0000-000000000018",
        "hasNextPage": true
    }
}"""

VEHICLES_PAGE_RESPONSE: dict[str, JsonValue] = json.loads(VEHICLES_PAGE_RESPONSE_JSON)

# Captured: the terminal page shape (2026-07-17) -- hasNextPage false
# beside an empty-string endCursor, the datum the decoder's
# promised-continuation guard is calibrated against.
VEHICLES_TERMINAL_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "auxInputType1": "powerTakeOff",
            "cameraSerial": "GCAM-AAA-003",
            "externalIds": {
                "samsara.serial": "GSYNAAA003",
                "samsara.vin": "4SYNTHV1N00000025"
            },
            "gateway": {
                "serial": "GSYN-AAA-003",
                "model": "VG34"
            },
            "harshAccelerationSettingType": "automatic",
            "id": "281000000000002",
            "licensePlate": "SYNPL03",
            "make": "KENWORTH",
            "model": "T8 SERIES",
            "name": "U-103 (Service Unit )",
            "notes": "",
            "serial": "GSYNAAA003",
            "tags": [
                {
                    "id": "2900002",
                    "name": "Region Beta - 08",
                    "parentTagId": "2900000"
                }
            ],
            "vin": "4SYNTHV1N00000025",
            "year": "2019",
            "vehicleRegulationMode": "regulated",
            "createdAtTime": "2021-07-21T01:17:51Z",
            "updatedAtTime": "2025-08-19T18:03:23Z",
            "esn": "80000001"
        }
    ],
    "pagination": {
        "endCursor": "",
        "hasNextPage": false
    }
}"""

VEHICLES_TERMINAL_RESPONSE: dict[str, JsonValue] = json.loads(
    VEHICLES_TERMINAL_RESPONSE_JSON
)


def _envelope_records(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    result = envelope['data']
    assert isinstance(result, list)
    records: list[JsonObject] = []
    for record in result:
        assert isinstance(record, dict)
        records.append(record)
    return records


VEHICLE_RECORDS: list[JsonObject] = _envelope_records(VEHICLES_PAGE_RESPONSE)
