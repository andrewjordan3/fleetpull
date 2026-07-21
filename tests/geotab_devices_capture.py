"""The committed GeoTab devices capture set (2026-07-09 probe session).

The seek-walk boundary fixture and its six Device records, the trailer-
shape record, the terminal empty page, and the ``GetCountOf`` envelope
-- all Captured from live GeoTab and scrubbed per the Data Hygiene
convention before commit (ids are pure inventions carrying no mapping
to any real identifier; names and VINs synthetic; the structural
properties are what the scrub preserves: ids strictly ascending, page
2's request offset equal to page 1's last record id, the boundary ids
hex-consecutive, distinctness and cross-module device references
intact).

Shared by the seek-decoder tests, the Device model tests, and the
fetch/Sync end-to-end tests -- a multi-consumer capture set, so it lives
in one helper module under ``tests/`` (the ``serial_executor``
precedent) instead of being duplicated beside each consumer. The raw
JSON literals are the captures verbatim (raw strings: one captured
value carries an escaped quote); the parsed objects beside them are
what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# Captured: Authenticate success (June 2026; the harness-pattern fixture,
# same capture as the classifier and ingress test modules carry).
AUTHENTICATE_SUCCESS_JSON: str = (
    '{"result": {"credentials": {"database": "exampledb", "sessionId":'
    ' "SyntheticSessionId000001", "userName": "user@example.com"},'
    ' "path": "ThisServer"}, "jsonrpc": "2.0"}'
)

# Captured: seek walk page 1 request (2026-07-09, resultsLimit 3 -- the
# walk's parameter, not the mechanism; production uses 5000). The
# authoritative request-key placement: sort inside params, sortBy id,
# an EXPLICIT null offset on the first page, credentials injected by
# the session strategy.
SEEK_PAGE_1_REQUEST_JSON: str = r"""
{
    "method": "Get",
    "params": {
        "typeName": "Device",
        "resultsLimit": 3,
        "sort": {
            "sortBy": "id",
            "sortDirection": "asc",
            "offset": null
        },
        "credentials": {
            "database": "exampledb",
            "userName": "user@example.com",
            "sessionId": "SyntheticSessionId000001"
        }
    }
}"""

SEEK_PAGE_1_REQUEST: dict[str, JsonValue] = json.loads(SEEK_PAGE_1_REQUEST_JSON)

# Captured: seek walk page 1 response -- two GO7-era shapes and one GO9
# (HTTP 200; ids strictly ascending).
SEEK_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": [
        {
            "auxWarningSpeed": [
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            ],
            "enableAuxWarning": [
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false
            ],
            "enableControlExternalRelay": false,
            "externalDeviceShutDownDelay": 10,
            "immobilizeArming": 30,
            "immobilizeUnit": true,
            "isAuxIgnTrigger": [
                false,
                false,
                false,
                false
            ],
            "isAuxInverted": [
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false
            ],
            "accelerationWarningThreshold": 16,
            "accelerometerThresholdWarningFactor": 0,
            "brakingWarningThreshold": -26,
            "corneringWarningThreshold": 18,
            "enableBeepOnDangerousDriving": true,
            "enableBeepOnRpm": true,
            "engineHourOffset": 0,
            "isActiveTrackingEnabled": false,
            "forceActiveTracking": true,
            "isDriverSeatbeltWarningOn": true,
            "isPassengerSeatbeltWarningOn": false,
            "isReverseDetectOn": false,
            "isIoxConnectionEnabled": true,
            "odometerFactor": 1,
            "odometerOffset": -1250.7679994106293,
            "rpmValue": 3500,
            "seatbeltWarningSpeed": 10,
            "activeFrom": "2017-09-12T13:58:56.000Z",
            "activeTo": "2021-11-05T13:35:43.215Z",
            "disableBuzzer": false,
            "enableBeepOnIdle": false,
            "enableSpeedWarning": false,
            "engineType": "EngineTypeGenericId",
            "idleMinutes": 3,
            "isSpeedIndicator": true,
            "minAccidentSpeed": 3,
            "speedingOff": 105,
            "speedingOn": 113,
            "goTalkLanguage": "English",
            "fuelTankCapacity": 0,
            "autoHos": "ON",
            "autoGroups": [],
            "customParameters": [],
            "enableMustReprogram": false,
            "engineVehicleIdentificationNumber": "4SYNTHV1N00000017",
            "ensureHotStart": false,
            "gpsOffDelay": 0,
            "licensePlate": "0000001",
            "licenseState": "",
            "major": 28,
            "minor": 42,
            "parameterVersion": 10,
            "pinDevice": true,
            "vehicleIdentificationNumber": "4SYNTHV1N00000017",
            "parameterVersionOnDevice": 7,
            "comment": "",
            "groups": [
                {
                    "id": "GroupVehicleId"
                },
                {
                    "id": "b44A1"
                }
            ],
            "timeZoneId": "America/Los_Angeles",
            "deviceFlags": {
                "activeFeatures": [
                    "GeotabDriveHos"
                ],
                "isActiveTrackingAllowed": false,
                "isContinuousConnectAllowed": false,
                "isEngineAllowed": true,
                "isGarminAllowed": true,
                "isHOSAllowed": true,
                "isIridiumAllowed": true,
                "isOdometerAllowed": true,
                "isTripDetailAllowed": true,
                "isUIAllowed": true,
                "isVINAllowed": true,
                "ratePlans": []
            },
            "deviceType": "GO7",
            "id": "b8E2",
            "ignoreDownloadsUntil": "1986-01-01T00:00:00.000Z",
            "communicationThresholdIntervalMoving": 86400,
            "communicationThresholdIntervalStationary": 86400,
            "maxSecondsBetweenLogs": 200,
            "name": "synthetic-unit-001",
            "productId": 111,
            "serialNumber": "000-000-0000",
            "timeToDownload": "1.00:00:00",
            "workTime": "WorkTimeStandardHoursId",
            "devicePlans": [
                "Pro"
            ],
            "devicePlanBillingInfo": [
                {
                    "billingLevel": 1,
                    "devicePlanName": "Pro"
                }
            ],
            "customFeatures": {
                "autoHos": true
            },
            "customProperties": [],
            "mediaFiles": []
        },
        {
            "auxWarningSpeed": [
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            ],
            "enableAuxWarning": [
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false
            ],
            "enableControlExternalRelay": false,
            "externalDeviceShutDownDelay": 10,
            "immobilizeArming": 30,
            "immobilizeUnit": true,
            "isAuxIgnTrigger": [
                false,
                false,
                false,
                false
            ],
            "isAuxInverted": [
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false
            ],
            "accelerationWarningThreshold": 16,
            "accelerometerThresholdWarningFactor": 0,
            "brakingWarningThreshold": -26,
            "corneringWarningThreshold": 18,
            "enableBeepOnDangerousDriving": true,
            "enableBeepOnRpm": true,
            "engineHourOffset": 0,
            "isActiveTrackingEnabled": false,
            "forceActiveTracking": true,
            "isDriverSeatbeltWarningOn": true,
            "isPassengerSeatbeltWarningOn": false,
            "isReverseDetectOn": false,
            "isIoxConnectionEnabled": true,
            "odometerFactor": 1,
            "odometerOffset": 1500.2080001831055,
            "rpmValue": 3500,
            "seatbeltWarningSpeed": 10,
            "activeFrom": "2017-09-12T13:58:56.000Z",
            "activeTo": "2021-10-05T22:09:06.254Z",
            "disableBuzzer": false,
            "enableBeepOnIdle": false,
            "enableSpeedWarning": false,
            "engineType": "EngineTypeGenericId",
            "idleMinutes": 3,
            "isSpeedIndicator": true,
            "minAccidentSpeed": 3,
            "speedingOff": 105,
            "speedingOn": 113,
            "goTalkLanguage": "English",
            "fuelTankCapacity": 0,
            "autoHos": "ON",
            "autoGroups": [],
            "customParameters": [],
            "enableMustReprogram": false,
            "engineVehicleIdentificationNumber": "4SYNTHV1N00000009",
            "ensureHotStart": false,
            "gpsOffDelay": 0,
            "licensePlate": "0000002",
            "licenseState": "",
            "major": 29,
            "minor": 47,
            "parameterVersion": 9,
            "pinDevice": true,
            "vehicleIdentificationNumber": "4SYNTHV1N00000009",
            "parameterVersionOnDevice": 6,
            "comment": "",
            "groups": [
                {
                    "id": "GroupVehicleId"
                },
                {
                    "id": "b44A1"
                }
            ],
            "timeZoneId": "America/Los_Angeles",
            "deviceFlags": {
                "activeFeatures": [
                    "GeotabDriveHos"
                ],
                "isActiveTrackingAllowed": false,
                "isContinuousConnectAllowed": false,
                "isEngineAllowed": true,
                "isGarminAllowed": true,
                "isHOSAllowed": true,
                "isIridiumAllowed": true,
                "isOdometerAllowed": true,
                "isTripDetailAllowed": true,
                "isUIAllowed": true,
                "isVINAllowed": true,
                "ratePlans": []
            },
            "deviceType": "GO7",
            "id": "b8E7",
            "ignoreDownloadsUntil": "1986-01-01T00:00:00.000Z",
            "communicationThresholdIntervalMoving": 86400,
            "communicationThresholdIntervalStationary": 86400,
            "maxSecondsBetweenLogs": 200,
            "name": "synthetic-unit-002",
            "productId": 111,
            "serialNumber": "000-000-0000",
            "timeToDownload": "1.00:00:00",
            "workTime": "WorkTimeStandardHoursId",
            "devicePlans": [
                "Pro"
            ],
            "devicePlanBillingInfo": [
                {
                    "billingLevel": 1,
                    "devicePlanName": "Pro"
                }
            ],
            "customFeatures": {
                "autoHos": true
            },
            "customProperties": [],
            "mediaFiles": []
        },
        {
            "isContinuousConnectEnabled": false,
            "obdAlertEnabled": false,
            "auxWarningSpeed": [
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            ],
            "enableAuxWarning": [
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false
            ],
            "enableControlExternalRelay": false,
            "externalDeviceShutDownDelay": 10,
            "immobilizeArming": 30,
            "immobilizeUnit": true,
            "isAuxIgnTrigger": [
                false,
                false,
                false,
                false
            ],
            "isAuxInverted": [
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false
            ],
            "accelerationWarningThreshold": 16,
            "accelerometerThresholdWarningFactor": 0,
            "brakingWarningThreshold": -26,
            "corneringWarningThreshold": 18,
            "enableBeepOnDangerousDriving": true,
            "enableBeepOnRpm": true,
            "engineHourOffset": 0,
            "isActiveTrackingEnabled": false,
            "forceActiveTracking": true,
            "isDriverSeatbeltWarningOn": true,
            "isPassengerSeatbeltWarningOn": false,
            "isReverseDetectOn": false,
            "isIoxConnectionEnabled": true,
            "odometerFactor": 1,
            "odometerOffset": -1700.8479996919632,
            "rpmValue": 3500,
            "seatbeltWarningSpeed": 6,
            "activeFrom": "2024-09-10T22:44:05.119Z",
            "activeTo": "2050-01-01T00:00:00.000Z",
            "disableBuzzer": false,
            "enableBeepOnIdle": false,
            "enableSpeedWarning": false,
            "engineType": "EngineTypeGenericId",
            "idleMinutes": 3,
            "isSpeedIndicator": true,
            "minAccidentSpeed": 2,
            "speedingOff": 121,
            "speedingOn": 129,
            "goTalkLanguage": "English",
            "fuelTankCapacity": 0,
            "disableSleeperBerth": false,
            "autoHos": "ON",
            "wifiHotspotLimits": [],
            "autoGroups": [
                {
                    "id": "b44C2"
                }
            ],
            "customParameters": [
                {
                    "bytes": "AA==",
                    "description": "Enable Driver Violation Alarm Duration",
                    "isEnabled": true,
                    "offset": 182
                }
            ],
            "enableMustReprogram": false,
            "engineVehicleIdentificationNumber": "4SYNTHV1N00000021",
            "ensureHotStart": false,
            "gpsOffDelay": 0,
            "licensePlate": "0000003",
            "licenseState": "IN",
            "major": 45,
            "minor": 42,
            "parameterVersion": 24,
            "pinDevice": true,
            "vehicleIdentificationNumber": "4SYNTHV1N00000021",
            "vinInfoMake": "Freightliner",
            "vinInfoModel": "New Cascadia 126\" Day cab",
            "vinInfoYear": "2020",
            "vinInfoVehicleType": 15,
            "parameterVersionOnDevice": 24,
            "comment": "",
            "groups": [
                {
                    "id": "GroupDieselId"
                },
                {
                    "id": "GroupVehicleId"
                },
                {
                    "id": "b44C2"
                }
            ],
            "timeZoneId": "America/Los_Angeles",
            "deviceFlags": {
                "activeFeatures": [
                    "GoActive",
                    "GeotabDriveHos"
                ],
                "isActiveTrackingAllowed": true,
                "isContinuousConnectAllowed": false,
                "isEngineAllowed": true,
                "isGarminAllowed": true,
                "isHOSAllowed": true,
                "isIridiumAllowed": true,
                "isOdometerAllowed": true,
                "isTripDetailAllowed": true,
                "isUIAllowed": true,
                "isVINAllowed": true,
                "ratePlans": []
            },
            "deviceType": "GO9",
            "id": "b8F3",
            "ignoreDownloadsUntil": "1986-01-01T00:00:00.000Z",
            "maxSecondsBetweenLogs": 200,
            "name": "synthetic-unit-003",
            "productId": 120,
            "serialNumber": "G9SYNTH00001",
            "timeToDownload": "1.00:00:00",
            "workTime": "WorkTimeStandardHoursId",
            "devicePlans": [
                "ProPlus"
            ],
            "devicePlanBillingInfo": [
                {
                    "billingLevel": 10,
                    "devicePlanName": "ProPlus Mode"
                }
            ],
            "customFeatures": {
                "autoHos": true
            },
            "customProperties": [],
            "mediaFiles": []
        }
    ],
    "jsonrpc": "2.0"
}"""

SEEK_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(SEEK_PAGE_1_RESPONSE_JSON)

# Captured: seek walk page 2 request -- its sort.offset equals page 1's
# last record id (the no-loss/no-overlap seam; 0xb8F3 + 1 == 0xb8F4).
SEEK_PAGE_2_REQUEST_JSON: str = r"""
{
    "method": "Get",
    "params": {
        "typeName": "Device",
        "resultsLimit": 3,
        "sort": {
            "sortBy": "id",
            "sortDirection": "asc",
            "offset": "b8F3"
        },
        "credentials": {
            "database": "exampledb",
            "userName": "user@example.com",
            "sessionId": "SyntheticSessionId000001"
        }
    }
}"""

SEEK_PAGE_2_REQUEST: dict[str, JsonValue] = json.loads(SEEK_PAGE_2_REQUEST_JSON)

# Captured: seek walk page 2 response -- three GO9s, two of which carry
# no deviceFlags/devicePlans (shape poverty is two records, not one:
# the model tests treat it as a shape, not a curiosity).
SEEK_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": [
        {
            "isContinuousConnectEnabled": false,
            "obdAlertEnabled": false,
            "auxWarningSpeed": [
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            ],
            "enableAuxWarning": [
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false
            ],
            "enableControlExternalRelay": false,
            "externalDeviceShutDownDelay": 10,
            "immobilizeArming": 30,
            "immobilizeUnit": true,
            "isAuxIgnTrigger": [
                false,
                false,
                false,
                false
            ],
            "isAuxInverted": [
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false
            ],
            "accelerationWarningThreshold": 16,
            "accelerometerThresholdWarningFactor": 0,
            "brakingWarningThreshold": -26,
            "corneringWarningThreshold": 18,
            "enableBeepOnDangerousDriving": true,
            "enableBeepOnRpm": true,
            "engineHourOffset": 0,
            "isActiveTrackingEnabled": false,
            "forceActiveTracking": true,
            "isDriverSeatbeltWarningOn": true,
            "isPassengerSeatbeltWarningOn": false,
            "isReverseDetectOn": false,
            "isIoxConnectionEnabled": true,
            "odometerFactor": 1,
            "odometerOffset": -1899.743999660015,
            "rpmValue": 3500,
            "seatbeltWarningSpeed": 10,
            "activeFrom": "2023-04-19T21:55:09.606Z",
            "activeTo": "2050-01-01T00:00:00.000Z",
            "disableBuzzer": false,
            "enableBeepOnIdle": false,
            "enableSpeedWarning": false,
            "engineType": "EngineTypeGenericId",
            "idleMinutes": 3,
            "isSpeedIndicator": true,
            "minAccidentSpeed": 2,
            "speedingOff": 105,
            "speedingOn": 113,
            "goTalkLanguage": "English",
            "fuelTankCapacity": 0,
            "disableSleeperBerth": false,
            "autoHos": "ON",
            "wifiHotspotLimits": [],
            "autoGroups": [],
            "customParameters": [
                {
                    "bytes": "AA==",
                    "description": "Enable Driver Violation Alarm Duration",
                    "isEnabled": true,
                    "offset": 182
                }
            ],
            "enableMustReprogram": false,
            "engineVehicleIdentificationNumber": "4SYNTHV1N00000013",
            "ensureHotStart": false,
            "gpsOffDelay": 0,
            "licensePlate": "0000004",
            "licenseState": "",
            "major": 45,
            "minor": 44,
            "parameterVersion": 18,
            "pinDevice": true,
            "vehicleIdentificationNumber": "4SYNTHV1N00000013",
            "vinInfoMake": "Freightliner",
            "vinInfoModel": "New Cascadia 126\" Day cab",
            "vinInfoYear": "2019",
            "vinInfoVehicleType": 15,
            "parameterVersionOnDevice": 18,
            "comment": "",
            "groups": [
                {
                    "id": "GroupDieselId"
                },
                {
                    "id": "GroupVehicleId"
                },
                {
                    "id": "b44B7"
                }
            ],
            "timeZoneId": "America/Los_Angeles",
            "deviceFlags": {
                "activeFeatures": [
                    "GoActive",
                    "GeotabDriveHos"
                ],
                "isActiveTrackingAllowed": true,
                "isContinuousConnectAllowed": false,
                "isEngineAllowed": true,
                "isGarminAllowed": true,
                "isHOSAllowed": true,
                "isIridiumAllowed": true,
                "isOdometerAllowed": true,
                "isTripDetailAllowed": true,
                "isUIAllowed": true,
                "isVINAllowed": true,
                "ratePlans": []
            },
            "deviceType": "GO9",
            "id": "b8F4",
            "ignoreDownloadsUntil": "1986-01-01T00:00:00.000Z",
            "maxSecondsBetweenLogs": 200,
            "name": "synthetic-unit-004",
            "productId": 120,
            "serialNumber": "G9SYNTH00002",
            "timeToDownload": "1.00:00:00",
            "workTime": "WorkTimeStandardHoursId",
            "devicePlans": [
                "ProPlus"
            ],
            "devicePlanBillingInfo": [
                {
                    "billingLevel": 10,
                    "devicePlanName": "ProPlus Mode"
                }
            ],
            "customFeatures": {
                "autoHos": true
            },
            "customProperties": [],
            "mediaFiles": []
        },
        {
            "isContinuousConnectEnabled": false,
            "obdAlertEnabled": false,
            "auxWarningSpeed": [
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            ],
            "enableAuxWarning": [
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false
            ],
            "enableControlExternalRelay": false,
            "externalDeviceShutDownDelay": 10,
            "immobilizeArming": 30,
            "immobilizeUnit": true,
            "isAuxIgnTrigger": [
                false,
                false,
                false,
                false
            ],
            "isAuxInverted": [
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false
            ],
            "accelerationWarningThreshold": 16,
            "accelerometerThresholdWarningFactor": 0,
            "brakingWarningThreshold": -26,
            "corneringWarningThreshold": 18,
            "enableBeepOnDangerousDriving": true,
            "enableBeepOnRpm": true,
            "engineHourOffset": 0,
            "isActiveTrackingEnabled": false,
            "forceActiveTracking": true,
            "isDriverSeatbeltWarningOn": true,
            "isPassengerSeatbeltWarningOn": false,
            "isReverseDetectOn": false,
            "isIoxConnectionEnabled": true,
            "odometerFactor": 1,
            "odometerOffset": 1194.4320001006126,
            "rpmValue": 3500,
            "seatbeltWarningSpeed": 10,
            "activeFrom": "2023-10-20T20:16:07.016Z",
            "activeTo": "2023-10-21T19:59:59.639Z",
            "disableBuzzer": true,
            "enableBeepOnIdle": false,
            "enableSpeedWarning": false,
            "engineType": "EngineTypeGenericId",
            "idleMinutes": 3,
            "isSpeedIndicator": true,
            "minAccidentSpeed": 3,
            "speedingOff": 105,
            "speedingOn": 113,
            "goTalkLanguage": "English",
            "fuelTankCapacity": 0,
            "disableSleeperBerth": false,
            "autoHos": "ON",
            "wifiHotspotLimits": [],
            "autoGroups": [],
            "customParameters": [
                {
                    "bytes": "AA==",
                    "description": "Enable Driver Violation Alarm Duration",
                    "isEnabled": true,
                    "offset": 182
                }
            ],
            "enableMustReprogram": false,
            "engineVehicleIdentificationNumber": "4SYNTHV1N00000002",
            "ensureHotStart": false,
            "gpsOffDelay": 0,
            "licensePlate": "0000005",
            "licenseState": "",
            "major": 37,
            "minor": 20,
            "parameterVersion": 16,
            "pinDevice": true,
            "vehicleIdentificationNumber": "4SYNTHV1N00000002",
            "parameterVersionOnDevice": 10,
            "comment": "",
            "groups": [
                {
                    "id": "GroupVehicleId"
                },
                {
                    "id": "b44A1"
                }
            ],
            "timeZoneId": "America/Los_Angeles",
            "deviceType": "GO9",
            "id": "b8F8",
            "ignoreDownloadsUntil": "1986-01-01T00:00:00.000Z",
            "communicationThresholdIntervalMoving": 86400,
            "communicationThresholdIntervalStationary": 86400,
            "maxSecondsBetweenLogs": 200,
            "name": "synthetic-unit-005",
            "productId": 120,
            "serialNumber": "000-000-0000",
            "timeToDownload": "1.00:00:00",
            "workTime": "WorkTimeStandardHoursId",
            "customFeatures": {
                "autoHos": true
            },
            "customProperties": [],
            "mediaFiles": []
        },
        {
            "isContinuousConnectEnabled": false,
            "obdAlertEnabled": false,
            "auxWarningSpeed": [
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            ],
            "enableAuxWarning": [
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false
            ],
            "enableControlExternalRelay": false,
            "externalDeviceShutDownDelay": 10,
            "immobilizeArming": 30,
            "immobilizeUnit": true,
            "isAuxIgnTrigger": [
                false,
                false,
                false,
                false
            ],
            "isAuxInverted": [
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false
            ],
            "accelerationWarningThreshold": 16,
            "accelerometerThresholdWarningFactor": 0,
            "brakingWarningThreshold": -26,
            "corneringWarningThreshold": 18,
            "enableBeepOnDangerousDriving": true,
            "enableBeepOnRpm": true,
            "engineHourOffset": 0,
            "isActiveTrackingEnabled": false,
            "forceActiveTracking": true,
            "isDriverSeatbeltWarningOn": true,
            "isPassengerSeatbeltWarningOn": false,
            "isReverseDetectOn": false,
            "isIoxConnectionEnabled": true,
            "odometerFactor": 1,
            "odometerOffset": -241.71199959516525,
            "rpmValue": 3500,
            "seatbeltWarningSpeed": 10,
            "activeFrom": "2023-04-19T22:03:30.531Z",
            "activeTo": "2023-12-29T21:55:33.257Z",
            "disableBuzzer": false,
            "enableBeepOnIdle": false,
            "enableSpeedWarning": false,
            "engineType": "EngineTypeGenericId",
            "idleMinutes": 3,
            "isSpeedIndicator": true,
            "minAccidentSpeed": 3,
            "speedingOff": 121,
            "speedingOn": 129,
            "goTalkLanguage": "English",
            "fuelTankCapacity": 0,
            "disableSleeperBerth": false,
            "autoHos": "ON",
            "wifiHotspotLimits": [],
            "autoGroups": [
                {
                    "id": "b44C2"
                }
            ],
            "customParameters": [
                {
                    "bytes": "AA==",
                    "description": "Enable Driver Violation Alarm Duration",
                    "isEnabled": true,
                    "offset": 182
                }
            ],
            "enableMustReprogram": false,
            "engineVehicleIdentificationNumber": "4SYNTHV1N00000025",
            "ensureHotStart": false,
            "gpsOffDelay": 0,
            "licensePlate": "0000006",
            "licenseState": "IN",
            "major": 39,
            "minor": 27,
            "parameterVersion": 21,
            "pinDevice": true,
            "vehicleIdentificationNumber": "4SYNTHV1N00000025",
            "parameterVersionOnDevice": 18,
            "comment": "",
            "groups": [
                {
                    "id": "GroupDieselId"
                },
                {
                    "id": "GroupVehicleId"
                },
                {
                    "id": "b44C2"
                }
            ],
            "timeZoneId": "America/Los_Angeles",
            "deviceType": "GO9",
            "id": "b91C",
            "ignoreDownloadsUntil": "1986-01-01T00:00:00.000Z",
            "communicationThresholdIntervalMoving": 86400,
            "communicationThresholdIntervalStationary": 86400,
            "maxSecondsBetweenLogs": 200,
            "name": "synthetic-unit-006 ",
            "productId": 120,
            "serialNumber": "000-000-0000",
            "timeToDownload": "1.00:00:00",
            "workTime": "WorkTimeStandardHoursId",
            "customFeatures": {
                "autoHos": true
            },
            "customProperties": [],
            "mediaFiles": []
        }
    ],
    "jsonrpc": "2.0"
}"""

SEEK_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(SEEK_PAGE_2_RESPONSE_JSON)

# Captured: the terminal empty page, from the same day's full-fleet walk
# (the devices: 0 call) -- the ONLY termination signal seek paging has.
SEEK_TERMINAL_RESPONSE_JSON: str = r"""
{
    "result": [],
    "jsonrpc": "2.0"
}"""

SEEK_TERMINAL_RESPONSE: dict[str, JsonValue] = json.loads(SEEK_TERMINAL_RESPONSE_JSON)

# Captured: trailer-shape Device record (deviceType "None", productId -1,
# tmpTrailerId; the VIN sentinels "" and the literal "?" live here).
# From the saved full-page capture, 2026-07-09.
TRAILER_DEVICE_RECORD_JSON: str = r"""
{
    "vehicleIdentificationNumber": "",
    "engineVehicleIdentificationNumber": "?",
    "odometerFactor": 1,
    "odometerOffset": 0,
    "engineHourOffset": 0,
    "licensePlate": "",
    "licenseState": "",
    "pinDevice": true,
    "autoGroups": [],
    "activeFrom": "2022-02-06T07:12:45.339Z",
    "activeTo": "2050-01-01T00:00:00.000Z",
    "comment": "",
    "groups": [
        {
            "id": "GroupTrailerId"
        }
    ],
    "timeZoneId": "America/New_York",
    "deviceType": "None",
    "id": "b9A4",
    "ignoreDownloadsUntil": "1986-01-01T00:00:00.000Z",
    "communicationThresholdIntervalMoving": 86400,
    "communicationThresholdIntervalStationary": 86400,
    "maxSecondsBetweenLogs": 200,
    "name": "synthetic-unit-007",
    "productId": -1,
    "serialNumber": "",
    "timeToDownload": "1.00:00:00",
    "workTime": "WorkTimeStandardHoursId",
    "customProperties": [],
    "mediaFiles": [],
    "tmpTrailerId": "SynthTmpTrailerId000001"
}"""

TRAILER_DEVICE_RECORD: JsonObject = json.loads(TRAILER_DEVICE_RECORD_JSON)

# Captured: GetCountOf response (2026-07-09) -- the completeness guard's
# slice-model fixture; 5,666 against the same-day capped 5,000 Get is
# the finding that forced seek paging.
GET_COUNT_OF_RESPONSE_JSON: str = r"""
{"result": 5666, "jsonrpc": "2.0"}"""

GET_COUNT_OF_RESPONSE: dict[str, JsonValue] = json.loads(GET_COUNT_OF_RESPONSE_JSON)


def _result_records(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    """Narrow a captured Get envelope's ``result`` to its record list.

    The captures are known-good record lists; the asserts exist for the
    type checker (and would fail loudly if a capture were ever edited
    into a different shape).
    """
    records = envelope['result']
    assert isinstance(records, list)
    narrowed: list[JsonObject] = []
    for record in records:
        assert isinstance(record, dict)
        narrowed.append(record)
    return narrowed


# The six tracked Device records of the seek walk, in walk order -- the
# model tests' mixed-shape set and the e2e page bodies.
DEVICE_RECORDS: list[JsonObject] = [
    *_result_records(SEEK_PAGE_1_RESPONSE),
    *_result_records(SEEK_PAGE_2_RESPONSE),
]
