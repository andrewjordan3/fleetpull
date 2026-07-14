"""Deterministic GeoTab Device fixtures modeled on provider wire shapes.

The seek-walk boundary fixture, six Device records, trailer-shape record,
terminal empty page, and ``GetCountOf`` envelope use purpose-built test
values. They preserve the structural and pagination properties exercised
by the tests: distinct device-generation shapes, missing nested blocks,
trailer-only fields, required sentinels, ids strictly ascending, page 2's
request offset equal to page 1's last record id, hex-consecutive boundary
ids, terminal empty-page behavior, and a count above the provider cap.

Shared by the seek-decoder tests, the Device model tests, and the
fetch/Sync end-to-end tests -- a multi-consumer fixture set, so it lives
in one helper module under ``tests/`` instead of being duplicated beside
each consumer. The JSON literals are the designed wire-shaped fixtures;
the parsed objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# Fixture: Authenticate success in the shared JSON-RPC envelope shape.
# The deterministic credential values are shared with classifier and ingress tests.
AUTHENTICATE_SUCCESS_JSON: str = (
    '{"result": {"credentials": {"database": "exampledb", "sessionId":'
    ' "SyntheticSessionId000001", "userName": "user@example.com"},'
    ' "path": "ThisServer"}, "jsonrpc": "2.0"}'
)

# Fixture: seek walk page 1 request (resultsLimit 3 -- the
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

# Fixture: seek walk page 1 response -- two GO7-era shapes and one GO9
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
            "engineHourOffset": 100.0,
            "isActiveTrackingEnabled": false,
            "forceActiveTracking": true,
            "isDriverSeatbeltWarningOn": true,
            "isPassengerSeatbeltWarningOn": false,
            "isReverseDetectOn": false,
            "isIoxConnectionEnabled": true,
            "odometerFactor": 1,
            "odometerOffset": 1000.0,
            "rpmValue": 3500,
            "seatbeltWarningSpeed": 10,
            "activeFrom": "2026-01-01T00:00:00.000Z",
            "activeTo": "2026-06-01T00:00:00.000Z",
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
            "engineVehicleIdentificationNumber": "4SYNTHV1N00000001",
            "ensureHotStart": false,
            "gpsOffDelay": 0,
            "licensePlate": "SYN-001",
            "licenseState": "",
            "major": 28,
            "minor": 42,
            "parameterVersion": 10,
            "pinDevice": true,
            "vehicleIdentificationNumber": "4SYNTHV1N00000001",
            "parameterVersionOnDevice": 7,
            "comment": "",
            "groups": [
                {
                    "id": "GroupVehicleId"
                },
                {
                    "id": "bE001"
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
            "id": "b101",
            "ignoreDownloadsUntil": "1986-01-01T00:00:00.000Z",
            "communicationThresholdIntervalMoving": 86400,
            "communicationThresholdIntervalStationary": 86400,
            "maxSecondsBetweenLogs": 200,
            "name": "synthetic-unit-001",
            "productId": 111,
            "serialNumber": "SYNTHSERIAL001",
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
            "engineHourOffset": 200.0,
            "isActiveTrackingEnabled": false,
            "forceActiveTracking": true,
            "isDriverSeatbeltWarningOn": true,
            "isPassengerSeatbeltWarningOn": false,
            "isReverseDetectOn": false,
            "isIoxConnectionEnabled": true,
            "odometerFactor": 1,
            "odometerOffset": 2000.0,
            "rpmValue": 3500,
            "seatbeltWarningSpeed": 10,
            "activeFrom": "2026-01-02T00:00:00.000Z",
            "activeTo": "2026-06-02T00:00:00.000Z",
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
            "engineVehicleIdentificationNumber": "4SYNTHV1N00000002",
            "ensureHotStart": false,
            "gpsOffDelay": 0,
            "licensePlate": "SYN-002",
            "licenseState": "",
            "major": 29,
            "minor": 47,
            "parameterVersion": 9,
            "pinDevice": true,
            "vehicleIdentificationNumber": "4SYNTHV1N00000002",
            "parameterVersionOnDevice": 6,
            "comment": "",
            "groups": [
                {
                    "id": "GroupVehicleId"
                },
                {
                    "id": "bE001"
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
            "id": "b102",
            "ignoreDownloadsUntil": "1986-01-01T00:00:00.000Z",
            "communicationThresholdIntervalMoving": 86400,
            "communicationThresholdIntervalStationary": 86400,
            "maxSecondsBetweenLogs": 200,
            "name": "synthetic-unit-002",
            "productId": 111,
            "serialNumber": "SYNTHSERIAL002",
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
            "engineHourOffset": 300.0,
            "isActiveTrackingEnabled": false,
            "forceActiveTracking": true,
            "isDriverSeatbeltWarningOn": true,
            "isPassengerSeatbeltWarningOn": false,
            "isReverseDetectOn": false,
            "isIoxConnectionEnabled": true,
            "odometerFactor": 1,
            "odometerOffset": 3000.0,
            "rpmValue": 3500,
            "seatbeltWarningSpeed": 6,
            "activeFrom": "2026-01-03T00:00:00.000Z",
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
                    "id": "bE003"
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
            "engineVehicleIdentificationNumber": "4SYNTHV1N00000003",
            "ensureHotStart": false,
            "gpsOffDelay": 0,
            "licensePlate": "SYN-003",
            "licenseState": "IN",
            "major": 45,
            "minor": 42,
            "parameterVersion": 24,
            "pinDevice": true,
            "vehicleIdentificationNumber": "4SYNTHV1N00000003",
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
                    "id": "bE003"
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
            "id": "b105",
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

# Fixture: seek walk page 2 request -- its sort.offset equals page 1's
# last record id (the no-loss/no-overlap seam; 0xb105 + 1 == 0xb106).
SEEK_PAGE_2_REQUEST_JSON: str = r"""
{
    "method": "Get",
    "params": {
        "typeName": "Device",
        "resultsLimit": 3,
        "sort": {
            "sortBy": "id",
            "sortDirection": "asc",
            "offset": "b105"
        },
        "credentials": {
            "database": "exampledb",
            "userName": "user@example.com",
            "sessionId": "SyntheticSessionId000001"
        }
    }
}"""

SEEK_PAGE_2_REQUEST: dict[str, JsonValue] = json.loads(SEEK_PAGE_2_REQUEST_JSON)

# Fixture: seek walk page 2 response -- three GO9s, two of which carry
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
            "engineHourOffset": 400.0,
            "isActiveTrackingEnabled": false,
            "forceActiveTracking": true,
            "isDriverSeatbeltWarningOn": true,
            "isPassengerSeatbeltWarningOn": false,
            "isReverseDetectOn": false,
            "isIoxConnectionEnabled": true,
            "odometerFactor": 1,
            "odometerOffset": 4000.0,
            "rpmValue": 3500,
            "seatbeltWarningSpeed": 10,
            "activeFrom": "2026-01-04T00:00:00.000Z",
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
            "engineVehicleIdentificationNumber": "4SYNTHV1N00000004",
            "ensureHotStart": false,
            "gpsOffDelay": 0,
            "licensePlate": "SYN-004",
            "licenseState": "",
            "major": 45,
            "minor": 44,
            "parameterVersion": 18,
            "pinDevice": true,
            "vehicleIdentificationNumber": "4SYNTHV1N00000004",
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
                    "id": "bE002"
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
            "id": "b106",
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
            "engineHourOffset": 500.0,
            "isActiveTrackingEnabled": false,
            "forceActiveTracking": true,
            "isDriverSeatbeltWarningOn": true,
            "isPassengerSeatbeltWarningOn": false,
            "isReverseDetectOn": false,
            "isIoxConnectionEnabled": true,
            "odometerFactor": 1,
            "odometerOffset": 5000.0,
            "rpmValue": 3500,
            "seatbeltWarningSpeed": 10,
            "activeFrom": "2026-01-05T00:00:00.000Z",
            "activeTo": "2026-06-05T00:00:00.000Z",
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
            "engineVehicleIdentificationNumber": "4SYNTHV1N00000005",
            "ensureHotStart": false,
            "gpsOffDelay": 0,
            "licensePlate": "SYN-005",
            "licenseState": "",
            "major": 37,
            "minor": 20,
            "parameterVersion": 16,
            "pinDevice": true,
            "vehicleIdentificationNumber": "4SYNTHV1N00000005",
            "parameterVersionOnDevice": 10,
            "comment": "",
            "groups": [
                {
                    "id": "GroupVehicleId"
                },
                {
                    "id": "bE001"
                }
            ],
            "timeZoneId": "America/Los_Angeles",
            "deviceType": "GO9",
            "id": "b107",
            "ignoreDownloadsUntil": "1986-01-01T00:00:00.000Z",
            "communicationThresholdIntervalMoving": 86400,
            "communicationThresholdIntervalStationary": 86400,
            "maxSecondsBetweenLogs": 200,
            "name": "synthetic-unit-005",
            "productId": 120,
            "serialNumber": "SYNTHSERIAL005",
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
            "engineHourOffset": 600.0,
            "isActiveTrackingEnabled": false,
            "forceActiveTracking": true,
            "isDriverSeatbeltWarningOn": true,
            "isPassengerSeatbeltWarningOn": false,
            "isReverseDetectOn": false,
            "isIoxConnectionEnabled": true,
            "odometerFactor": 1,
            "odometerOffset": 6000.0,
            "rpmValue": 3500,
            "seatbeltWarningSpeed": 10,
            "activeFrom": "2026-01-06T00:00:00.000Z",
            "activeTo": "2026-06-06T00:00:00.000Z",
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
                    "id": "bE003"
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
            "engineVehicleIdentificationNumber": "4SYNTHV1N00000006",
            "ensureHotStart": false,
            "gpsOffDelay": 0,
            "licensePlate": "SYN-006",
            "licenseState": "IN",
            "major": 39,
            "minor": 27,
            "parameterVersion": 21,
            "pinDevice": true,
            "vehicleIdentificationNumber": "4SYNTHV1N00000006",
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
                    "id": "bE003"
                }
            ],
            "timeZoneId": "America/Los_Angeles",
            "deviceType": "GO9",
            "id": "b10A",
            "ignoreDownloadsUntil": "1986-01-01T00:00:00.000Z",
            "communicationThresholdIntervalMoving": 86400,
            "communicationThresholdIntervalStationary": 86400,
            "maxSecondsBetweenLogs": 200,
            "name": "synthetic-unit-006",
            "productId": 120,
            "serialNumber": "SYNTHSERIAL006",
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

# Fixture: the terminal empty page -- the only termination signal seek paging has.
SEEK_TERMINAL_RESPONSE_JSON: str = r"""
{
    "result": [],
    "jsonrpc": "2.0"
}"""

SEEK_TERMINAL_RESPONSE: dict[str, JsonValue] = json.loads(SEEK_TERMINAL_RESPONSE_JSON)

# Fixture: trailer-shape Device record (deviceType "None", productId -1,
# tmpTrailerId; the VIN sentinels "" and the literal "?" live here).
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
    "activeFrom": "2026-02-01T00:00:00.000Z",
    "activeTo": "2050-01-01T00:00:00.000Z",
    "comment": "",
    "groups": [
        {
            "id": "GroupTrailerId"
        }
    ],
    "timeZoneId": "America/New_York",
    "deviceType": "None",
    "id": "b179",
    "ignoreDownloadsUntil": "1986-01-01T00:00:00.000Z",
    "communicationThresholdIntervalMoving": 86400,
    "communicationThresholdIntervalStationary": 86400,
    "maxSecondsBetweenLogs": 200,
    "name": "synthetic-trailer-001",
    "productId": -1,
    "serialNumber": "",
    "timeToDownload": "1.00:00:00",
    "workTime": "WorkTimeStandardHoursId",
    "customProperties": [],
    "mediaFiles": [],
    "tmpTrailerId": "SynthTmpTrailerId000001"
}"""

TRAILER_DEVICE_RECORD: JsonObject = json.loads(TRAILER_DEVICE_RECORD_JSON)

# Fixture: GetCountOf response (2026-07-09) -- the completeness guard's
# slice-model fixture; a purpose-built count above the capped 5,000 Get
# preserves the completeness-check branch that forced seek paging.
GET_COUNT_OF_RESPONSE_JSON: str = r"""
{
    "result": 5100,
    "jsonrpc": "2.0"
}"""

GET_COUNT_OF_RESPONSE: dict[str, JsonValue] = json.loads(GET_COUNT_OF_RESPONSE_JSON)


def _result_records(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    """Narrow a fixture Get envelope's ``result`` to its record list.

    The fixtures are known-good record lists; the asserts exist for the
    type checker (and would fail loudly if a fixture were ever edited
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
