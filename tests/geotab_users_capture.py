"""The committed GeoTab users capture set (2026-07-16 probe session).

Seven of the 157 swept ``User`` records -- the four captured driver
variants (both ``viewDriversOwnDataOnly`` values, the ``America8Day`` /
``America8DayBig`` HOS variants, the one bare-username login) and the
three non-driver variants (the ``accessGroupFilter`` carrier, a
service-style account, the ``hh:mm:ss tt`` date-format and ``Usd``
variants) -- plus the ``GetCountOf`` envelope. Captured from live GeoTab
and scrubbed per the Data Hygiene convention before commit (ids are
pure inventions carrying no mapping to any real identifier -- ordering
and distinctness are the preserved properties; names, logins, phone and
license and carrier numbers, license-province categoricals, company
identity, and GUIDs synthetic; load-bearing properties preserved: the
driver-only key block present on exactly the ``isDriver: true``
records, absent -- not null -- elsewhere; exactly one
``accessGroupFilter``; the
authority/company equality classes; the 2050 still-active sentinel; all
timestamps, locale fields, and GeoTab vocabulary verbatim).

Consumed by the User model tests -- kept as a helper module under
``tests/`` so future consumers share one capture set (the
``geotab_devices_capture`` precedent). The raw JSON literal is the
capture verbatim; the parsed objects beside it are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# Captured: the 2026-07-16 full-population sweep (bare Get, typeName
# User, 157 records; count == GetCountOf). The committed subset is the
# seven shape variants above, ascending id order preserved.
USERS_RESPONSE_JSON: str = r"""
{
    "result": [
        {
            "driverGroups": [
                {
                    "id": "GroupCompanyId"
                }
            ],
            "keys": [],
            "viewDriversOwnDataOnly": true,
            "licenseProvince": "OH",
            "licenseNumber": "L0000001",
            "isMaintenanceNotificationEnabled": false,
            "isServiceDisruptionNotificationsEnabled": false,
            "smsNotificationsOptIn": false,
            "whatsAppNotificationsOptIn": false,
            "isAceDisclaimerDisabled": false,
            "showRateThisApp": false,
            "featurePreview": "",
            "acceptedEULA": 20,
            "wifiEULA": 0,
            "activeDashboardReports": [],
            "activeDefaultDashboards": [],
            "jobPriorities": [],
            "bookmarks": [],
            "activeFrom": "2020-11-20T17:05:22.587Z",
            "activeTo": "2050-01-01T00:00:00.000Z",
            "availableDashboardReports": [],
            "cannedResponseOptions": [],
            "changePassword": false,
            "comment": "",
            "companyGroups": [
                {
                    "id": "GroupCompanyId"
                }
            ],
            "mediaFiles": [],
            "dateFormat": "MM/dd/yy HH:mm:ss",
            "phoneNumber": "",
            "displayCurrency": "Cad",
            "countryCode": "ca",
            "phoneNumberExtension": "",
            "defaultGoogleMapStyle": "Roadmap",
            "defaultMapEngine": "",
            "defaultOpenStreetMapStyle": "MapBox",
            "defaultHereMapStyle": "Roadmap",
            "defaultPage": "dashboard",
            "designation": "",
            "employeeNo": "",
            "firstName": "Synthetic",
            "fuelEconomyUnit": "MPGUS",
            "electricEnergyEconomyUnit": "MPGEUS",
            "hosRuleSet": "America8DayBig",
            "isYardMoveEnabled": true,
            "isPersonalConveyanceEnabled": true,
            "isExemptHOSEnabled": false,
            "isAdverseDrivingEnabled": true,
            "maxPCDistancePerDay": 0,
            "authorityName": "Example Fleet Services",
            "authorityAddress": "100 Synthetic Blvd Synthetic City CA 00000",
            "id": "b5CE1",
            "isEULAAccepted": true,
            "isNewsEnabled": true,
            "isLabsEnabled": true,
            "isMetric": false,
            "language": "en",
            "firstDayOfWeek": "Monday",
            "lastName": "User001",
            "mapViews": [
                {
                    "name": "North America",
                    "viewport": {
                        "x": -181,
                        "y": 69,
                        "width": 145.5,
                        "height": -52
                    },
                    "highlightGroups": []
                }
            ],
            "name": "user001@example.com",
            "privateUserGroups": [],
            "reportGroups": [],
            "securityGroups": [
                {
                    "id": "GroupDriveUserSecurityId",
                    "name": "**DriveUserSecurity**"
                }
            ],
            "showClickOnceWarning": true,
            "timeZoneId": "America/Los_Angeles",
            "userAuthenticationType": "BasicAuthentication",
            "zoneDisplayMode": "Default",
            "companyName": "Example Fleet Services",
            "companyAddress": "100 Synthetic Blvd Synthetic City CA 00000",
            "carrierNumber": "1000001",
            "lastAccessDate": "2026-07-15T13:16:19.215Z",
            "isDriver": true,
            "isEmailReportEnabled": true,
            "iAMMetadata": {
                "userId": "00000000-0000-0000-0000-000000000011",
                "connectionName": "None",
                "isIAMVerified": true,
                "isWelcomeEmailSent": true
            },
            "isAutoAdded": false
        },
        {
            "driverGroups": [
                {
                    "id": "GroupCompanyId"
                }
            ],
            "keys": [],
            "viewDriversOwnDataOnly": false,
            "licenseProvince": "VT",
            "licenseNumber": "L0000002",
            "isMaintenanceNotificationEnabled": false,
            "isServiceDisruptionNotificationsEnabled": false,
            "smsNotificationsOptIn": false,
            "whatsAppNotificationsOptIn": false,
            "isAceDisclaimerDisabled": false,
            "showRateThisApp": false,
            "featurePreview": "",
            "acceptedEULA": 20,
            "wifiEULA": 0,
            "activeDashboardReports": [],
            "activeDefaultDashboards": [],
            "jobPriorities": [],
            "bookmarks": [],
            "activeFrom": "2022-11-16T17:21:26.956Z",
            "activeTo": "2050-01-01T00:00:00.000Z",
            "availableDashboardReports": [],
            "cannedResponseOptions": [],
            "changePassword": false,
            "comment": "",
            "companyGroups": [
                {
                    "id": "GroupCompanyId"
                }
            ],
            "mediaFiles": [],
            "dateFormat": "MM/dd/yy HH:mm:ss",
            "phoneNumber": "",
            "displayCurrency": "Cad",
            "countryCode": "ca",
            "phoneNumberExtension": "",
            "defaultGoogleMapStyle": "Roadmap",
            "defaultMapEngine": "",
            "defaultOpenStreetMapStyle": "MapBox",
            "defaultHereMapStyle": "Roadmap",
            "defaultPage": "dashboard",
            "designation": "",
            "employeeNo": "",
            "firstName": "Synthetic",
            "fuelEconomyUnit": "MPGUS",
            "electricEnergyEconomyUnit": "MPGEUS",
            "hosRuleSet": "America8DayBig",
            "isYardMoveEnabled": false,
            "isPersonalConveyanceEnabled": false,
            "isExemptHOSEnabled": false,
            "isAdverseDrivingEnabled": true,
            "maxPCDistancePerDay": 0,
            "authorityName": "Example Fleet Services",
            "authorityAddress": "100 Synthetic Blvd Synthetic City CA 00000",
            "id": "b5CE4",
            "isEULAAccepted": true,
            "isNewsEnabled": true,
            "isLabsEnabled": true,
            "isMetric": false,
            "language": "en",
            "firstDayOfWeek": "Sunday",
            "lastName": "User002",
            "mapViews": [
                {
                    "name": "North America",
                    "viewport": {
                        "x": -181,
                        "y": 69,
                        "width": 145.5,
                        "height": -52
                    },
                    "highlightGroups": []
                }
            ],
            "name": "synthuser001",
            "privateUserGroups": [],
            "reportGroups": [],
            "securityGroups": [
                {
                    "id": "GroupDriveUserSecurityId",
                    "name": "**DriveUserSecurity**"
                }
            ],
            "showClickOnceWarning": true,
            "timeZoneId": "America/Los_Angeles",
            "userAuthenticationType": "BasicAuthentication",
            "zoneDisplayMode": "Default",
            "companyName": "Example Fleet Services",
            "companyAddress": "100 Synthetic Blvd Synthetic City CA 00000",
            "carrierNumber": "1000001",
            "lastAccessDate": "2026-07-15T08:03:32.420Z",
            "isDriver": true,
            "isEmailReportEnabled": true,
            "iAMMetadata": {
                "userId": "00000000-0000-0000-0000-000000000014",
                "connectionName": "None",
                "isIAMVerified": true,
                "isWelcomeEmailSent": true
            },
            "isAutoAdded": false
        },
        {
            "driverGroups": [
                {
                    "id": "GroupCompanyId"
                }
            ],
            "keys": [],
            "viewDriversOwnDataOnly": false,
            "licenseProvince": "OH",
            "licenseNumber": "L0000003",
            "isMaintenanceNotificationEnabled": false,
            "isServiceDisruptionNotificationsEnabled": false,
            "smsNotificationsOptIn": false,
            "whatsAppNotificationsOptIn": false,
            "isAceDisclaimerDisabled": false,
            "showRateThisApp": false,
            "featurePreview": "",
            "acceptedEULA": 20,
            "wifiEULA": 0,
            "activeDashboardReports": [],
            "activeDefaultDashboards": [],
            "jobPriorities": [],
            "bookmarks": [],
            "activeFrom": "2022-12-27T16:14:48.828Z",
            "activeTo": "2050-01-01T00:00:00.000Z",
            "availableDashboardReports": [],
            "cannedResponseOptions": [],
            "changePassword": false,
            "comment": "",
            "companyGroups": [
                {
                    "id": "GroupCompanyId"
                }
            ],
            "mediaFiles": [],
            "dateFormat": "MM/dd/yy HH:mm:ss",
            "phoneNumber": "",
            "displayCurrency": "Cad",
            "countryCode": "ca",
            "phoneNumberExtension": "",
            "defaultGoogleMapStyle": "Roadmap",
            "defaultMapEngine": "",
            "defaultOpenStreetMapStyle": "MapBox",
            "defaultHereMapStyle": "Roadmap",
            "defaultPage": "dashboard",
            "designation": "",
            "employeeNo": "",
            "firstName": "Synthetic",
            "fuelEconomyUnit": "MPGUS",
            "electricEnergyEconomyUnit": "MPGEUS",
            "hosRuleSet": "America8DayBig",
            "isYardMoveEnabled": true,
            "isPersonalConveyanceEnabled": true,
            "isExemptHOSEnabled": false,
            "isAdverseDrivingEnabled": true,
            "maxPCDistancePerDay": 0,
            "authorityName": "Example Fleet Services",
            "authorityAddress": "100 Synthetic Blvd Synthetic City CA 00000",
            "id": "b5CF2",
            "isEULAAccepted": true,
            "isNewsEnabled": true,
            "isLabsEnabled": true,
            "isMetric": false,
            "language": "en",
            "firstDayOfWeek": "Sunday",
            "lastName": "User003",
            "mapViews": [
                {
                    "name": "North America",
                    "viewport": {
                        "x": -181,
                        "y": 69,
                        "width": 145.5,
                        "height": -52
                    },
                    "highlightGroups": []
                }
            ],
            "name": "user003@example.com",
            "privateUserGroups": [],
            "reportGroups": [],
            "securityGroups": [
                {
                    "id": "GroupDriveUserSecurityId",
                    "name": "**DriveUserSecurity**"
                }
            ],
            "showClickOnceWarning": true,
            "timeZoneId": "America/Los_Angeles",
            "userAuthenticationType": "BasicAuthentication",
            "zoneDisplayMode": "Default",
            "companyName": "Example Fleet Services",
            "companyAddress": "100 Synthetic Blvd Synthetic City CA 00000",
            "carrierNumber": "1000001",
            "lastAccessDate": "2026-07-15T08:36:22.308Z",
            "isDriver": true,
            "isEmailReportEnabled": true,
            "iAMMetadata": {
                "userId": "00000000-0000-0000-0000-000000000013",
                "connectionName": "None",
                "isIAMVerified": true,
                "isWelcomeEmailSent": true
            },
            "isAutoAdded": false
        },
        {
            "driverGroups": [
                {
                    "id": "GroupCompanyId"
                }
            ],
            "keys": [],
            "viewDriversOwnDataOnly": false,
            "licenseProvince": "OH",
            "licenseNumber": "L0000004",
            "isMaintenanceNotificationEnabled": false,
            "isServiceDisruptionNotificationsEnabled": false,
            "smsNotificationsOptIn": false,
            "whatsAppNotificationsOptIn": false,
            "isAceDisclaimerDisabled": false,
            "showRateThisApp": false,
            "featurePreview": "",
            "acceptedEULA": 20,
            "wifiEULA": 0,
            "activeDashboardReports": [],
            "activeDefaultDashboards": [],
            "jobPriorities": [],
            "bookmarks": [],
            "activeFrom": "2025-06-16T18:23:57.737Z",
            "activeTo": "2050-01-01T00:00:00.000Z",
            "availableDashboardReports": [],
            "cannedResponseOptions": [],
            "changePassword": false,
            "comment": "",
            "companyGroups": [
                {
                    "id": "GroupCompanyId"
                }
            ],
            "mediaFiles": [],
            "dateFormat": "MM/dd/yy HH:mm:ss",
            "phoneNumber": "+1 5550000001",
            "displayCurrency": "Cad",
            "countryCode": "us",
            "phoneNumberExtension": "",
            "defaultGoogleMapStyle": "Roadmap",
            "defaultMapEngine": "GoogleMaps",
            "defaultOpenStreetMapStyle": "MapBox",
            "defaultHereMapStyle": "Roadmap",
            "defaultPage": "dashboard",
            "designation": "",
            "employeeNo": "",
            "firstName": "Synthetic",
            "fuelEconomyUnit": "MPGUS",
            "electricEnergyEconomyUnit": "MPGEUS",
            "hosRuleSet": "America8Day",
            "isYardMoveEnabled": true,
            "isPersonalConveyanceEnabled": false,
            "isExemptHOSEnabled": false,
            "isAdverseDrivingEnabled": true,
            "maxPCDistancePerDay": 0,
            "authorityName": "Example Fleet Services",
            "authorityAddress": "100 Synthetic Blvd Synthetic City CA 00000",
            "id": "b5D07",
            "isEULAAccepted": true,
            "isNewsEnabled": true,
            "isLabsEnabled": true,
            "isMetric": false,
            "language": "en",
            "firstDayOfWeek": "Sunday",
            "lastName": "User004",
            "mapViews": [
                {
                    "name": "All assets",
                    "viewport": {
                        "x": 0,
                        "y": 0,
                        "width": 0,
                        "height": 0
                    },
                    "highlightGroups": [],
                    "settings": "{\"defaultAllAssets\":true}"
                },
                {
                    "name": "North America",
                    "viewport": {
                        "x": -181,
                        "y": 69,
                        "width": 145.5,
                        "height": -52
                    },
                    "highlightGroups": []
                }
            ],
            "name": "user004@example.com",
            "privateUserGroups": [],
            "reportGroups": [],
            "securityGroups": [
                {
                    "id": "GroupDriveUserSecurityId",
                    "name": "**DriveUserSecurity**"
                }
            ],
            "showClickOnceWarning": true,
            "timeZoneId": "America/Los_Angeles",
            "userAuthenticationType": "BasicAuthentication",
            "zoneDisplayMode": "Default",
            "companyName": "Example Fleet Services",
            "companyAddress": "100 Synthetic Blvd Synthetic City CA 00000",
            "carrierNumber": "1000001",
            "lastAccessDate": "2026-05-26T12:44:56.575Z",
            "isDriver": true,
            "isEmailReportEnabled": true,
            "iAMMetadata": {
                "userId": "00000000-0000-0000-0000-000000000012",
                "connectionName": "None",
                "isIAMVerified": true,
                "isWelcomeEmailSent": true
            },
            "isAutoAdded": false
        },
        {
            "isMaintenanceNotificationEnabled": false,
            "isServiceDisruptionNotificationsEnabled": false,
            "smsNotificationsOptIn": false,
            "whatsAppNotificationsOptIn": false,
            "isAceDisclaimerDisabled": false,
            "showRateThisApp": false,
            "featurePreview": "",
            "acceptedEULA": 20,
            "wifiEULA": 0,
            "activeDashboardReports": [
                "b71E2",
                "b71E5",
                "b71E9",
                "b71F0",
                "b71F4",
                "b71F8"
            ],
            "activeDefaultDashboards": [],
            "jobPriorities": [],
            "bookmarks": [],
            "activeFrom": "2025-10-09T21:01:36.030Z",
            "activeTo": "2050-01-01T00:00:00.000Z",
            "availableDashboardReports": [
                "b71E2",
                "b71E5",
                "b71E9",
                "b71F0",
                "b71F4",
                "b71F8"
            ],
            "cannedResponseOptions": [],
            "changePassword": false,
            "comment": "",
            "companyGroups": [
                {
                    "id": "GroupDriverActivityGroupId"
                }
            ],
            "mediaFiles": [],
            "accessGroupFilter": {
                "id": "aSYN0000000000000000004"
            },
            "dateFormat": "MM/dd/yy HH:mm:ss",
            "phoneNumber": "+1 5550000002",
            "displayCurrency": "Cad",
            "countryCode": "us",
            "phoneNumberExtension": "",
            "defaultGoogleMapStyle": "Roadmap",
            "defaultMapEngine": "GoogleMaps",
            "defaultOpenStreetMapStyle": "MapBox",
            "defaultHereMapStyle": "Roadmap",
            "defaultPage": "map",
            "designation": "Corp",
            "employeeNo": "",
            "firstName": "Synthetic",
            "fuelEconomyUnit": "MPGUS",
            "electricEnergyEconomyUnit": "MPGEUS",
            "hosRuleSet": "None",
            "isYardMoveEnabled": false,
            "isPersonalConveyanceEnabled": false,
            "isExemptHOSEnabled": false,
            "isAdverseDrivingEnabled": true,
            "maxPCDistancePerDay": 0,
            "authorityName": "Example Fleet Services",
            "authorityAddress": "100 Synthetic Blvd Synthetic City CA 00000",
            "id": "b5D13",
            "isEULAAccepted": true,
            "isNewsEnabled": true,
            "isLabsEnabled": true,
            "isMetric": false,
            "language": "en",
            "firstDayOfWeek": "Sunday",
            "lastName": "User005",
            "mapViews": [
                {
                    "name": "Last view",
                    "viewport": {
                        "x": 0,
                        "y": 0,
                        "width": 0,
                        "height": 0
                    },
                    "highlightGroups": [],
                    "settings": "{\"isLastMapView\":true}"
                },
                {
                    "name": "All assets",
                    "viewport": {
                        "x": 0,
                        "y": 0,
                        "width": 0,
                        "height": 0
                    },
                    "highlightGroups": [],
                    "settings": "{\"defaultAllAssets\":true}"
                },
                {
                    "name": "North America",
                    "viewport": {
                        "x": -181,
                        "y": 69,
                        "width": 145.5,
                        "height": -52
                    },
                    "highlightGroups": []
                }
            ],
            "name": "user005@example.com",
            "privateUserGroups": [],
            "reportGroups": [
                {
                    "id": "b6A17"
                }
            ],
            "securityGroups": [
                {
                    "id": "GroupSupervisorSecurityId",
                    "name": "**SupervisorSecurity**"
                }
            ],
            "showClickOnceWarning": true,
            "timeZoneId": "America/Los_Angeles",
            "userAuthenticationType": "BasicAuthentication",
            "zoneDisplayMode": "Default",
            "companyName": "Example Fleet Services",
            "companyAddress": "100 Synthetic Blvd Synthetic City CA 00000",
            "carrierNumber": "",
            "lastAccessDate": "2025-10-09T21:01:36.445Z",
            "isDriver": false,
            "isEmailReportEnabled": true,
            "iAMMetadata": {
                "userId": "00000000-0000-0000-0000-000000000017",
                "connectionName": "None",
                "isIAMVerified": true,
                "isWelcomeEmailSent": true
            },
            "isAutoAdded": false
        },
        {
            "isMaintenanceNotificationEnabled": false,
            "isServiceDisruptionNotificationsEnabled": false,
            "smsNotificationsOptIn": false,
            "whatsAppNotificationsOptIn": false,
            "isAceDisclaimerDisabled": false,
            "showRateThisApp": false,
            "featurePreview": "",
            "acceptedEULA": 20,
            "wifiEULA": 0,
            "activeDashboardReports": [
                "b71E2",
                "b71E5",
                "b71E9",
                "b71F0",
                "b71F4",
                "b71F8"
            ],
            "activeDefaultDashboards": [],
            "jobPriorities": [],
            "bookmarks": [],
            "activeFrom": "2026-03-17T15:50:22.363Z",
            "activeTo": "2050-01-01T00:00:00.000Z",
            "availableDashboardReports": [
                "b71E2",
                "b71E5",
                "b71E9",
                "b71F0",
                "b71F4",
                "b71F8"
            ],
            "cannedResponseOptions": [],
            "changePassword": false,
            "comment": "",
            "companyGroups": [
                {
                    "id": "GroupCompanyId"
                }
            ],
            "mediaFiles": [],
            "dateFormat": "MM/dd/yy HH:mm:ss",
            "phoneNumber": "",
            "displayCurrency": "Cad",
            "countryCode": "ca",
            "phoneNumberExtension": "",
            "defaultGoogleMapStyle": "Roadmap",
            "defaultMapEngine": "GoogleMaps",
            "defaultOpenStreetMapStyle": "MapBox",
            "defaultHereMapStyle": "Roadmap",
            "defaultPage": "map",
            "designation": "",
            "employeeNo": "",
            "firstName": "Synthetic",
            "fuelEconomyUnit": "MPGUS",
            "electricEnergyEconomyUnit": "MPGEUS",
            "hosRuleSet": "None",
            "isYardMoveEnabled": false,
            "isPersonalConveyanceEnabled": false,
            "isExemptHOSEnabled": false,
            "isAdverseDrivingEnabled": true,
            "maxPCDistancePerDay": 0,
            "authorityName": "Example Fleet Services",
            "authorityAddress": "100 Synthetic Blvd Synthetic City CA 00000",
            "id": "b5D2A",
            "isEULAAccepted": true,
            "isNewsEnabled": true,
            "isLabsEnabled": true,
            "isMetric": false,
            "language": "en",
            "firstDayOfWeek": "Sunday",
            "lastName": "User006",
            "mapViews": [
                {
                    "name": "All assets",
                    "viewport": {
                        "x": 0,
                        "y": 0,
                        "width": 0,
                        "height": 0
                    },
                    "highlightGroups": [],
                    "settings": "{\"defaultAllAssets\":true}"
                },
                {
                    "name": "North America",
                    "viewport": {
                        "x": -181,
                        "y": 69,
                        "width": 145.5,
                        "height": -52
                    },
                    "highlightGroups": []
                }
            ],
            "name": "user006@example.com",
            "privateUserGroups": [],
            "reportGroups": [],
            "securityGroups": [
                {
                    "id": "GroupEverythingSecurityId",
                    "name": "**EverythingSecurity**"
                }
            ],
            "showClickOnceWarning": true,
            "timeZoneId": "America/Los_Angeles",
            "userAuthenticationType": "BasicAuthentication",
            "zoneDisplayMode": "Default",
            "companyName": "Example Fleet Services",
            "companyAddress": "100 Synthetic Blvd Synthetic City CA 00000",
            "carrierNumber": "",
            "lastAccessDate": "2026-03-23T16:53:00.681Z",
            "isDriver": false,
            "isEmailReportEnabled": true,
            "iAMMetadata": {
                "userId": "00000000-0000-0000-0000-000000000016",
                "connectionName": "None",
                "isIAMVerified": true,
                "isWelcomeEmailSent": true
            },
            "isAutoAdded": false
        },
        {
            "isMaintenanceNotificationEnabled": false,
            "isServiceDisruptionNotificationsEnabled": false,
            "smsNotificationsOptIn": false,
            "whatsAppNotificationsOptIn": false,
            "isAceDisclaimerDisabled": false,
            "showRateThisApp": false,
            "featurePreview": "",
            "acceptedEULA": 20,
            "wifiEULA": 0,
            "activeDashboardReports": [
                "b71E2",
                "b71E5",
                "b71E9",
                "b71F0",
                "b71F4",
                "b71F8",
                "bFFFFFFFFFFFF9C3D",
                "bFFFFFFFFFFFF9C4A"
            ],
            "activeDefaultDashboards": [],
            "jobPriorities": [
                "ITAndIntegrations",
                "TrackAssets",
                "Utilization",
                "TrackDrivers",
                "HOS",
                "Safety",
                "Installation",
                "Maintenance",
                "Inspections",
                "EV",
                "RoutingAndDispatching",
                "PublicWorks",
                "Driving",
                "CostSavings",
                "FuelAndEmissions",
                "SystemAdmin",
                "UserAdmin"
            ],
            "bookmarks": [],
            "activeFrom": "2026-06-11T14:32:35.200Z",
            "activeTo": "2050-01-01T00:00:00.000Z",
            "availableDashboardReports": [
                "b71E2",
                "b71E5",
                "b71E9",
                "b71F0",
                "b71F4",
                "b71F8",
                "bFFFFFFFFFFFF9C3D",
                "bFFFFFFFFFFFF9C4A"
            ],
            "cannedResponseOptions": [],
            "changePassword": false,
            "comment": "",
            "companyGroups": [
                {
                    "id": "GroupCompanyId"
                }
            ],
            "mediaFiles": [],
            "dateFormat": "MM/dd/yy hh:mm:ss tt",
            "phoneNumber": "+1 5550000003",
            "displayCurrency": "Usd",
            "countryCode": "us",
            "phoneNumberExtension": "",
            "defaultGoogleMapStyle": "Roadmap",
            "defaultMapEngine": "GoogleMaps",
            "defaultOpenStreetMapStyle": "MapBox",
            "defaultHereMapStyle": "Roadmap",
            "defaultPage": "dashboard",
            "designation": "",
            "employeeNo": "",
            "firstName": "Synthetic",
            "fuelEconomyUnit": "MPGUS",
            "electricEnergyEconomyUnit": "MPGEUS",
            "hosRuleSet": "None",
            "isYardMoveEnabled": false,
            "isPersonalConveyanceEnabled": false,
            "isExemptHOSEnabled": false,
            "isAdverseDrivingEnabled": true,
            "maxPCDistancePerDay": 0,
            "authorityName": "Example Fleet Services",
            "authorityAddress": "100 Synthetic Blvd Synthetic City CA 00000",
            "id": "b5D3B",
            "isEULAAccepted": true,
            "isNewsEnabled": true,
            "isLabsEnabled": true,
            "isMetric": false,
            "language": "en",
            "firstDayOfWeek": "Sunday",
            "lastName": "User007",
            "mapViews": [
                {
                    "name": "All assets",
                    "viewport": {
                        "x": 0,
                        "y": 0,
                        "width": 0,
                        "height": 0
                    },
                    "highlightGroups": [],
                    "settings": "{\"defaultAllAssets\":true}"
                },
                {
                    "name": "North America",
                    "viewport": {
                        "x": -181,
                        "y": 69,
                        "width": 145.5,
                        "height": -52
                    },
                    "highlightGroups": []
                }
            ],
            "name": "user007@example.com",
            "privateUserGroups": [],
            "reportGroups": [],
            "securityGroups": [
                {
                    "id": "GroupEverythingSecurityId",
                    "name": "**EverythingSecurity**"
                }
            ],
            "showClickOnceWarning": true,
            "timeZoneId": "America/Chicago",
            "userAuthenticationType": "BasicAuthentication",
            "zoneDisplayMode": "Default",
            "companyName": "Example Fleet Services",
            "companyAddress": "100 Synthetic Blvd Synthetic City CA 00000",
            "carrierNumber": "",
            "lastAccessDate": "2026-07-16T13:29:41.359Z",
            "isDriver": false,
            "isEmailReportEnabled": true,
            "iAMMetadata": {
                "userId": "00000000-0000-0000-0000-000000000015",
                "connectionName": "None",
                "isIAMVerified": true,
                "isWelcomeEmailSent": true
            },
            "isAutoAdded": false
        }
    ],
    "jsonrpc": "2.0"
}"""

USERS_RESPONSE: dict[str, JsonValue] = json.loads(USERS_RESPONSE_JSON)


def _envelope_records(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    result = envelope['result']
    assert isinstance(result, list)
    records: list[JsonObject] = []
    for record in result:
        assert isinstance(record, dict)
        records.append(record)
    return records


USER_RECORDS: list[JsonObject] = _envelope_records(USERS_RESPONSE)

# Captured: GetCountOf User (2026-07-16) -- the completeness guard's
# truth envelope; equalled the sweep's record count exactly.
GET_COUNT_OF_USERS_RESPONSE_JSON: str = r"""
{
    "result": 157,
    "jsonrpc": "2.0"
}"""

GET_COUNT_OF_USERS_RESPONSE: dict[str, JsonValue] = json.loads(
    GET_COUNT_OF_USERS_RESPONSE_JSON
)
