# src/fleetpull/models/geotab/__init__.py
"""GeoTab response models; the face re-exports each endpoint module's models."""

from fleetpull.models.geotab.device import CustomFeatures, Device, DeviceFlags
from fleetpull.models.geotab.exception_event import (
    ExceptionEvent,
    ExceptionEventDeviceRef,
    ExceptionEventDiagnosticRef,
    ExceptionEventDriverRef,
    ExceptionEventRuleRef,
)
from fleetpull.models.geotab.shared import (
    GeotabTimeSpan,
    bare_id_to_reference,
    parse_timespan,
)
from fleetpull.models.geotab.trip import (
    Trip,
    TripDeviceRef,
    TripDriverRef,
    TripStopPoint,
)
from fleetpull.models.geotab.user import User, UserAccessGroupFilterRef

__all__: list[str] = [
    'CustomFeatures',
    'Device',
    'DeviceFlags',
    'ExceptionEvent',
    'ExceptionEventDeviceRef',
    'ExceptionEventDiagnosticRef',
    'ExceptionEventDriverRef',
    'ExceptionEventRuleRef',
    'GeotabTimeSpan',
    'Trip',
    'TripDeviceRef',
    'TripDriverRef',
    'TripStopPoint',
    'User',
    'UserAccessGroupFilterRef',
    'bare_id_to_reference',
    'parse_timespan',
]
