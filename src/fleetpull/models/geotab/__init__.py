# src/fleetpull/models/geotab/__init__.py
"""GeoTab response models; the face re-exports each endpoint module's models."""

from fleetpull.models.geotab.device import CustomFeatures, Device, DeviceFlags
from fleetpull.models.geotab.log_record import LogRecord, LogRecordDeviceRef
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

__all__: list[str] = [
    'CustomFeatures',
    'Device',
    'DeviceFlags',
    'GeotabTimeSpan',
    'LogRecord',
    'LogRecordDeviceRef',
    'Trip',
    'TripDeviceRef',
    'TripDriverRef',
    'TripStopPoint',
    'bare_id_to_reference',
    'parse_timespan',
]
