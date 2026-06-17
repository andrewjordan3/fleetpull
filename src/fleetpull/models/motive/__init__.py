"""Motive response models; the face re-exports each endpoint module's models."""

from fleetpull.models.motive.shared import DriverSummary, EldDeviceInfo
from fleetpull.models.motive.vehicles import (
    AvailabilityDetails,
    AvailabilityStatus,
    Vehicle,
    VehicleStatus,
)

__all__: list[str] = [
    'AvailabilityDetails',
    'AvailabilityStatus',
    'DriverSummary',
    'EldDeviceInfo',
    'Vehicle',
    'VehicleStatus',
]
