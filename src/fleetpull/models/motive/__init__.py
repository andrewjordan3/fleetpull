"""Motive response models; the face re-exports each endpoint module's models."""

from fleetpull.models.motive.driving_periods import DrivingPeriod
from fleetpull.models.motive.idle_events import IdleEvent
from fleetpull.models.motive.shared import (
    DriverSummary,
    EldDeviceInfo,
    VehicleSummary,
)
from fleetpull.models.motive.vehicle_locations import (
    VehicleLocation,
    VehicleLocationType,
)
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
    'DrivingPeriod',
    'EldDeviceInfo',
    'IdleEvent',
    'Vehicle',
    'VehicleLocation',
    'VehicleLocationType',
    'VehicleStatus',
    'VehicleSummary',
]
