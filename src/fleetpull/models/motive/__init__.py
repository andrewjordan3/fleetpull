"""Motive response models; the face re-exports each endpoint module's models."""

from fleetpull.models.motive.driving_period import DrivingPeriod
from fleetpull.models.motive.group import Group
from fleetpull.models.motive.idle_event import IdleEvent
from fleetpull.models.motive.shared import (
    EldDeviceInfo,
    UserSummary,
    VehicleSummary,
)
from fleetpull.models.motive.user import User
from fleetpull.models.motive.vehicle import (
    AvailabilityDetails,
    AvailabilityStatus,
    Vehicle,
    VehicleStatus,
)
from fleetpull.models.motive.vehicle_location import (
    VehicleLocation,
    VehicleLocationType,
)

__all__: list[str] = [
    'AvailabilityDetails',
    'AvailabilityStatus',
    'DrivingPeriod',
    'EldDeviceInfo',
    'Group',
    'IdleEvent',
    'User',
    'UserSummary',
    'Vehicle',
    'VehicleLocation',
    'VehicleLocationType',
    'VehicleStatus',
    'VehicleSummary',
]
