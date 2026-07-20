# src/fleetpull/models/samsara/__init__.py
"""Samsara response models; the face re-exports each endpoint module's models."""

from fleetpull.models.samsara.driver import (
    Driver,
    DriverActivationStatus,
    DriverCarrierSettings,
    DriverHosSetting,
    DriverStaticAssignedVehicleRef,
    DriverTagRef,
)
from fleetpull.models.samsara.trip import (
    Trip,
    TripAddress,
    TripCoordinates,
)
from fleetpull.models.samsara.vehicle import (
    Vehicle,
    VehicleExternalIds,
    VehicleGatewayRef,
    VehicleStaticAssignedDriverRef,
)

__all__: list[str] = [
    'Driver',
    'DriverActivationStatus',
    'DriverCarrierSettings',
    'DriverHosSetting',
    'DriverStaticAssignedVehicleRef',
    'DriverTagRef',
    'Trip',
    'TripAddress',
    'TripCoordinates',
    'Vehicle',
    'VehicleExternalIds',
    'VehicleGatewayRef',
    'VehicleStaticAssignedDriverRef',
]
