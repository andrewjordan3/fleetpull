# src/fleetpull/models/samsara/__init__.py
"""Samsara response models; the face re-exports each endpoint module's models."""

from fleetpull.models.samsara.vehicle import (
    Vehicle,
    VehicleExternalIds,
    VehicleGatewayRef,
    VehicleStaticAssignedDriverRef,
)

__all__: list[str] = [
    'Vehicle',
    'VehicleExternalIds',
    'VehicleGatewayRef',
    'VehicleStaticAssignedDriverRef',
]
