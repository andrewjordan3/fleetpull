# src/fleetpull/endpoints/motive/__init__.py
"""The Motive endpoints face: binding factories for Motive endpoints."""

from fleetpull.endpoints.motive.vehicle_locations import (
    build_vehicle_locations_endpoint,
)
from fleetpull.endpoints.motive.vehicles import build_vehicles_endpoint

__all__: list[str] = [
    'build_vehicle_locations_endpoint',
    'build_vehicles_endpoint',
]
