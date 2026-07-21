# src/fleetpull/models/samsara/__init__.py
"""Samsara response models; the face re-exports each endpoint module's models."""

from fleetpull.models.samsara.address import (
    Address,
    AddressGeofence,
    AddressGeofenceCircle,
    AddressGeofenceSettings,
)
from fleetpull.models.samsara.asset_location import (
    AssetLocation,
    AssetLocationAssetRef,
    AssetLocationFix,
)
from fleetpull.models.samsara.driver import (
    Driver,
    DriverActivationStatus,
    DriverCarrierSettings,
    DriverHosSetting,
    DriverStaticAssignedVehicleRef,
    DriverTagRef,
)
from fleetpull.models.samsara.driver_fuel_energy_report import (
    DriverFuelEnergyCost,
    DriverFuelEnergyDriverRef,
    DriverFuelEnergyReport,
)
from fleetpull.models.samsara.driver_vehicle_assignment import (
    AssignmentDriverRef,
    AssignmentVehicleExternalIds,
    AssignmentVehicleRef,
    DriverVehicleAssignment,
)
from fleetpull.models.samsara.engine_state import EngineState
from fleetpull.models.samsara.gps_reading import (
    GpsReading,
    GpsReadingAddressRef,
    GpsReadingReverseGeo,
)
from fleetpull.models.samsara.idling_event import (
    AssetRef,
    FuelCost,
    IdlingAddress,
    IdlingEvent,
    OperatorRef,
)
from fleetpull.models.samsara.odometer_reading import OdometerReading
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
from fleetpull.models.samsara.vehicle_fuel_energy_report import (
    VehicleFuelEnergyCost,
    VehicleFuelEnergyExternalIds,
    VehicleFuelEnergyReport,
    VehicleFuelEnergyVehicleRef,
)

__all__: list[str] = [
    'Address',
    'AddressGeofence',
    'AddressGeofenceCircle',
    'AddressGeofenceSettings',
    'AssetLocation',
    'AssetLocationAssetRef',
    'AssetLocationFix',
    'AssetRef',
    'AssignmentDriverRef',
    'AssignmentVehicleExternalIds',
    'AssignmentVehicleRef',
    'Driver',
    'DriverActivationStatus',
    'DriverCarrierSettings',
    'DriverFuelEnergyCost',
    'DriverFuelEnergyDriverRef',
    'DriverFuelEnergyReport',
    'DriverHosSetting',
    'DriverStaticAssignedVehicleRef',
    'DriverTagRef',
    'DriverVehicleAssignment',
    'EngineState',
    'FuelCost',
    'GpsReading',
    'GpsReadingAddressRef',
    'GpsReadingReverseGeo',
    'IdlingAddress',
    'IdlingEvent',
    'OdometerReading',
    'OperatorRef',
    'Trip',
    'TripAddress',
    'TripCoordinates',
    'Vehicle',
    'VehicleExternalIds',
    'VehicleFuelEnergyCost',
    'VehicleFuelEnergyExternalIds',
    'VehicleFuelEnergyReport',
    'VehicleFuelEnergyVehicleRef',
    'VehicleGatewayRef',
    'VehicleStaticAssignedDriverRef',
]
