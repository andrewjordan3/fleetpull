# src/fleetpull/models/geotab/__init__.py
"""GeoTab response models; the face re-exports each endpoint module's models."""

from fleetpull.models.geotab.device import CustomFeatures, Device, DeviceFlags
from fleetpull.models.geotab.driver_change import (
    DriverChange,
    DriverChangeDeviceRef,
    DriverChangeDriverRef,
)
from fleetpull.models.geotab.duty_status_log import (
    DutyStatusLog,
    DutyStatusLogDeviceRef,
    DutyStatusLogDriverRef,
)
from fleetpull.models.geotab.dvir_log import (
    DvirLog,
    DvirLogDefectList,
    DvirLogDeviceRef,
    DvirLogDriverRef,
    DvirLogTrailerRef,
)
from fleetpull.models.geotab.exception_event import (
    ExceptionEvent,
    ExceptionEventDeviceRef,
    ExceptionEventDiagnosticRef,
    ExceptionEventDriverRef,
    ExceptionEventRuleRef,
)
from fleetpull.models.geotab.fault_data import (
    FaultData,
    FaultDataControllerRef,
    FaultDataDeviceRef,
    FaultDataDiagnosticRef,
    FaultDataFailureModeRef,
    FaultDataFaultStates,
)
from fleetpull.models.geotab.fill_up import (
    FillUp,
    FillUpDeviceRef,
    FillUpDriverRef,
    FillUpLocation,
    FillUpTankCapacity,
    FillUpTankLevelExtrema,
    FillUpTankLevelPoint,
)
from fleetpull.models.geotab.fuel_and_energy_used import (
    FuelAndEnergyUsed,
    FuelAndEnergyUsedDeviceRef,
)
from fleetpull.models.geotab.fuel_tax_detail import (
    FuelTaxDetail,
    FuelTaxDetailDeviceRef,
    FuelTaxDetailDriverRef,
)
from fleetpull.models.geotab.log_record import LogRecord, LogRecordDeviceRef
from fleetpull.models.geotab.shared import (
    GeotabAddressedLocation,
    GeotabCoordinate,
    GeotabTimeSpan,
    bare_id_to_reference,
    parse_timespan,
)
from fleetpull.models.geotab.status_data import (
    StatusData,
    StatusDataDeviceRef,
    StatusDataDiagnosticRef,
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
    'DriverChange',
    'DriverChangeDeviceRef',
    'DriverChangeDriverRef',
    'DutyStatusLog',
    'DutyStatusLogDeviceRef',
    'DutyStatusLogDriverRef',
    'DvirLog',
    'DvirLogDefectList',
    'DvirLogDeviceRef',
    'DvirLogDriverRef',
    'DvirLogTrailerRef',
    'ExceptionEvent',
    'ExceptionEventDeviceRef',
    'ExceptionEventDiagnosticRef',
    'ExceptionEventDriverRef',
    'ExceptionEventRuleRef',
    'FaultData',
    'FaultDataControllerRef',
    'FaultDataDeviceRef',
    'FaultDataDiagnosticRef',
    'FaultDataFailureModeRef',
    'FaultDataFaultStates',
    'FillUp',
    'FillUpDeviceRef',
    'FillUpDriverRef',
    'FillUpLocation',
    'FillUpTankCapacity',
    'FillUpTankLevelExtrema',
    'FillUpTankLevelPoint',
    'FuelAndEnergyUsed',
    'FuelAndEnergyUsedDeviceRef',
    'FuelTaxDetail',
    'FuelTaxDetailDeviceRef',
    'FuelTaxDetailDriverRef',
    'GeotabAddressedLocation',
    'GeotabCoordinate',
    'GeotabTimeSpan',
    'LogRecord',
    'LogRecordDeviceRef',
    'StatusData',
    'StatusDataDeviceRef',
    'StatusDataDiagnosticRef',
    'Trip',
    'TripDeviceRef',
    'TripDriverRef',
    'TripStopPoint',
    'User',
    'UserAccessGroupFilterRef',
    'bare_id_to_reference',
    'parse_timespan',
]
