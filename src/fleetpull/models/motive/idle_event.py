# src/fleetpull/models/motive/idle_event.py
"""The Motive idle-event response model (captured 2026-07-15).

One record per engine-idle interval from ``GET /v1/idle_events``. Both
timestamps were always present in capture — the endpoint has no
in-progress shape analogue. ``veh_fuel_start`` / ``veh_fuel_end`` are the
ELD's cumulative fuel counters, mirrored as-is. The ``rg_*`` fields are
the provider's reverse-geocode metadata; when ``rg_match`` is false,
``location`` carries a distance-direction prefix (``"2.6 mi NW of …"``)
instead of a bare place name — both formats mirror verbatim.

The endpoint's date window is interpreted on company-local day boundaries
and matched by overlap (DESIGN §8, captured 2026-07-15) — the reason its
endpoint leaf pads the wire window; nothing about that reaches this
model, whose ``start_time`` remains the routing anchor.
"""

from datetime import datetime

from pydantic import Field

from fleetpull.model_contract import ResponseModel
from fleetpull.models.motive.shared import (
    EldDeviceInfo,
    UserSummary,
    VehicleSummary,
)

__all__: list[str] = ['IdleEvent']


class IdleEvent(ResponseModel):
    """One engine-idle interval for one vehicle.

    Attributes:
        event_id: Motive's internal event identifier (wire key ``id``).
        start_time: UTC start of the idle interval; the routing anchor.
        end_time: UTC end of the idle interval.
        veh_fuel_start: ELD cumulative fuel counter at interval start;
            null when the vehicle reports no fuel counters
            (live-observed 2026-07-16).
        veh_fuel_end: ELD cumulative fuel counter at interval end; null
            when the vehicle reports no fuel counters.
        lat: Event latitude.
        lon: Event longitude.
        city: Reverse-geocoded place name.
        state: Reverse-geocoded state / province code.
        rg_brg: Reverse-geocode bearing from the matched place, degrees.
        rg_km: Reverse-geocode distance from the matched place,
            kilometers.
        rg_match: Whether the reverse geocoder matched a place directly.
        end_type: Free-form interval-end reason (``"engine_stop"`` /
            ``"vehicle_moving"`` observed), mirrored, never interpreted.
        driver: Attributed driver; null when the idle is unattributed.
        vehicle: The vehicle the interval belongs to.
        eld_device: The ELD hardware that reported the interval.
        location: Provider-formatted place string; carries a
            distance-direction prefix when ``rg_match`` is false.
    """

    event_id: int = Field(alias='id')
    start_time: datetime
    end_time: datetime
    veh_fuel_start: float | None = None
    veh_fuel_end: float | None = None
    lat: float
    lon: float
    city: str
    state: str
    rg_brg: float
    rg_km: float
    rg_match: bool
    end_type: str
    driver: UserSummary | None = None
    vehicle: VehicleSummary
    eld_device: EldDeviceInfo
    location: str
