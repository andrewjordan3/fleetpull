# src/fleetpull/models/motive/driving_periods.py
"""The Motive driving-period response model (captured 2026-07-15).

One record per contiguous driving span from ``GET /v1/driving_periods``.
Completed records reproduce ``duration = end_time - start_time`` exactly
(float seconds); in-progress records (``status: "in_progress"``) null
every end-side field — ``end_time``, ``end_kilometers``, ``distance``,
the destination and its coordinates — while ``duration`` keeps counting
as a fractional elapsed value. ``distance`` is a provider-formatted
string (``"42.2 mi"``) mirrored verbatim — the merely-ugly side of the
coercion boundary; real distance arithmetic uses the kilometer odometer
fields. ``start_time`` was never observed null and is the endpoint's
retrieval anchor (start-anchored UTC window matching, DESIGN §8).

Excluded fields, per capture discipline: ``source`` and the four
``*_hvb_*`` battery fields were never observed non-null (a diesel
fleet's capture), so no honest dtype exists; they join the model when a
capture pins their types.
"""

from datetime import datetime

from pydantic import Field

from fleetpull.model_contract import ResponseModel
from fleetpull.models.motive.shared import DriverSummary, VehicleSummary

__all__: list[str] = ['DrivingPeriod']


class DrivingPeriod(ResponseModel):
    """One contiguous driving span for one vehicle.

    Attributes:
        period_id: Motive's internal period identifier (wire key ``id``).
        start_time: UTC start of the span; the retrieval anchor.
        end_time: UTC end of the span; null while the span is in progress.
        status: Free-form lifecycle string (``"complete"`` /
            ``"in_progress"`` observed), mirrored, never interpreted.
        period_type: Free-form span-type string (wire key ``type``;
            ``"driving"`` observed), mirrored, never interpreted.
        annotation_status: Provider annotation state; null when absent.
        notes: Free-form annotation text; null when absent.
        duration: Span length in float seconds; on an in-progress record,
            the elapsed value so far.
        start_kilometers: Odometer at span start, kilometers.
        end_kilometers: Odometer at span end; null while in progress.
        driver: Attributed driver; null when the span is unattributed.
        vehicle: The vehicle the span belongs to.
        origin: Provider-formatted start address, mirrored verbatim
            (empty strings normalize to null at the DataFrame boundary);
            null when absent.
        origin_lat: Start latitude; null when the provider has none.
        origin_lon: Start longitude; null when the provider has none.
        destination_lat: End latitude; null while in progress.
        destination_lon: End longitude; null while in progress.
        destination: Provider-formatted end address, mirrored verbatim —
            the captured in-progress record carries ``""`` here (nulled
            at the DataFrame boundary); null when absent.
        distance: Provider-formatted distance string (``"42.2 mi"``),
            mirrored verbatim; null while in progress.
    """

    period_id: int = Field(alias='id')
    start_time: datetime
    end_time: datetime | None = None
    status: str
    period_type: str = Field(alias='type')
    annotation_status: int | None = None
    notes: str | None = None
    duration: float
    start_kilometers: float
    end_kilometers: float | None = None
    driver: DriverSummary | None = None
    vehicle: VehicleSummary
    origin: str | None = None
    origin_lat: float | None = None
    origin_lon: float | None = None
    destination_lat: float | None = None
    destination_lon: float | None = None
    destination: str | None = None
    distance: str | None = None
