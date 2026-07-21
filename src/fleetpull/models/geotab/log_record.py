# src/fleetpull/models/geotab/log_record.py
"""GeoTab LogRecord response model (``GetFeed`` on ``typeName: LogRecord``).

Written from the 2026-07-21 live probe session, never from docs. A
LogRecord is one GPS reading from a device's telemetry stream — the
ACTIVE feed archetype: records are emitted once, never re-emitted, and
carry NO per-record ``version`` (unlike every calculated feed and unlike
the otherwise-parallel ``StatusData``), so append-only storage is
trivially complete and the consumer reconciles by ``id`` alone
(DESIGN §4).

Requiredness posture: the census is a large uniform whole-page total —
2,000/2,000 records carried every key — so every field is required with
no nullable arm (none was observed). The census is a TENANT-SCOPED
observation (DESIGN §8): it proves this tenant's shapes at capture time,
never other tenants'. Volume on the probed tenant exceeds 50,000
records/day (a 50,000-record page did not cover one day), which is why
the leaf declares the 50,000 protocol-maximum ``resultsLimit``.

``dateTime`` (the event time) arrives as an RFC3339 ``Z`` string and is
recovered tz-aware by validation, the GeoTab sibling idiom; ``speed`` is
a bare int on all 2,000 census records and is mirrored verbatim (the
odometer_readings bare-int precedent — an integral wire value is not
widened speculatively).
"""

from datetime import datetime

from pydantic import ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel

__all__: list[str] = ['LogRecord', 'LogRecordDeviceRef']


class LogRecordDeviceRef(ResponseModel):
    """The reading's device reference: the id alone, on every record."""

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class LogRecord(ResponseModel):
    """One GeoTab GPS reading from the LogRecord feed.

    A pure mirror of the 2,000/2,000 whole-page census: six keys, all
    present and non-null on every record, so all six are required.

    Attributes:
        date_time: The reading's UTC instant — the endpoint's event time.
        device: The emitting vehicle unit's reference.
        id: GeoTab's record id (ids and feed versions share one counter
            space — DESIGN §8).
        latitude: GPS latitude in decimal degrees.
        longitude: GPS longitude in decimal degrees.
        speed: Speed in km/h (the provider speed-unit settlement: the Trip delta-arithmetic check, DESIGN section 8, 2026-07-13 -- same provider telemetry family, not a docs assumption) — a bare int on the wire, mirrored verbatim.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    date_time: datetime
    device: LogRecordDeviceRef
    id: str
    latitude: float
    longitude: float
    speed: int
