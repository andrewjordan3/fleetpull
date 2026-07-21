# src/fleetpull/models/geotab/fuel_and_energy_used.py
"""GeoTab FuelAndEnergyUsed response model (``GetFeed`` on that ``typeName``).

Written from the 2026-07-21 live probe session, never from docs. A
FuelAndEnergyUsed record is one provider-calculated per-trip fuel/energy
usage total — a CALCULATED feed: past records re-emit under a higher
``version`` on reprocessing, stored as emitted and reconciled by
``(id, max version)`` (DESIGN §4).

``FuelUsed`` is NOT ported: on the probed tenant it was observed
IDENTICAL to this surface (same ids, same values, week-wide), and the
provider documents this type as its successor — porting both would ship
one dataset twice (DESIGN §8).

THE ESTIMATES-ONLY-TENANT CAVEAT (DESIGN §8): the probed tenant has NO
fuel-transaction (fuel-card) integration, so every fuel value on this
surface is provider-derived from telemetry — estimates, not
transactions. The census cannot speak for integrated tenants.

Requiredness posture: the census is a large uniform whole-page total —
2,000/2,000 records carried every key — so every field is required with
no nullable arm (none was observed). The census is a TENANT-SCOPED
observation. ``confidence`` observed ``'None'`` on 1,994/2,000 and
``'FuelUsedInconsistent'`` on 6 — census-open, a plain str. Mixed
int-or-float wire numerics (``totalFuelUsed``,
``totalIdlingFuelUsedL``) are modeled ``float``; ``dateTime`` (the
event time) is recovered tz-aware by validation, the GeoTab sibling
idiom.
"""

from datetime import datetime

from pydantic import ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel

__all__: list[str] = ['FuelAndEnergyUsed', 'FuelAndEnergyUsedDeviceRef']


class FuelAndEnergyUsedDeviceRef(ResponseModel):
    """The usage record's device reference: the id alone, on every record."""

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class FuelAndEnergyUsed(ResponseModel):
    """One GeoTab per-trip fuel/energy usage total.

    A pure mirror of the 2,000/2,000 whole-page census: seven keys, all
    present and non-null on every record, so all seven are required.
    The estimates-only-tenant caveat and the ``FuelUsed`` non-port are
    the module docstring's.

    Attributes:
        confidence: The provider's confidence token — ``'None'`` on
            nearly every census record, ``'FuelUsedInconsistent'`` on
            the rest (census-open plain str).
        date_time: The usage total's UTC instant — the endpoint's event
            time.
        device: The vehicle unit's reference.
        id: GeoTab's record id.
        total_fuel_used: Fuel used over the covered trip, liters (the wire key totalIdlingFuelUsedL's own L suffix -- unit evidence on the wire, not a docs assumption) (mixed
            int-or-float on the wire, modeled float).
        total_idling_fuel_used_l: Idling fuel used over the covered
            trip, liters (mixed int-or-float, modeled float).
        version: The record's version token — the calculated-feed
            reconcile key beside ``id``.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    confidence: str
    date_time: datetime
    device: FuelAndEnergyUsedDeviceRef
    id: str
    total_fuel_used: float
    total_idling_fuel_used_l: float
    version: str
