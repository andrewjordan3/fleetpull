# src/fleetpull/models/motive/vehicle_utilization.py
"""Motive VehicleUtilization response model
(``GET /v2/vehicle_utilization``, post-decoder window-stamped grain).

Written from captured live responses (2026-07-21 probe session: 120
records sampled across the 1,466-vehicle listing, structurally uniform
-- every key on every sampled record), never from docs. The model
mirrors the record ``MotiveWindowReportPageDecoder`` emits, and its two
field families have different provenance:

- ``window_start`` / ``window_end`` are DECODER-SYNTHESIZED
  (``windowStartDate``/``windowEndDate``): rollup rows carry NO date or
  time identity of any kind, so the decoder stamps each row with the
  window the SENT spec asked for, copied verbatim from its
  ``start_date``/``end_date`` date labels (the fuel-energy pair's
  request-sourced stamp, on Motive wire). ``MotiveWindowStamp`` lifts
  each label to its UTC-midnight instant -- a representation for
  partition routing, never a timezone conversion (``shared.py``).
- Everything else is WIRE-VERBATIM: the metric core, the ``vehicle``
  ref (the shared ``VehicleSummary``, its third carrying surface), and
  the ``message`` status string, snake_case keys mirrored directly.

**THE COMPANY-LOCAL CAVEAT (the documented obligation on this
mirror).** The ``start_date``/``end_date`` labels are interpreted in
COMPANY-LOCAL days -- the account's ``/v1/companies`` capture carries a
company-local zone at a UTC-5 offset (DESIGN section 8) -- so each row
is the provider's rollup over its company-local day(s), mirrored
verbatim. The window stamps carry the day LABELS, not UTC day
boundaries; nothing here converts anything.

**THE ROLLUP GRAIN IS THE REQUEST WINDOW -- proven on this surface;
do not sum day rows (precedent-based).** A 1-day and a 6-day request
each returned exactly one rollup row per vehicle over the SAME
1,466-vehicle population -- the grain is the window, which is why the
binding declares ``fixed_unit_days=1``. Additivity was NOT probed here;
on the provider family's only probed rollup surfaces (the Samsara
fuel-energy pair) day rollups were NON-ADDITIVE into wider windows
(89/267 mismatched), so the same posture applies as precedent: day rows
MUST NOT be summed to reproduce a wider window's rollup.

The population is the WHOLE vehicle fleet regardless of window (the
1-day and 6-day totals were both 1,466): inactive vehicles ride with
zeroed metrics and a populated ``message`` status string -- there is no
absence arm on this surface, so the metric core is REQUIRED (the
fuel-energy whole-walk reasoning: a rollup surface computes its metrics
per window and has no absence mechanism to be conservative about). The
window stamps and the ``vehicle`` ref are required STRUCTURALLY -- a
rollup row without its window or its entity is meaningless.

``last_located_at`` is the one nullable key (str-or-None in census) and
mirrors VERBATIM as a string: its value format and zone semantics are
unprobed, and Motive's rollup timestamps are documented company-local,
so parsing it would presume what no capture has shown. ``message`` is
free text -- a plain ``str``, no vocabulary claim.
"""

from pydantic import Field

from fleetpull.model_contract import ResponseModel
from fleetpull.models.motive.shared import MotiveWindowStamp, VehicleSummary

__all__: list[str] = ['VehicleUtilization']


class VehicleUtilization(ResponseModel):
    """One vehicle's utilization rollup over exactly one request window.

    A pure mirror of the window-stamped post-decoder record (module
    docstring: the window stamps are decoder-synthesized from the sent
    spec's date labels; everything else is wire-verbatim). Field
    semantics and units are Motive's; no value is derived or
    interpreted here. Day rows MUST NOT be summed to reproduce a wider
    window's rollup (module docstring -- the provider-family
    precedent).

    Attributes:
        window_start: The request window's start-date label
            (decoder-synthesized ``windowStartDate``, verbatim from the
            sent ``start_date``) at UTC midnight -- the event-time
            column: the row's time identity is the window that produced
            it. A COMPANY-LOCAL day label (module docstring).
        window_end: The request window's end-date label
            (decoder-synthesized ``windowEndDate``, verbatim from the
            sent ``end_date``) at UTC midnight. INCLUSIVE: at the fixed
            1-day unit it equals ``window_start``.
        vehicle: The rollup's vehicle entity (the shared
            ``VehicleSummary``; ``vin`` null on some rows of this
            surface, every other key populated in census).
        driving_fuel: Fuel used while driving in the window, provider
            units mirrored verbatim.
        driving_time: Time spent driving in the window, a float
            (contrast the driver arm's bare-int durations -- each arm
            mirrors its own wire).
        idle_fuel: Fuel used while idling in the window.
        idle_time: Time spent idling in the window, a float.
        total_distance: Distance covered in the window.
        total_fuel: Total fuel used in the window.
        utilization_percentage: The provider's utilization figure for
            the window, mirrored uninterpreted.
        last_located_at: The vehicle's last-location timestamp, mirrored
            VERBATIM as a string (format and zone semantics unprobed;
            the company-local documentation obligation -- module
            docstring); null when the provider has none.
        message: Free-text status string -- populated on inactive
            zero-metric rows (e.g. a no-data notice), empty otherwise;
            a plain ``str``, no vocabulary claim.
    """

    # Decoder-synthesized window identity (the sent spec's own labels).
    window_start: MotiveWindowStamp = Field(alias='windowStartDate')
    window_end: MotiveWindowStamp = Field(alias='windowEndDate')

    # The rollup's entity.
    vehicle: VehicleSummary

    # The wire-verbatim metric core (required -- no absence arm exists
    # on this surface; provider units mirrored verbatim).
    driving_fuel: float
    driving_time: float
    idle_fuel: float
    idle_time: float
    total_distance: float
    total_fuel: float
    utilization_percentage: float

    # The wire-verbatim status pair.
    last_located_at: str | None = None
    message: str
