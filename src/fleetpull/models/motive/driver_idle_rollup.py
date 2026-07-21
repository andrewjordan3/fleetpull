# src/fleetpull/models/motive/driver_idle_rollup.py
"""Motive DriverIdleRollup response model
(``GET /v2/driver_utilization``, post-decoder window-stamped grain).

Written from captured live responses (2026-07-21 probe session: 100
records sampled, structurally uniform), never from docs. NOTE THE
NAMING: the wire's OWN envelope vocabulary is
``driver_idle_rollups``/``driver_idle_rollup`` -- different from its
``/v2/driver_utilization`` path -- and the model mirrors the wire's
vocabulary (the endpoint records the legacy-name mapping in
``ENDPOINTS.md``).

The model mirrors the record ``MotiveWindowReportPageDecoder`` emits:
``window_start`` / ``window_end`` are DECODER-SYNTHESIZED
(``windowStartDate``/``windowEndDate``) -- rollup rows carry NO date or
time identity of any kind, so the decoder stamps each row with the
window the SENT spec asked for, copied verbatim from its
``start_date``/``end_date`` date labels, and ``MotiveWindowStamp``
lifts each label to its UTC-midnight instant (``shared.py``);
everything else is wire-verbatim.

**THE COMPANY-LOCAL CAVEAT (the documented obligation on this
mirror).** The date labels are interpreted in COMPANY-LOCAL days -- the
account's ``/v1/companies`` capture carries a company-local zone at a
UTC-5 offset (DESIGN section 8) -- so each row is the provider's rollup
over its company-local day(s), mirrored verbatim. The window stamps
carry the day LABELS, not UTC day boundaries; nothing here converts
anything.

**THE ROLLUP GRAIN IS THE REQUEST WINDOW -- proven on this surface;
do not sum day rows (precedent-based).** A quiet single day returned 13
rows and a six-day window 653 -- one rollup row per driver with
activity in the window, per window (contrast the vehicle arm's
whole-fleet-every-window population). The grain forces the binding's
``fixed_unit_days=1``. Additivity was NOT probed here; on the provider
family's only probed rollup surfaces (the Samsara fuel-energy pair) day
rollups were NON-ADDITIVE into wider windows (89/267 mismatched), so
the same posture applies as precedent: day rows MUST NOT be summed to
reproduce a wider window's rollup.

``driver`` is the shared 8-key ``UserSummary`` (its fourth carrying
surface) and NULLABLE: the census found it populated on 99 of 100
sampled rows and NULL on one -- an UNATTRIBUTED rollup bucket the
provider emits alongside the per-driver rows, mirrored as a null ref,
never dropped. The metric core is REQUIRED (every key on every sampled
record; the rollup-surface reasoning -- metrics are computed per
window, with no absence mechanism to be conservative about), and the
window stamps are required STRUCTURALLY. ``idle_time`` and
``driving_time`` are bare INTS on this arm -- floats on the vehicle
arm -- and each arm mirrors its own wire.
"""

from pydantic import Field

from fleetpull.model_contract import ResponseModel
from fleetpull.models.motive.shared import MotiveWindowStamp, UserSummary

__all__: list[str] = ['DriverIdleRollup']


class DriverIdleRollup(ResponseModel):
    """One driver's idle rollup over exactly one request window.

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
        driver: The rollup's driver entity (the shared ``UserSummary``);
            NULL on the unattributed rollup bucket row (module
            docstring), populated with the full 8-key shape otherwise.
        utilization: The provider's utilization figure for the window,
            mirrored uninterpreted.
        driving_time: Time spent driving in the window, a bare int
            (contrast the vehicle arm's float durations -- each arm
            mirrors its own wire).
        idle_time: Time spent idling in the window, a bare int.
        driving_fuel: Fuel used while driving in the window, provider
            units mirrored verbatim.
        idle_fuel: Fuel used while idling in the window.
    """

    # Decoder-synthesized window identity (the sent spec's own labels).
    window_start: MotiveWindowStamp = Field(alias='windowStartDate')
    window_end: MotiveWindowStamp = Field(alias='windowEndDate')

    # The rollup's entity; null on the unattributed bucket row.
    driver: UserSummary | None = None

    # The wire-verbatim metric core (required -- rollup surfaces have no
    # absence arm; int durations on THIS arm, provider units verbatim).
    utilization: float
    driving_time: int
    idle_time: int
    driving_fuel: float
    idle_fuel: float
