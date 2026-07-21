# src/fleetpull/models/geotab/fault_data.py
"""GeoTab FaultData response model (``GetFeed`` on ``typeName: FaultData``).

Written from the 2026-07-21 feed wave two census (30-day seeded pulls at
the probed tenant), never from docs. A FaultData record is one
engine-fault observation — an ACTIVE feed with NO per-record
``version`` (the LogRecord asymmetry: append-only storage is trivially
complete and the consumer reconciles by ``id`` alone, DESIGN §4).

Requiredness posture (the wave-two conservative stance, DESIGN §8): the
census is a TENANT-SCOPED observation, so structural requiredness is
limited to the record identity — ``id``, ``dateTime`` (the event
time), and the primary entity ref (``device``, the faulting unit) —
and every other field is optional EVEN where the census was total
(2,000/2,000): a tenant census cannot promise another tenant's
presence. The observed arms:

- ``failureMode`` is MIXED object-or-string on this census (the one
  proven mixed ref here); EVERY reference field — ``controller``,
  ``device``, ``diagnostic``, ``failureMode`` — rides the shared
  ``bare_id_to_reference`` lift regardless, because the census-scope
  lesson (DESIGN §8, StatusData's ``controller``) is that a tenant
  census cannot prove the string arm absent and the lift is structural
  and sentinel-agnostic.
- The RARE QUARTET — ``diagnosticSeverity`` (str), ``riskOfBreakdown``
  (float), ``severity`` (str), ``sourceAddress`` (int) — appeared on
  2/2,000 records each: optional scalars.
- ``faultStates`` is a WIRE-PLURAL NAME carrying a SINGULAR object
  shape (``{effectiveStatus}``) — mirrored as one nested model, the
  plural-name/singular-shape wire fact recorded on it.

``dateTime`` is recovered tz-aware by validation, the GeoTab sibling
idiom. ``faultState`` and ``faultStates.effectiveStatus`` are
census-open vocabularies — plain strs, never enums.
"""

from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel
from fleetpull.models.geotab.shared import bare_id_to_reference

__all__: list[str] = [
    'FaultData',
    'FaultDataControllerRef',
    'FaultDataDeviceRef',
    'FaultDataDiagnosticRef',
    'FaultDataFailureModeRef',
    'FaultDataFaultStates',
]


class FaultDataControllerRef(ResponseModel):
    """The source controller reference.

    Census-observed as an ``{id}`` object on every carrier; the shared
    coercion lifts a bare string defensively (the StatusData
    census-scope lesson — an unobserved sentinel arm still lands as
    ``controller__id``).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class FaultDataDeviceRef(ResponseModel):
    """The faulting vehicle unit's reference: the id alone, on every record."""

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class FaultDataDiagnosticRef(ResponseModel):
    """The fault's diagnostic-definition reference: the id alone."""

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class FaultDataFailureModeRef(ResponseModel):
    """The failure-mode reference.

    The one PROVEN mixed ref on this census: an ``{id}`` object or a
    bare id string, the bare form lifted by the shared coercion so both
    arms land as ``failure_mode__id``.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class FaultDataFaultStates(ResponseModel):
    """The ``faultStates`` block: a WIRE-PLURAL NAME, a SINGULAR shape.

    Despite the plural wire key, the census shows ONE object carrying
    ``effectiveStatus`` (census-open plain str) — mirrored as a single
    nested model, never a list. Required within the block: a
    ``faultStates`` block without its one observed key is a shape
    change and must fail loudly.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    effective_status: str


class FaultData(ResponseModel):
    """One GeoTab engine-fault observation from the FaultData feed.

    The wave-two conservative mirror (the module docstring's posture):
    ``id`` / ``date_time`` / ``device`` required, everything else
    optional even where census-total.

    Attributes:
        amber_warning_lamp: The amber-warning lamp state.
        controller: The source controller reference (object-only on
            this census; defensively lifted).
        count: The fault's occurrence count.
        date_time: The fault's UTC instant — the endpoint's event time.
        device: The faulting vehicle unit's reference.
        diagnostic: The fault's diagnostic-definition reference.
        diagnostic_severity: Rare-quartet severity token (2/2,000;
            census-open plain str).
        failure_mode: The failure-mode reference — proven
            object-or-string, both arms landing as ``failure_mode__id``.
        fault_state: The fault-state token (census-open plain str).
        fault_states: The plural-named singular status block.
        id: GeoTab's record id.
        malfunction_lamp: The malfunction-indicator lamp state.
        protect_warning_lamp: The protect-warning lamp state.
        red_stop_lamp: The red-stop lamp state.
        risk_of_breakdown: Rare-quartet breakdown-risk score (2/2,000).
        severity: Rare-quartet severity token (2/2,000; census-open).
        source_address: Rare-quartet source address (2/2,000).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    amber_warning_lamp: bool | None = None
    controller: Annotated[
        FaultDataControllerRef | None, BeforeValidator(bare_id_to_reference)
    ] = None
    count: int | None = None
    date_time: datetime
    device: Annotated[FaultDataDeviceRef, BeforeValidator(bare_id_to_reference)]
    diagnostic: Annotated[
        FaultDataDiagnosticRef | None, BeforeValidator(bare_id_to_reference)
    ] = None
    diagnostic_severity: str | None = None
    failure_mode: Annotated[
        FaultDataFailureModeRef | None, BeforeValidator(bare_id_to_reference)
    ] = None
    fault_state: str | None = None
    fault_states: FaultDataFaultStates | None = None
    id: str
    malfunction_lamp: bool | None = None
    protect_warning_lamp: bool | None = None
    red_stop_lamp: bool | None = None
    risk_of_breakdown: float | None = None
    severity: str | None = None
    source_address: int | None = None
