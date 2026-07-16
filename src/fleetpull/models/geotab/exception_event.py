# src/fleetpull/models/geotab/exception_event.py
"""The GeoTab ExceptionEvent response model (captured 2026-07-13).

One record per continuous rule-violation interval per device per rule:
the interval opens at ``active_from`` and closes at ``active_to``, and
``duration = active_to - active_from`` holds exactly on every captured
record, including a fractional-second span reproducing a fractional
``active_from`` (DESIGN §8). Records mutate after creation
(``last_modified_date_time`` observed ~17 minutes past
``created_date_time``) — the provider-level lookback absorbs it.

A pure mirror of the union of captured fields, everything optional.
``driver`` and ``diagnostic`` arrive as either a reference object or a
bare sentinel string (``"UnknownDriverId"``, ``"NoDiagnosticId"``) —
the shared ``bare_id_to_reference`` coercion lifts the bare form to
``{"id": <string>}`` verbatim, so the sentinel lands as the reference's
``id`` and its other fields null exactly on sentinel rows. The
object-form ``driver`` shape is inferred from Trip's captured grammar
(these captures carry only the sentinel); the first object-form capture
upgrades it to Captured.
"""

from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel
from fleetpull.models.geotab.shared import GeotabTimeSpan, bare_id_to_reference

__all__: list[str] = [
    'ExceptionEvent',
    'ExceptionEventDeviceRef',
    'ExceptionEventDiagnosticRef',
    'ExceptionEventDriverRef',
    'ExceptionEventRuleRef',
]


class ExceptionEventRuleRef(ResponseModel):
    """The event's rule reference: state, reason, and the rule id."""

    model_config = ConfigDict(alias_generator=to_camel)

    state: str | None = None
    reason: str | None = None
    id: str | None = None


class ExceptionEventDeviceRef(ResponseModel):
    """The event's device reference: the id alone."""

    model_config = ConfigDict(alias_generator=to_camel)

    id: str | None = None


class ExceptionEventDriverRef(ResponseModel):
    """The event's driver reference.

    Arrives as an object or the bare ``"UnknownDriverId"`` sentinel
    string; the ``ExceptionEvent.driver`` field's coercion lifts the
    bare form to ``{"id": <string>}``, so ``is_driver`` is null exactly
    on sentinel rows. The object-form shape mirrors Trip's captured
    driver grammar (inferred; no object-form ExceptionEvent capture
    exists yet).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str | None = None
    is_driver: bool | None = None


class ExceptionEventDiagnosticRef(ResponseModel):
    """The event's diagnostic reference.

    Arrives as the bare ``"NoDiagnosticId"`` sentinel string in every
    capture; the coercion lifts it to ``{"id": <string>}``.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str | None = None


class ExceptionEvent(ResponseModel):
    """One GeoTab ExceptionEvent: a rule-violation interval.

    Attributes:
        id: GeoTab's event id (an ``a``-prefixed GUID-like string — a
            different id space from the ``b``-hex entities).
        version: The record's version token.
        rule: The violated rule's reference (state, reason, rule id).
        device: The vehicle reference.
        driver: The driver reference; the bare ``"UnknownDriverId"``
            sentinel lands as ``driver.id`` verbatim.
        diagnostic: The diagnostic reference; the bare
            ``"NoDiagnosticId"`` sentinel lands as ``diagnostic.id``.
        active_from: Interval start (UTC) — the endpoint's event time
            and partition anchor.
        active_to: Interval end (UTC).
        duration: The interval length (.NET TimeSpan on the wire);
            reproduces ``active_to - active_from`` exactly in capture.
        distance: Distance traveled during the interval, km.
        state: The event's lifecycle state token, mirrored verbatim.
        created_date_time: When the provider materialized the record.
        last_modified_date_time: The record's last mutation.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    # Identity.
    id: str | None = None
    version: str | None = None
    rule: ExceptionEventRuleRef | None = None
    device: ExceptionEventDeviceRef | None = None
    driver: Annotated[
        ExceptionEventDriverRef | None, BeforeValidator(bare_id_to_reference)
    ] = None
    diagnostic: Annotated[
        ExceptionEventDiagnosticRef | None, BeforeValidator(bare_id_to_reference)
    ] = None

    # The interval.
    active_from: datetime | None = None
    active_to: datetime | None = None
    duration: GeotabTimeSpan = None

    # Measures and state.
    distance: float | None = None
    state: str | None = None
    created_date_time: datetime | None = None
    last_modified_date_time: datetime | None = None
