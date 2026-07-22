# src/fleetpull/orchestrator/batch.py
"""Per-batch transform: validate and frame one record batch, and (watermark only)
guard, window-filter, and fold its event time.

``process_batch`` is the shared per-batch step both run-executor arms drive: the
snapshot arm passes ``context=None`` (validate + frame only); the watermark arm
passes a ``WindowContext`` to additionally apply the future-event guard, filter the
frame to the resume window, and produce the batch's in-window fold candidate.
``combine_latest_event_time`` folds those candidates across batches. Pure
transforms -- the caller writes the frame and commits the watermark (DESIGN §14).
"""

from dataclasses import dataclass
from datetime import datetime

import polars as pl

from fleetpull.endpoints.shared import EndpointDefinition
from fleetpull.incremental import DateWindow
from fleetpull.model_contract import ResponseModel
from fleetpull.records import latest_event_time, models_to_dataframe, validate_records
from fleetpull.storage import in_window
from fleetpull.vocabulary import JsonObject

__all__: list[str] = [
    'ProcessedBatch',
    'WindowContext',
    'combine_latest_event_time',
    'process_batch',
]


@dataclass(frozen=True, slots=True)
class WindowContext:
    """The watermark arm's per-batch context: window, clock, and event column.

    Present only on the watermark path; ``process_batch`` takes
    ``WindowContext | None`` and treats ``None`` as the snapshot path (no
    guard, no window filter, no fold candidate).

    Attributes:
        window: The resolved half-open ``[start, end)`` resume window.
        now: The run's clock instant, for the future-event guard.
        event_time_column: The response model's UTC datetime field the window
            filter and fold read.
    """

    window: DateWindow
    now: datetime
    event_time_column: str


@dataclass(frozen=True, slots=True)
class ProcessedBatch:
    """One processed batch: the frame to write and its fold candidate.

    Attributes:
        frame: The frame to hand to ``writer.write`` -- the validated, framed
            batch on the snapshot path; that frame filtered to the resume
            window on the watermark path. Its ``height`` is the run's per-batch
            row count (one row per validated model; the ledger counts the rows
            written for the window).
        latest_event_time: The maximum in-window event time in this batch, or
            ``None`` (snapshot path, or an empty/all-filtered batch). The
            cross-batch fold input.
    """

    frame: pl.DataFrame
    latest_event_time: datetime | None


def process_batch(
    batch: list[JsonObject],
    definition: EndpointDefinition[ResponseModel],
    context: WindowContext | None,
) -> ProcessedBatch:
    """Validate, frame, and (watermark only) window one batch.

    The shared per-batch transform both runner arms drive. Snapshot path
    (``context is None``): validate the raw records against the response model
    and frame them; the frame is written as-is and carries no fold candidate.
    Watermark path: additionally filter the frame to the resume window and fold
    the in-window maximum event time.

    An overlap- or pad-anchored endpoint legitimately returns records past the
    window's trailing edge -- including, on a long run, events that materialized
    AFTER the run clock but before wall-clock ``now`` (e.g. Motive
    ``idle_events``, whose company-local-day window is padded a day each side).
    That is an EXPECTED, HANDLED condition, not an anomaly: the window filter
    drops those records (they fall outside the resume window) and the next run's
    window covers them, so it is never a fatal error. The fold uses the
    *filtered* frame so such a record never advances the watermark past the
    trailing edge and skips the next run's cutoff holdback.

    Args:
        batch: One batch of raw response records from the driver.
        definition: The endpoint binding (response model for validation).
        context: The watermark per-batch context, or ``None`` for snapshot.

    Returns:
        The frame to write and its fold candidate.

    Raises:
        ProviderResponseError: Validation errors propagate from
            ``validate_records`` and framing errors from ``models_to_dataframe``
            unchanged. The watermark path adds no guard of its own.

    Side Effects:
        None -- pure transform; the caller writes the frame.
    """
    models = validate_records(batch, definition.response_model)
    frame = models_to_dataframe(models, definition.response_model)
    if context is None:
        return ProcessedBatch(frame=frame, latest_event_time=None)
    in_scope = frame.filter(in_window(context.event_time_column, context.window))
    return ProcessedBatch(
        frame=in_scope,
        latest_event_time=latest_event_time(in_scope, context.event_time_column),
    )


def combine_latest_event_time(
    running: datetime | None, candidate: datetime | None
) -> datetime | None:
    """Fold a batch's in-window max into the running watermark candidate.

    None-tolerant: an empty or all-filtered batch contributes ``None`` and
    leaves the running maximum unchanged.

    Args:
        running: The accumulated maximum so far, or ``None``.
        candidate: This batch's in-window maximum, or ``None``.

    Returns:
        The greater of the two, or whichever is non-``None``, or ``None``.

    Side Effects:
        None.
    """
    if running is None:
        return candidate
    if candidate is None:
        return running
    return max(running, candidate)
