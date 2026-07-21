# src/fleetpull/orchestrator/watermark_drive.py
"""The watermark drive: the run executor's plan-and-drive unit-loop arm.

``WatermarkDrive`` drives one watermark endpoint's run (DESIGN sections 4/5):
re-claim any incomplete work units and drive them (``backfill_unit_workers``
concurrently), then resolve the residual window exactly as before --
watermark less lookback (floored), else coverage frontier, else the
cold-start anchor, against the cutoff trailing edge -- plan it into
``backfill_chunk_days`` units (or the endpoint's declared
``fixed_unit_days``, which wins over config), and drive those. Every unit is
its own transaction: fetch the unit's window (the fan-out threads unchanged
within it), write parquet, record the ledger row, and mark the unit done
with its folded observation. The watermark advances on the PREFIX-ADVANCE
rule (2026-07-20): after each completion, to the maximum observation across
the contiguous done-prefix, through the cursor store's atomic forward-only
write -- so units may complete in any order and every persisted watermark is
still true at every instant; a crash resumes from the work-units ledger
instead of refetching the window. The pure resume decisions live in
``orchestrator/resume.py``, the per-batch transform in
``orchestrator/batch.py``, and the claim choreography in
``orchestrator/unit_loop.py``, so the drive only orchestrates -- read state,
call pure functions, write state.
"""

import logging
from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta
from functools import partial

from fleetpull.endpoints.shared import EndpointDefinition, WatermarkMode
from fleetpull.exceptions import ConfigurationError
from fleetpull.incremental import (
    DateWindow,
    resolve_resume_start,
    resolve_trailing_edge,
    window_or_none,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.orchestrator.backfill import plan_backfill_units
from fleetpull.orchestrator.batch import ProcessedBatch, WindowContext
from fleetpull.orchestrator.drivers import RequestDriver
from fleetpull.orchestrator.outcome import CaughtUp, Executed, RunOutcome
from fleetpull.orchestrator.recording import recorded_run
from fleetpull.orchestrator.resume import resolve_watermark_start
from fleetpull.orchestrator.spine import RunnerSpine
from fleetpull.orchestrator.streaming import (
    BatchObserver,
    drain_batches,
    observe_batches,
    stream_processed_batches,
)
from fleetpull.orchestrator.unit_loop import UnitCrew, drive_claimable_units
from fleetpull.storage import WriteResult
from fleetpull.timing import to_iso8601
from fleetpull.vocabulary import Provider

__all__: list[str] = ['WatermarkDrive']

logger = logging.getLogger(__name__)


def _window_context(
    definition: EndpointDefinition[ResponseModel], window: DateWindow, now: datetime
) -> WindowContext:
    """Build the per-batch transform context, asserting the event-time column.

    A watermark endpoint always declares an ``event_time_column`` (the endpoint
    definition forbids otherwise); this narrows it for the type checker and fails
    loudly if that invariant is ever broken.

    Args:
        definition: The watermark endpoint being run.
        window: The run's half-open window.
        now: The run instant (the future-event guard's bound).

    Returns:
        The ``WindowContext`` for ``process_batch``.

    Raises:
        ConfigurationError: The endpoint declares no event-time column.
    """
    event_time_column = definition.event_time_column
    if event_time_column is None:
        raise ConfigurationError(
            'watermark endpoint has no event_time_column',
            provider=definition.provider.value,
            endpoint=definition.name,
        )
    return WindowContext(window=window, now=now, event_time_column=event_time_column)


def _merge_executed(outcomes: Sequence[Executed]) -> Executed:
    """Fold the driven units' outcomes into the invocation's one ``Executed``.

    Counts sum; the pruned partitions concatenate in drive order (a residual
    unit re-covering a leftover unit's dates may repeat one -- the report is
    informational, never consumed as a set).

    Args:
        outcomes: The per-unit outcomes, in drive order; at least one.

    Returns:
        The aggregated ``Executed``.
    """
    observations = [
        outcome.latest_observed
        for outcome in outcomes
        if outcome.latest_observed is not None
    ]
    return Executed(
        records_fetched=sum(outcome.records_fetched for outcome in outcomes),
        write=WriteResult(
            rows_written=sum(outcome.write.rows_written for outcome in outcomes),
            duplicates_dropped=sum(
                outcome.write.duplicates_dropped for outcome in outcomes
            ),
            files_written=sum(outcome.write.files_written for outcome in outcomes),
            deleted_partitions=tuple(
                deleted_date
                for outcome in outcomes
                for deleted_date in outcome.write.deleted_partitions
            ),
        ),
        latest_observed=max(observations) if observations else None,
    )


class WatermarkDrive:
    """Drives one watermark endpoint's run through the plan-and-drive loop.

    Constructed once by ``EndpointRunner.__init__`` from the shared spine;
    ``run`` takes the endpoint, its request driver, its mode, and the
    optional per-frame observer.
    """

    def __init__(self, spine: RunnerSpine) -> None:
        """
        Args:
            spine: The shared drive kit -- collaborators plus the runner's
                writer-factory and projection seams.
        """
        self._spine = spine

    def run(
        self,
        definition: EndpointDefinition[ResponseModel],
        driver: RequestDriver,
        mode: WatermarkMode,
        observer: BatchObserver | None,
    ) -> RunOutcome:
        """Run the watermark arm: the plan-and-drive unit loop.

        Incomplete units outrank the watermark: orphaned ``claimed`` units are
        reset (an in-progress unit found at run start is by definition
        orphaned -- fleetpull assumes a single driver per state database) and
        every claimable unit is re-claimed and driven,
        ``backfill_unit_workers`` at a time. Then the residual window is
        resolved exactly as before and planned into units
        (``_plan_residual_units``), and those are driven the same
        way. Each unit commits
        independently; the watermark advances per completion across the
        contiguous done-prefix (the prefix-advance rule, so out-of-order
        completions never overstate it); a failing unit returns to a
        claimable state and fails the endpoint after in-flight siblings
        finish. An invocation that drove nothing is ``CaughtUp``
        -- the resume point had reached the trailing edge, or every planned
        unit was already complete. After the last unit commits, the merged
        outcome projects into ``metadata.json`` (post-commit, best-effort);
        a ``CaughtUp`` invocation writes nothing.

        Args:
            definition: The watermark endpoint to run.
            driver: The request driver supplying each unit's batches.
            mode: The endpoint's watermark mode (lookback and cutoff).
            observer: The optional per-frame hook, applied within every unit.

        Returns:
            ``Executed`` aggregated over the driven units, or ``CaughtUp``
            when nothing was driven.

        Raises:
            ConfigurationError: A stored watermark dated after ``now`` (Guard A), a
                cross-mode feed cursor on this endpoint, or a missing event-time
                column.
            FleetpullError: A fetch, validation, write, guard, or completion failure
                -- the failed unit's run is recorded failed, the unit returns to a
                claimable state, and the error re-raises.
        """
        provider = definition.provider
        name = definition.name
        state = self._spine.state
        # The cursor guards (Guard A, cross-mode) fire before any unit
        # drives; the residual resolution below re-derives this value after
        # the leftover units have advanced the cursor.
        resolve_watermark_start(
            state.cursors.get_cursor(provider, name),
            mode.lookback,
            self._spine.clock.now_utc(),
            provider,
            name,
        )
        state.units.reset_claimed_to_pending(provider, name)
        commit_prefix = partial(self._commit_watermark_prefix, provider, name)
        # Heal a crash that landed between a unit's done-mark and its prefix
        # commit: the prefix read is cheap and the advance is atomic and
        # forward-only, so an up-to-date cursor makes this a no-op.
        commit_prefix()
        crew = UnitCrew(
            queue=state.units,
            provider=provider,
            endpoint=name,
            drive_unit=partial(self._drive_unit, definition, driver, observer),
            commit_prefix=commit_prefix,
        )
        workers = self._spine.sync.backfill_unit_workers
        outcomes = drive_claimable_units(crew, workers=workers)
        residual = self._resolve_residual_window(definition, mode)
        if residual is not None:
            self._plan_residual_units(definition, mode, residual)
            outcomes.extend(drive_claimable_units(crew, workers=workers))
        if not outcomes:
            logger.info('caught up: provider=%s endpoint=%s', provider.value, name)
            return CaughtUp()
        merged = _merge_executed(outcomes)
        # The residual window is the run's resolved window; a run that only
        # re-drove leftover units resolved none, and its projection carries a
        # null window.
        self._spine.projection.project(definition, merged, window=residual)
        return merged

    def _commit_watermark_prefix(self, provider: Provider, name: str) -> None:
        """Advance the watermark across the contiguous done-prefix.

        The prefix-advance rule's commit (DESIGN section 5, 2026-07-20):
        read the maximum observation over the endpoint's contiguous
        done-prefix and advance the watermark to it through the store's
        atomic forward-only write. Invoked after every unit completion (and
        once at run start, healing a crash between a done-mark and its
        commit); concurrent invocations are safe -- the guard lives inside
        the store's statement, so a stale prefix read can never write the
        cursor backward.

        Args:
            provider: The provider whose watermark to commit.
            name: The endpoint whose watermark to commit.

        Side Effects:
            May advance the endpoint's cursor row.
        """
        state = self._spine.state
        observation = state.units.done_prefix_observation(provider, name)
        if observation is not None:
            state.cursors.advance_watermark_forward(provider, name, observation)

    def _resolve_residual_window(
        self,
        definition: EndpointDefinition[ResponseModel],
        mode: WatermarkMode,
    ) -> DateWindow | None:
        """Resolve the not-yet-planned residual window, exactly as the resume chain.

        The same resolution the whole-window arm performed, run after the
        leftover units have driven: the stored watermark less the lookback
        margin (floored to its UTC midnight), else the coverage frontier,
        else the cold-start anchor -- against the cutoff-held trailing edge.
        Both bounds are midnight-aligned by construction, which is what makes
        the result plannable into whole-day units.

        Args:
            definition: The watermark endpoint being run.
            mode: The endpoint's watermark mode (lookback and cutoff).

        Returns:
            The residual ``DateWindow``, or ``None`` when the resume point
            has reached the trailing edge (nothing new to plan).

        Raises:
            ConfigurationError: A stored watermark dated after ``now`` (Guard
                A) or a cross-mode feed cursor (from
                ``resolve_watermark_start``).
        """
        state = self._spine.state
        now = self._spine.clock.now_utc()
        end = resolve_trailing_edge(now, mode.cutoff)
        stored = state.cursors.get_cursor(definition.provider, definition.name)
        watermark_start = resolve_watermark_start(
            stored, mode.lookback, now, definition.provider, definition.name
        )
        frontier = state.recorder.coverage_frontier(
            definition.provider, definition.name
        )
        start = resolve_resume_start(
            watermark_start, frontier, self._spine.sync.default_start_datetime
        )
        return window_or_none(start, end)

    def _plan_residual_units(
        self,
        definition: EndpointDefinition[ResponseModel],
        mode: WatermarkMode,
        residual: DateWindow,
    ) -> None:
        """Tile the residual window into work units and enqueue them.

        The residual-planning step of the plan-and-drive loop: pick the unit
        width (the endpoint's declared ``fixed_unit_days`` wins over config
        -- on a window-grain rollup surface the unit width is part of the
        row's meaning, so it must never float with
        ``sync.backfill_chunk_days``; config remains the default for
        endpoints declaring ``None``), tile the window into units, enqueue
        them idempotently, and narrate the plan -- the one moment the
        resolved window and its claimable-unit count (the idempotent
        enqueue's newly inserted rows; leftovers were driven already) are
        both in hand.

        Args:
            definition: The watermark endpoint being run.
            mode: The endpoint's watermark mode (the fixed unit width, when
                declared).
            residual: The resolved residual window to plan.

        Side Effects:
            Enqueues the planned units and emits one INFO line.
        """
        provider = definition.provider
        name = definition.name
        chunk_days = (
            self._spine.sync.backfill_chunk_days
            if mode.fixed_unit_days is None
            else mode.fixed_unit_days
        )
        chunk = timedelta(days=chunk_days)
        claimable_units = self._spine.state.units.enqueue(
            plan_backfill_units(provider, name, residual, chunk)
        )
        logger.info(
            'window planned: provider=%s endpoint=%s window_start=%s '
            'window_end=%s claimable_units=%d',
            provider.value,
            name,
            to_iso8601(residual.start),
            to_iso8601(residual.end),
            claimable_units,
        )

    def _drive_unit(
        self,
        definition: EndpointDefinition[ResponseModel],
        driver: RequestDriver,
        observer: BatchObserver | None,
        window: DateWindow,
    ) -> Executed:
        """Drive one unit: fetch its window, write, record.

        The per-unit transaction the claim loop invokes: build the unit's
        batch stream (the fan-out threads the unit's (member x window) pieces
        on the ``FetchPool``, unchanged) and run it through the commit spine.
        The unit's folded observation rides the returned outcome; the
        watermark advance is the unit loop's prefix commit, never this
        drive's.

        Args:
            definition: The watermark endpoint being run.
            driver: The request driver supplying the unit's batches.
            observer: The optional per-frame hook.
            window: The unit's half-open window.

        Returns:
            The unit's ``Executed`` outcome, its ``latest_observed`` carrying
            the folded in-window maximum (or ``None`` for an empty unit).

        Raises:
            FleetpullError: A fetch, validation, write, or completion failure
                -- the unit's run is recorded failed and the error re-raised.
        """
        client = self._spine.clients.client_for(definition.provider)
        now = self._spine.clock.now_utc()
        context = _window_context(definition, window, now)
        batches = observe_batches(
            stream_processed_batches(
                definition, driver, client, resume=window, context=context
            ),
            observer,
        )
        return self._execute_window(definition, batches, context)

    def _execute_window(
        self,
        definition: EndpointDefinition[ResponseModel],
        batches: Iterator[ProcessedBatch],
        context: WindowContext,
    ) -> Executed:
        """Run one window: open, consume/write/finalize, complete.

        The per-unit commit spine. It consumes an already-built
        processed-batch stream (the caller constructs it, wrapping in the
        batch observer where one applies), so the spine is blind to fetch
        mechanics. Opens a window run, writes each batch's in-window frame,
        folds the observed maximum, finalizes, and completes the run. The
        fold rides the returned outcome; the watermark advance belongs to
        the unit loop's prefix commit. The per-unit crash order is parquet
        -> ``complete_run`` (the ledger, here) -> ``mark_done`` (observed)
        -> prefix commit (both one level up): a crash after completion but
        before the done-mark leaves the unit claimable, so the next
        invocation re-drives it whole before resolving any residual --
        unit-gating plus the run-start prefix heal replace the retired
        cursor-before-completion ordering (DESIGN section 14).

        Args:
            definition: The endpoint being run.
            batches: The unit's processed-batch stream, ready to consume.
            context: The window, run instant, and event-time column for the
                per-batch transform.

        Returns:
            ``Executed`` with the fetched-row count, the write report, and
            the folded in-window maximum.

        Raises:
            FleetpullError: A fetch, validation, write, or completion failure -- the
                run is recorded failed and the original error re-raised.
        """
        window = context.window
        recorder = self._spine.state.recorder
        run_id = recorder.start_window_run(
            definition.provider, definition.name, window=window
        )
        with recorded_run(recorder, run_id):
            writer = self._spine.make_writer(definition, window)
            records_fetched, latest_observed = drain_batches(batches, writer)
            write = writer.finalize()
            # The cursor deliberately does NOT advance here: the unit's
            # observation rides the outcome to the unit loop, which records
            # it at mark_done and advances the watermark across the
            # contiguous done-prefix only (the prefix-advance rule, DESIGN
            # section 5) -- a per-unit advance would overstate coverage the
            # moment units complete out of order.
            recorder.complete_run(run_id, row_count=records_fetched)
            return Executed(
                records_fetched=records_fetched,
                write=write,
                latest_observed=latest_observed,
            )
