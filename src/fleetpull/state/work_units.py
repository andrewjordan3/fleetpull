# src/fleetpull/state/work_units.py
"""The work-units store: the backfill claim queue.

A backfill decomposes a (provider, endpoint) range into date chunks — and an
opaque ``partition_key`` (a vehicle id, a driver id, ...) for partitioned
endpoints — and this store is the claim queue over those units. It is dumb
persistence plus an atomic claim: it stores the units a caller plans, hands them
to fetch workers one at a time, records each outcome, and recovers from a crash —
knowing nothing about HTTP, parquet, chunking, or what a ``partition_key`` means
(DESIGN §5). Runs after ``migrate_to_head`` — the ``work_units`` table must exist.

Enqueue is idempotent (``INSERT OR IGNORE`` on the natural key, with partial unique
indexes so a NULL ``partition_key`` still dedups), so re-running a backfill plan
never duplicates units. ``claim_next`` is a single atomic ``UPDATE ... WHERE
unit_id = (SELECT ... LIMIT 1) RETURNING ...`` — safe under concurrency because WAL
serializes writers, no app-level lock — that takes the lowest claimable ``unit_id``
(FIFO), increments ``attempt_count``, and clears the prior attempt's outcome.
Lifecycle: ``pending → claimed → done | failed``; ``failed`` units are re-served on
a later pass, unconditionally — there is no attempt cap (``attempt_count`` still
increments at claim, recorded and narrated, so a crash mid-execution counts). A
finite cap would convert a persistent failure into a silently skipped unit — a
coverage hole behind an advancing watermark — so a poison unit fails the endpoint
loudly every invocation instead, and the prefix-advance rule's gap-unreachability
argument leans on exactly this always-claimable property (DESIGN §5); a future
bounded retry policy must re-derive that argument before adding one.

Crash recovery is a startup reset: a single ``fleetpull`` invocation runs the whole
backfill as one process, so at startup any ``claimed`` row is stale (its worker is
gone) and ``reset_claimed_to_pending`` reverts it — no lease, no heartbeat (sound
only because two invocations never run against one state DB at once). An
unparseable stored chunk is state-store corruption and raises ``ConfigurationError``,
the same stance as the cursor store.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Final

from fleetpull.state.database import (
    SqliteScalar,
    StateDatabase,
    expect_int,
    expect_text,
    parse_stored_instant,
)
from fleetpull.timing import Clock, to_iso8601
from fleetpull.vocabulary import Provider

__all__: list[str] = [
    'ClaimedWorkUnit',
    'WorkUnitProgress',
    'WorkUnitSpec',
    'WorkUnitStatus',
    'WorkUnitStore',
]

logger = logging.getLogger(__name__)


class WorkUnitStatus(StrEnum):
    """
    The ``work_units.status`` lifecycle value.

    ``pending`` → ``claimed`` → ``done`` / ``failed``; a ``failed`` unit returns to
    ``claimed`` when re-served on a later claim. The store writes and filters
    on this closed set, so centralizing the four literals keeps them out of
    scattered string form. The values equal the schema CHECK literals exactly; the
    two are held in two places by the same boundary discipline as ``CursorKind``
    (the migration runner owns its DDL, the store owns its writes, neither imports
    the other), pinned by the round-trip tests against a real migrated table.
    """

    PENDING = 'pending'
    CLAIMED = 'claimed'
    DONE = 'done'
    FAILED = 'failed'


@dataclass(frozen=True, slots=True)
class WorkUnitSpec:
    """
    One backfill unit to enqueue: a date chunk of an endpoint, optionally partitioned.

    Pure data, no validation — the ``chunk_start < chunk_end`` guard lives in
    :meth:`WorkUnitStore.enqueue`, as with the ledger.

    Attributes:
        provider: The provider being backfilled.
        endpoint: The endpoint being backfilled.
        partition_key: The opaque per-endpoint partition (a vehicle id, a driver
            id, ...), or ``None`` for an unpartitioned endpoint. The store never
            interprets it.
        chunk_start: The chunk's inclusive start, timezone-aware UTC.
        chunk_end: The chunk's end, timezone-aware UTC; must be after
            ``chunk_start``.
    """

    provider: Provider
    endpoint: str
    partition_key: str | None
    chunk_start: datetime
    chunk_end: datetime


@dataclass(frozen=True, slots=True)
class ClaimedWorkUnit:
    """
    A unit handed to a worker by :meth:`WorkUnitStore.claim_next`.

    Attributes:
        unit_id: The claimed unit's id (the table's rowid alias).
        spec: The unit's identity and chunk, reconstructed from the row.
        attempt_count: The post-increment attempt number of the claim that
            returned this unit (1 on the first claim).
    """

    unit_id: int
    spec: WorkUnitSpec
    attempt_count: int


@dataclass(frozen=True, slots=True)
class WorkUnitProgress:
    """
    Per-status unit counts for one (provider, endpoint), from :meth:`WorkUnitStore.progress`.

    Attributes:
        pending: Units awaiting claim.
        claimed: Units currently in flight.
        done: Units completed successfully.
        failed: Units that failed their last attempt (re-served on a later pass).
    """

    pending: int
    claimed: int
    done: int
    failed: int


_INSERT_UNIT_SQL: Final[str] = """
INSERT OR IGNORE INTO work_units (
    provider, endpoint, partition_key, chunk_start, chunk_end
)
VALUES (?, ?, ?, ?, ?)
"""

_RESET_CLAIMED_SQL: Final[str] = """
UPDATE work_units
SET status = ?
WHERE provider = ? AND endpoint = ? AND status = ?
"""

_CLAIM_NEXT_SQL: Final[str] = """
UPDATE work_units
SET status = ?, claimed_at = ?, attempt_count = attempt_count + 1,
    finished_at = NULL, last_error = NULL
WHERE unit_id = (
    SELECT unit_id FROM work_units
    WHERE provider = ? AND endpoint = ? AND status IN (?, ?)
    ORDER BY unit_id
    LIMIT 1
)
RETURNING unit_id, partition_key, chunk_start, chunk_end, attempt_count
"""

_MARK_DONE_SQL: Final[str] = """
UPDATE work_units
SET status = ?, finished_at = ?, observed_max = ?
WHERE unit_id = ? AND status = ?
"""

# The prefix-advance read (DESIGN section 5, 2026-07-20): the maximum
# observation across the contiguous done-prefix -- every done unit whose
# chunk starts before the earliest not-done unit (or all done units when
# none remains). MAX over ``to_iso8601``'s fixed-width Z-form is
# chronological; NULL observations (empty units, pre-v3 completions) are
# ignored by MAX, and an all-NULL prefix returns NULL.
_DONE_PREFIX_OBSERVATION_SQL: Final[str] = """
SELECT MAX(observed_max) FROM work_units
WHERE provider = ? AND endpoint = ? AND status = ?
  AND chunk_start < COALESCE(
      (SELECT MIN(chunk_start) FROM work_units
       WHERE provider = ? AND endpoint = ? AND status != ?),
      '9999-12-31T23:59:59Z'
  )
"""

_MARK_FAILED_SQL: Final[str] = """
UPDATE work_units
SET status = ?, finished_at = ?, last_error = ?
WHERE unit_id = ? AND status = ?
"""

_PROGRESS_SQL: Final[str] = """
SELECT status, count(*) FROM work_units
WHERE provider = ? AND endpoint = ?
GROUP BY status
"""


def _build_claimed_unit(
    provider: Provider, endpoint: str, row: tuple[SqliteScalar, ...]
) -> ClaimedWorkUnit:
    """
    Reconstruct a :class:`ClaimedWorkUnit` from a claim's ``RETURNING`` row.

    Narrows the row's columns to their STRICT types and parses the chunk bounds
    back from ISO-8601 UTC. The ``spec`` reuses the claim call's ``provider`` /
    ``endpoint`` plus the row's ``partition_key`` and chunks.

    Args:
        provider: The provider of the claim call (reused in the spec).
        endpoint: The endpoint of the claim call (reused in the spec).
        row: The ``RETURNING`` row: ``(unit_id, partition_key, chunk_start,
            chunk_end, attempt_count)``.

    Returns:
        The reconstructed claimed unit.

    Raises:
        ConfigurationError: A stored chunk bound is not parseable ISO-8601 UTC —
            state-store corruption, the cursor-store stance.
        RuntimeError: A column came back with a type the STRICT schema forbids — a
            SQLite contract violation, surfaced loudly.
    """
    raw_unit_id, raw_partition_key, raw_chunk_start, raw_chunk_end, raw_attempts = row
    unit_id = expect_int(raw_unit_id, 'work_units.unit_id')
    partition_key = (
        None
        if raw_partition_key is None
        else expect_text(raw_partition_key, 'work_units.partition_key')
    )
    spec: WorkUnitSpec = WorkUnitSpec(
        provider=provider,
        endpoint=endpoint,
        partition_key=partition_key,
        chunk_start=parse_stored_instant(
            expect_text(raw_chunk_start, 'work_units.chunk_start'),
            provider=provider,
            endpoint=endpoint,
            column=f'work-unit chunk_start (unit {unit_id})',
        ),
        chunk_end=parse_stored_instant(
            expect_text(raw_chunk_end, 'work_units.chunk_end'),
            provider=provider,
            endpoint=endpoint,
            column=f'work-unit chunk_end (unit {unit_id})',
        ),
    )
    return ClaimedWorkUnit(
        unit_id=unit_id,
        spec=spec,
        attempt_count=expect_int(raw_attempts, 'work_units.attempt_count'),
    )


class WorkUnitStore:
    """
    The backfill claim queue: enqueue, atomic claim, completion, and crash recovery.

    Dumb persistence plus an atomic claim (DESIGN §5). The caller plans the
    decomposition and drives enqueue/claim/execute/complete; the store only stores
    and serves units, knowing nothing about HTTP, parquet, chunking, or what a
    ``partition_key`` means. Runs after ``migrate_to_head`` (the ``work_units``
    table must exist).

    Args:
        database: The initialized, migrated state database supplying connections.
        clock: The clock stamping ``claimed_at`` and ``finished_at``.
    """

    def __init__(self, database: StateDatabase, clock: Clock) -> None:
        self._database: StateDatabase = database
        self._clock: Clock = clock

    def enqueue(self, units: Sequence[WorkUnitSpec]) -> int:
        """
        Insert work units idempotently, returning the number newly inserted.

        ``INSERT OR IGNORE`` on the natural key (provider, endpoint, partition_key,
        and the full window): re-enqueuing an existing unit inserts nothing, so a
        re-run of a backfill plan never duplicates units. A same-start /
        different-end unit is distinct (the store makes no overlap judgment — that
        is the caller's concern).

        Args:
            units: The units to enqueue; each must have ``chunk_start`` strictly
                before ``chunk_end``.

        Returns:
            The count of units actually inserted (existing ones are ignored).

        Raises:
            ValueError: A unit has ``chunk_start`` not strictly before
                ``chunk_end``, or a chunk bound is naive or not UTC (surfaced from
                the timing codec) — caller bugs, kept stdlib.

        Side Effects:
            Opens a connection, inserts the new rows, and commits.
        """
        rows: list[tuple[str, str, str | None, str, str]] = []
        for unit in units:
            if unit.chunk_start >= unit.chunk_end:
                raise ValueError('chunk_start must be strictly before chunk_end')
            rows.append(
                (
                    unit.provider.value,
                    unit.endpoint,
                    unit.partition_key,
                    to_iso8601(unit.chunk_start),
                    to_iso8601(unit.chunk_end),
                )
            )
        with self._database.transaction() as connection:
            changes_before: int = connection.total_changes
            connection.executemany(_INSERT_UNIT_SQL, rows)
            inserted: int = connection.total_changes - changes_before
        logger.debug('enqueued %s work units (%s new)', len(rows), inserted)
        return inserted

    def reset_claimed_to_pending(self, provider: Provider, endpoint: str) -> int:
        """
        Revert every ``claimed`` unit to ``pending`` (startup crash recovery).

        Called once at backfill startup per endpoint: with no workers live yet, any
        ``claimed`` row is stale (its worker died in a prior crashed invocation), so
        reverting it is safe — no lease, no heartbeat. ``attempt_count`` is left
        untouched, so a crash-looping unit's attempts stay recorded. ``done`` /
        ``failed`` / ``pending`` rows are not touched.

        Args:
            provider: The provider whose units to reset.
            endpoint: The endpoint whose units to reset.

        Returns:
            The number of units reverted from ``claimed`` to ``pending``.

        Side Effects:
            Opens a connection, updates the matching rows, and commits.
        """
        with self._database.transaction() as connection:
            reset_count: int = connection.execute(
                _RESET_CLAIMED_SQL,
                (
                    WorkUnitStatus.PENDING.value,
                    provider.value,
                    endpoint,
                    WorkUnitStatus.CLAIMED.value,
                ),
            ).rowcount
        logger.debug(
            'reset %s claimed work units to pending: provider=%s endpoint=%s',
            reset_count,
            provider.value,
            endpoint,
        )
        return reset_count

    def claim_next(self, provider: Provider, endpoint: str) -> ClaimedWorkUnit | None:
        """
        Atomically claim the lowest claimable unit, or return ``None``.

        Claims the lowest-``unit_id`` (FIFO) ``pending`` or ``failed`` unit, in
        one atomic ``UPDATE ... RETURNING`` that flips it to ``claimed``,
        increments ``attempt_count`` (recorded and narrated; a crash
        mid-execution still counts), and clears the prior attempt's
        ``finished_at`` / ``last_error``. There is deliberately no attempt cap
        (the module docstring carries the fail-loud rationale). Safe under
        concurrency without an app-level lock: WAL serializes writers, so a
        competing claim's subquery re-evaluates only after this one commits.

        Args:
            provider: The provider whose queue to claim from.
            endpoint: The endpoint whose queue to claim from.

        Returns:
            The claimed unit, or ``None`` when nothing is claimable.

        Raises:
            ConfigurationError: The claimed row holds an unparseable chunk —
                state-store corruption.
            RuntimeError: A claimed column came back with a forbidden type — a
                SQLite contract violation.

        Side Effects:
            Opens a connection, claims one row, and commits.
        """
        claimed_at: str = to_iso8601(self._clock.now_utc())
        with self._database.transaction() as connection:
            row = connection.execute(
                _CLAIM_NEXT_SQL,
                (
                    WorkUnitStatus.CLAIMED.value,
                    claimed_at,
                    provider.value,
                    endpoint,
                    WorkUnitStatus.PENDING.value,
                    WorkUnitStatus.FAILED.value,
                ),
            ).fetchone()
        if row is None:
            return None
        claimed_unit: ClaimedWorkUnit = _build_claimed_unit(provider, endpoint, row)
        logger.debug(
            'claimed work unit: unit_id=%s attempt=%s',
            claimed_unit.unit_id,
            claimed_unit.attempt_count,
        )
        return claimed_unit

    def mark_done(self, unit_id: int, *, observed_max: datetime | None) -> None:
        """
        Mark a claimed unit ``done``, recording its folded observation.

        Only a ``claimed`` unit may be completed; the guarded UPDATE matches no row
        otherwise. ``observed_max`` is the unit's folded in-window maximum event
        time — the prefix-advance watermark rule's datum
        (``done_prefix_observation`` reads it); ``None`` records an empty unit,
        which the prefix read ignores.

        Args:
            unit_id: The claimed unit to complete.
            observed_max: The unit's folded in-window maximum event time, or
                ``None`` when the unit observed no in-window event.

        Raises:
            ValueError: No ``claimed`` unit has this ``unit_id`` (it does not exist
                or is not currently claimed) — a caller bug, kept stdlib; or a
                naive / non-UTC ``observed_max`` (surfaced from the timing codec).

        Side Effects:
            Opens a connection, updates the row, and commits.
        """
        finished_at: str = to_iso8601(self._clock.now_utc())
        serialized_observation: str | None = (
            to_iso8601(observed_max) if observed_max is not None else None
        )
        with self._database.transaction() as connection:
            affected: int = connection.execute(
                _MARK_DONE_SQL,
                (
                    WorkUnitStatus.DONE.value,
                    finished_at,
                    serialized_observation,
                    unit_id,
                    WorkUnitStatus.CLAIMED.value,
                ),
            ).rowcount
            if affected == 0:
                raise ValueError(
                    f'no claimed work unit with unit_id {unit_id} to mark done'
                )
        logger.debug('marked work unit done: unit_id=%s', unit_id)

    def done_prefix_observation(
        self, provider: Provider, endpoint: str
    ) -> datetime | None:
        """
        The maximum observation across the contiguous done-prefix.

        The prefix-advance watermark rule's read (DESIGN §5, 2026-07-20): over
        this endpoint's units ordered by ``chunk_start``, take every ``done``
        unit before the earliest not-done one (all of them when none remains)
        and return the maximum recorded ``observed_max``. Out-of-order parallel
        completions beyond a gap contribute nothing until the gap closes, so a
        watermark advanced to this value never overstates coverage. The query
        is gap-blind by design: it sees only rows, so a hole no row represents
        — a never-enqueued window between done units — would not gate the
        prefix. Soundness therefore rests on the four §5 invariants (one
        enqueue site running only after the claim loop drains, the
        capless always-claimable queue, no row deletion, hole-free planner tiling),
        which make such a hole unreachable; the state-layer gap-blindness test
        pins this dependence.

        Args:
            provider: The provider whose units to read.
            endpoint: The endpoint whose units to read.

        Returns:
            The prefix's maximum observation, or ``None`` when the prefix is
            empty or holds only empty units.

        Raises:
            ConfigurationError: The stored observation is unparseable —
                state-store corruption, the store's uniform stance.
            RuntimeError: The observation column came back non-text,
                violating the STRICT schema contract.

        Side Effects:
            Opens a connection and reads.
        """
        done = WorkUnitStatus.DONE.value
        with self._database.connect() as connection:
            row: tuple[SqliteScalar] | None = connection.execute(
                _DONE_PREFIX_OBSERVATION_SQL,
                (provider.value, endpoint, done, provider.value, endpoint, done),
            ).fetchone()
        observation = row[0] if row is not None else None
        if observation is None:
            return None
        return parse_stored_instant(
            expect_text(observation, 'work_units.observed_max'),
            provider=provider,
            endpoint=endpoint,
            column='work-unit observed_max',
        )

    def mark_failed(self, unit_id: int, *, error_detail: str) -> None:
        """
        Mark a claimed unit ``failed`` with an error detail.

        Only a ``claimed`` unit may be failed. ``attempt_count`` is not touched here
        — it was already incremented at claim — so the attempt record stays true.

        Args:
            unit_id: The claimed unit to fail.
            error_detail: Human-readable failure context recorded on the row.

        Raises:
            ValueError: No ``claimed`` unit has this ``unit_id`` — a caller bug,
                kept stdlib.

        Side Effects:
            Opens a connection, updates the row, and commits.
        """
        finished_at: str = to_iso8601(self._clock.now_utc())
        with self._database.transaction() as connection:
            affected: int = connection.execute(
                _MARK_FAILED_SQL,
                (
                    WorkUnitStatus.FAILED.value,
                    finished_at,
                    error_detail,
                    unit_id,
                    WorkUnitStatus.CLAIMED.value,
                ),
            ).rowcount
            if affected == 0:
                raise ValueError(
                    f'no claimed work unit with unit_id {unit_id} to mark failed'
                )
        logger.debug('marked work unit failed: unit_id=%s', unit_id)

    def progress(self, provider: Provider, endpoint: str) -> WorkUnitProgress:
        """
        Return per-status unit counts for one (provider, endpoint).

        Args:
            provider: The provider whose units to count.
            endpoint: The endpoint whose units to count.

        Returns:
            The counts, with statuses absent from the queue reported as ``0``.

        Raises:
            RuntimeError: A grouped ``status`` or count came back with a forbidden
                type — a SQLite contract violation.

        Side Effects:
            Opens a connection and reads the grouped counts.
        """
        with self._database.connect() as connection:
            grouped_rows = connection.execute(
                _PROGRESS_SQL, (provider.value, endpoint)
            ).fetchall()
        counts: dict[str, int] = {}
        for status_value, count_value in grouped_rows:
            counts[expect_text(status_value, 'work_units.status')] = expect_int(
                count_value, 'work-unit count'
            )
        return WorkUnitProgress(
            pending=counts.get(WorkUnitStatus.PENDING.value, 0),
            claimed=counts.get(WorkUnitStatus.CLAIMED.value, 0),
            done=counts.get(WorkUnitStatus.DONE.value, 0),
            failed=counts.get(WorkUnitStatus.FAILED.value, 0),
        )
