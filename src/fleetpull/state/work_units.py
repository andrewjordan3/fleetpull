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
a later pass, capped at ``max_attempts`` (``attempt_count`` increments at claim, so
a crash mid-execution counts) so a poison unit lets the backfill terminate.

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

from fleetpull.exceptions import ConfigurationError
from fleetpull.state.database import SqliteScalar, StateDatabase
from fleetpull.timing import Clock, from_iso8601, to_iso8601
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
    ``claimed`` when re-served under the attempt cap. The store writes and filters
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
        failed: Units that failed their last attempt (re-served under the cap).
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
    WHERE provider = ? AND endpoint = ?
          AND status IN (?, ?) AND attempt_count < ?
    ORDER BY unit_id
    LIMIT 1
)
RETURNING unit_id, partition_key, chunk_start, chunk_end, attempt_count
"""

_MARK_DONE_SQL: Final[str] = """
UPDATE work_units
SET status = ?, finished_at = ?
WHERE unit_id = ? AND status = ?
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
    unit_id, partition_key, chunk_start_text, chunk_end_text, attempt_count = row
    if not isinstance(unit_id, int):
        raise RuntimeError(f'work_units.unit_id was not an integer: {unit_id!r}')
    if partition_key is not None and not isinstance(partition_key, str):
        raise RuntimeError(
            f'work_units.partition_key was not text or null: {partition_key!r}'
        )
    if not isinstance(chunk_start_text, str):
        raise RuntimeError(f'work_units.chunk_start was not text: {chunk_start_text!r}')
    if not isinstance(chunk_end_text, str):
        raise RuntimeError(f'work_units.chunk_end was not text: {chunk_end_text!r}')
    if not isinstance(attempt_count, int):
        raise RuntimeError(
            f'work_units.attempt_count was not an integer: {attempt_count!r}'
        )
    try:
        chunk_start: datetime = from_iso8601(chunk_start_text)
        chunk_end: datetime = from_iso8601(chunk_end_text)
    except ValueError as error:
        raise ConfigurationError(
            'state database holds an unparseable work-unit chunk',
            provider=provider.value,
            endpoint=endpoint,
            detail=(
                f'work unit {unit_id} chunk '
                f'[{chunk_start_text!r}, {chunk_end_text!r}] is not ISO-8601 UTC'
            ),
        ) from error
    spec: WorkUnitSpec = WorkUnitSpec(
        provider=provider,
        endpoint=endpoint,
        partition_key=partition_key,
        chunk_start=chunk_start,
        chunk_end=chunk_end,
    )
    return ClaimedWorkUnit(unit_id=unit_id, spec=spec, attempt_count=attempt_count)


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
        with self._database.connect() as connection:
            changes_before: int = connection.total_changes
            connection.executemany(_INSERT_UNIT_SQL, rows)
            inserted: int = connection.total_changes - changes_before
            connection.commit()
        logger.debug('enqueued %s work units (%s new)', len(rows), inserted)
        return inserted

    def reset_claimed_to_pending(self, provider: Provider, endpoint: str) -> int:
        """
        Revert every ``claimed`` unit to ``pending`` (startup crash recovery).

        Called once at backfill startup per endpoint: with no workers live yet, any
        ``claimed`` row is stale (its worker died in a prior crashed invocation), so
        reverting it is safe — no lease, no heartbeat. ``attempt_count`` is left
        untouched, so a crash-looping unit still reaches the cap. ``done`` /
        ``failed`` / ``pending`` rows are not touched.

        Args:
            provider: The provider whose units to reset.
            endpoint: The endpoint whose units to reset.

        Returns:
            The number of units reverted from ``claimed`` to ``pending``.

        Side Effects:
            Opens a connection, updates the matching rows, and commits.
        """
        with self._database.connect() as connection:
            reset_count: int = connection.execute(
                _RESET_CLAIMED_SQL,
                (
                    WorkUnitStatus.PENDING.value,
                    provider.value,
                    endpoint,
                    WorkUnitStatus.CLAIMED.value,
                ),
            ).rowcount
            connection.commit()
        logger.debug(
            'reset %s claimed work units to pending: provider=%s endpoint=%s',
            reset_count,
            provider.value,
            endpoint,
        )
        return reset_count

    def claim_next(
        self, provider: Provider, endpoint: str, *, max_attempts: int
    ) -> ClaimedWorkUnit | None:
        """
        Atomically claim the lowest claimable unit, or return ``None``.

        Claims the lowest-``unit_id`` (FIFO) ``pending`` or ``failed`` unit whose
        ``attempt_count`` is below ``max_attempts``, in one atomic
        ``UPDATE ... RETURNING`` that flips it to ``claimed``, increments
        ``attempt_count`` (so a crash mid-execution still counts toward the cap),
        and clears the prior attempt's ``finished_at`` / ``last_error``. Safe under
        concurrency without an app-level lock: WAL serializes writers, so a
        competing claim's subquery re-evaluates only after this one commits.

        Args:
            provider: The provider whose queue to claim from.
            endpoint: The endpoint whose queue to claim from.
            max_attempts: The retry cap; a unit at or over it is skipped, so a
                poison unit lets the backfill terminate. Must be at least 1.

        Returns:
            The claimed unit, or ``None`` when nothing is claimable.

        Raises:
            ValueError: ``max_attempts`` is below 1 — it could never satisfy
                ``attempt_count < max_attempts`` and would silently claim nothing.
            ConfigurationError: The claimed row holds an unparseable chunk —
                state-store corruption.
            RuntimeError: A claimed column came back with a forbidden type — a
                SQLite contract violation.

        Side Effects:
            Opens a connection, claims one row, and commits.
        """
        if max_attempts < 1:
            raise ValueError(f'max_attempts must be at least 1, got {max_attempts}')
        claimed_at: str = to_iso8601(self._clock.now_utc())
        with self._database.connect() as connection:
            row: tuple[SqliteScalar, ...] | None = connection.execute(
                _CLAIM_NEXT_SQL,
                (
                    WorkUnitStatus.CLAIMED.value,
                    claimed_at,
                    provider.value,
                    endpoint,
                    WorkUnitStatus.PENDING.value,
                    WorkUnitStatus.FAILED.value,
                    max_attempts,
                ),
            ).fetchone()
            connection.commit()
        if row is None:
            return None
        claimed_unit: ClaimedWorkUnit = _build_claimed_unit(provider, endpoint, row)
        logger.debug(
            'claimed work unit: unit_id=%s attempt=%s',
            claimed_unit.unit_id,
            claimed_unit.attempt_count,
        )
        return claimed_unit

    def mark_done(self, unit_id: int) -> None:
        """
        Mark a claimed unit ``done``.

        Only a ``claimed`` unit may be completed; the guarded UPDATE matches no row
        otherwise.

        Args:
            unit_id: The claimed unit to complete.

        Raises:
            ValueError: No ``claimed`` unit has this ``unit_id`` (it does not exist
                or is not currently claimed) — a caller bug, kept stdlib.

        Side Effects:
            Opens a connection, updates the row, and commits.
        """
        finished_at: str = to_iso8601(self._clock.now_utc())
        with self._database.connect() as connection:
            affected: int = connection.execute(
                _MARK_DONE_SQL,
                (
                    WorkUnitStatus.DONE.value,
                    finished_at,
                    unit_id,
                    WorkUnitStatus.CLAIMED.value,
                ),
            ).rowcount
            if affected == 0:
                raise ValueError(
                    f'no claimed work unit with unit_id {unit_id} to mark done'
                )
            connection.commit()
        logger.debug('marked work unit done: unit_id=%s', unit_id)

    def mark_failed(self, unit_id: int, *, error_detail: str) -> None:
        """
        Mark a claimed unit ``failed`` with an error detail.

        Only a ``claimed`` unit may be failed. ``attempt_count`` is not touched here
        — it was already incremented at claim — so the cap still applies on retry.

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
        with self._database.connect() as connection:
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
            connection.commit()
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
            grouped_rows: list[tuple[SqliteScalar, SqliteScalar]] = connection.execute(
                _PROGRESS_SQL, (provider.value, endpoint)
            ).fetchall()
        counts: dict[str, int] = {}
        for status_value, count_value in grouped_rows:
            if not isinstance(status_value, str):
                raise RuntimeError(f'work_units.status was not text: {status_value!r}')
            if not isinstance(count_value, int):
                raise RuntimeError(
                    f'work-unit count was not an integer: {count_value!r}'
                )
            counts[status_value] = count_value
        return WorkUnitProgress(
            pending=counts.get(WorkUnitStatus.PENDING.value, 0),
            claimed=counts.get(WorkUnitStatus.CLAIMED.value, 0),
            done=counts.get(WorkUnitStatus.DONE.value, 0),
            failed=counts.get(WorkUnitStatus.FAILED.value, 0),
        )
