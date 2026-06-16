# src/fleetpull/state/run_ledger.py
"""The run ledger: the operational record of every fetch and the coverage frontier.

One row per run — one fetch of one (provider, endpoint) over one window (the
watermark arm: ``window_start``/``window_end``) or one version range (the feed
arm: ``from_version``/``to_version``), never both. A sync invocation produces many
runs; incremental and backfill-chunk fetches alike record one, so the ledger is
the single coverage source (DESIGN §5). Runs after ``migrate_to_head`` — the
``runs`` table must already exist.

Two-phase lifecycle: ``start_run`` inserts a ``running`` row stamped from the
injected ``Clock`` with exactly one range arm populated; ``complete_run`` closes
it ``succeeded`` with the row count (and a feed run's end ``toVersion``);
``fail_run`` closes it ``failed`` with an error detail. The single-arm invariant,
a non-negative row count, and a well-ordered window are guarded both here (the API
refuses a cross-arm or malformed write) and by the table's CHECK constraints (the
structural backstop), mirroring the cursor store's two-places-by-discipline split.

``coverage_frontier`` reads ``max(window_end)`` over an endpoint's ``succeeded``
runs — the implementation of DESIGN §4/§5 resume arm (2). It is watermark-only:
feed endpoints always hold a committed cursor and never reach this arm. A stored
``window_end`` that is not parseable ISO-8601 UTC is state-store corruption and
raises ``ConfigurationError``, the same stance as the cursor store. A crashed
run's stale ``running`` row is diagnostic only — the frontier filters
``succeeded`` — and reaping it is deferred.
"""

import logging
from datetime import datetime
from enum import StrEnum
from typing import Final

from fleetpull.exceptions import ConfigurationError
from fleetpull.state.database import SqliteScalar, StateDatabase
from fleetpull.timing import Clock, from_iso8601, to_iso8601
from fleetpull.vocabulary import Provider

__all__: list[str] = ['RunLedger', 'RunStatus']

logger = logging.getLogger(__name__)


class RunStatus(StrEnum):
    """
    The ``runs.status`` lifecycle value: ``running`` then ``succeeded`` / ``failed``.

    The store writes and filters on this closed set, so centralizing the three
    literals keeps them out of scattered string form. The values equal the schema
    CHECK literals exactly; the two are held in two places by the same boundary
    discipline as ``CursorKind`` (the migration runner owns its DDL, the store owns
    its writes, neither imports the other), pinned by the round-trip tests against a
    real migrated table with the CHECK active.
    """

    RUNNING = 'running'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'


_INSERT_RUN_SQL: Final[str] = """
INSERT INTO runs (
    provider, endpoint, status, window_start, window_end, from_version, started_at
)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

_SELECT_RUN_ARM_SQL: Final[str] = 'SELECT from_version FROM runs WHERE run_id = ?'

_COMPLETE_WATERMARK_RUN_SQL: Final[str] = """
UPDATE runs
SET status = ?, ended_at = ?, row_count = ?
WHERE run_id = ?
"""

_COMPLETE_FEED_RUN_SQL: Final[str] = """
UPDATE runs
SET status = ?, ended_at = ?, row_count = ?, to_version = ?
WHERE run_id = ?
"""

_FAIL_RUN_SQL: Final[str] = """
UPDATE runs
SET status = ?, ended_at = ?, error_detail = ?
WHERE run_id = ?
"""

_COVERAGE_FRONTIER_SQL: Final[str] = """
SELECT max(window_end) FROM runs
WHERE provider = ? AND endpoint = ? AND status = ? AND window_end IS NOT NULL
"""


class RunLedger:
    """
    Records each fetch run and answers the coverage frontier (DESIGN §5).

    One row per run — one fetch of one (provider, endpoint) over one window
    (watermark arm) or version range (feed arm). The two-phase lifecycle is
    :meth:`start_run` → :meth:`complete_run` / :meth:`fail_run`. The single range
    arm, a non-negative row count, and a well-ordered window are enforced both by
    the API guards here and by the table's CHECK constraints (the structural
    backstop). :meth:`coverage_frontier` is the watermark-only implementation of
    resume arm (2); feed endpoints hold a committed cursor and never reach it. Runs
    after ``migrate_to_head`` (the ``runs`` table must exist).

    Args:
        database: The initialized, migrated state database supplying connections.
        clock: The clock stamping ``started_at`` and ``ended_at``.
    """

    def __init__(self, database: StateDatabase, clock: Clock) -> None:
        self._database: StateDatabase = database
        self._clock: Clock = clock

    def start_run(
        self,
        provider: Provider,
        endpoint: str,
        *,
        window: tuple[datetime, datetime] | None = None,
        from_version: str | None = None,
    ) -> int:
        """
        Open a run: insert a ``running`` row with exactly one range arm populated.

        Exactly one of ``window`` or ``from_version`` must be given — the run's
        range arm. ``window`` is the watermark arm ``(window_start, window_end)``,
        both timezone-aware UTC and serialized via the timing codec;
        ``from_version`` is the feed arm's opaque start token.

        Args:
            provider: The provider being fetched.
            endpoint: The endpoint being fetched.
            window: The watermark arm: ``(window_start, window_end)``, both UTC,
                with ``window_start`` strictly before ``window_end``.
            from_version: The feed arm: the opaque start version.

        Returns:
            The new run's ``run_id`` (the table's rowid alias).

        Raises:
            ValueError: Neither or both arms were given; ``window_start`` is not
                strictly before ``window_end``; or a window bound is naive or not
                UTC (surfaced from the timing codec) — all caller bugs, kept stdlib.
            RuntimeError: The INSERT returned no ``lastrowid`` — a SQLite contract
                violation, surfaced loudly.

        Side Effects:
            Opens a connection, inserts one row, and commits.
        """
        if (window is None) == (from_version is None):
            raise ValueError(
                'start_run requires exactly one range arm: pass window or '
                'from_version, never both and never neither'
            )
        window_start_text: str | None = None
        window_end_text: str | None = None
        if window is not None:
            window_start, window_end = window
            if window_start >= window_end:
                raise ValueError('window_start must be strictly before window_end')
            window_start_text = to_iso8601(window_start)
            window_end_text = to_iso8601(window_end)
        started_at: str = to_iso8601(self._clock.now_utc())
        with self._database.connect() as connection:
            run_id: int | None = connection.execute(
                _INSERT_RUN_SQL,
                (
                    provider.value,
                    endpoint,
                    RunStatus.RUNNING.value,
                    window_start_text,
                    window_end_text,
                    from_version,
                    started_at,
                ),
            ).lastrowid
            connection.commit()
        if run_id is None:
            raise RuntimeError('runs INSERT returned no lastrowid')
        logger.debug(
            'started run: run_id=%s provider=%s endpoint=%s',
            run_id,
            provider.value,
            endpoint,
        )
        return run_id

    def complete_run(
        self, run_id: int, *, row_count: int, to_version: str | None = None
    ) -> None:
        """
        Close a run ``succeeded`` with its row count (and a feed run's ``toVersion``).

        Reads the run's range arm and refuses to cross it: a watermark run rejects
        a ``to_version``, a feed run requires one. The arm CHECK is the structural
        backstop behind these guards.

        Args:
            run_id: The run to close, from :meth:`start_run`.
            row_count: Rows fetched for the run; must be non-negative (zero is a
                valid empty fetch).
            to_version: The feed arm's end version — required for a feed run,
                refused for a watermark run.

        Raises:
            ValueError: ``row_count`` is negative; ``run_id`` is unknown; a
                watermark run was given a ``to_version``; or a feed run was not —
                all caller bugs, kept stdlib.

        Side Effects:
            Opens a connection, reads the run's arm, updates the row, and commits.
        """
        if row_count < 0:
            raise ValueError(f'row_count must be non-negative, got {row_count}')
        ended_at: str = to_iso8601(self._clock.now_utc())
        with self._database.connect() as connection:
            arm_row = connection.execute(_SELECT_RUN_ARM_SQL, (run_id,)).fetchone()
            if arm_row is None:
                raise ValueError(f'no run with run_id {run_id}')
            arm_from_version: SqliteScalar = arm_row[0]
            if arm_from_version is not None:
                if to_version is None:
                    raise ValueError('feed runs must record to_version on completion')
                connection.execute(
                    _COMPLETE_FEED_RUN_SQL,
                    (
                        RunStatus.SUCCEEDED.value,
                        ended_at,
                        row_count,
                        to_version,
                        run_id,
                    ),
                )
            else:
                if to_version is not None:
                    raise ValueError('to_version is only valid for feed runs')
                connection.execute(
                    _COMPLETE_WATERMARK_RUN_SQL,
                    (RunStatus.SUCCEEDED.value, ended_at, row_count, run_id),
                )
            connection.commit()
        logger.debug('completed run: run_id=%s row_count=%s', run_id, row_count)

    def fail_run(self, run_id: int, *, error_detail: str) -> None:
        """
        Close a run ``failed`` with an error detail.

        Arm-agnostic: failing a run touches no range column, so no arm read is
        needed — an UPDATE that matches no row is the unknown-``run_id`` signal.

        Args:
            run_id: The run to close, from :meth:`start_run`.
            error_detail: Human-readable failure context recorded on the row.

        Raises:
            ValueError: ``run_id`` is unknown (the UPDATE matched no row) — a
                caller bug, kept stdlib.

        Side Effects:
            Opens a connection, updates the row, and commits.
        """
        ended_at: str = to_iso8601(self._clock.now_utc())
        with self._database.connect() as connection:
            affected: int = connection.execute(
                _FAIL_RUN_SQL,
                (RunStatus.FAILED.value, ended_at, error_detail, run_id),
            ).rowcount
            if affected == 0:
                raise ValueError(f'no run with run_id {run_id}')
            connection.commit()
        logger.debug('failed run: run_id=%s', run_id)

    def coverage_frontier(self, provider: Provider, endpoint: str) -> datetime | None:
        """
        Return the high-water mark of completed watermark coverage, or ``None``.

        ``max(window_end)`` over this endpoint's ``succeeded`` runs carrying a
        window — the implementation of DESIGN §4/§5 resume arm (2). A backfill
        chunk that completed empty is still ``succeeded``, so its window counts and
        empty history is never re-scanned. The lexical ``max`` over the TEXT column
        is the chronological one because ``to_iso8601`` emits a fixed-width,
        zero-padded, ``Z``-suffixed form. Watermark-only by design: feed endpoints
        always hold a committed cursor and never reach this arm.

        Args:
            provider: The provider whose coverage to read.
            endpoint: The endpoint whose coverage to read.

        Returns:
            The latest covered ``window_end`` as a UTC datetime, or ``None`` when no
            succeeded watermark run exists for this (provider, endpoint).

        Raises:
            ConfigurationError: A stored ``window_end`` is not parseable ISO-8601
                UTC — state-store corruption, the same stance as the cursor store.
            RuntimeError: The aggregated ``window_end`` came back non-text,
                violating the STRICT ``TEXT`` schema contract.

        Side Effects:
            Opens a connection and reads one aggregate row.
        """
        with self._database.connect() as connection:
            frontier_row = connection.execute(
                _COVERAGE_FRONTIER_SQL,
                (provider.value, endpoint, RunStatus.SUCCEEDED.value),
            ).fetchone()
        # A bare max() always returns exactly one row; its single column is NULL
        # when no succeeded watermark run qualifies.
        frontier_text: SqliteScalar = frontier_row[0]
        if frontier_text is None:
            return None
        if not isinstance(frontier_text, str):
            raise RuntimeError(f'runs.window_end was not text: {frontier_text!r}')
        try:
            return from_iso8601(frontier_text)
        except ValueError as error:
            raise ConfigurationError(
                'state database holds an unparseable run window_end',
                provider=provider.value,
                endpoint=endpoint,
                detail=f'run window_end {frontier_text!r} is not ISO-8601 UTC',
            ) from error
