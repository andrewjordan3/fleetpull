# src/fleetpull/state/run_ledger.py
"""The run ledger: the operational record of every fetch and the coverage frontier.

One row per run — one fetch of one (provider, endpoint) in one of three sync modes:
a *snapshot* (no range — a full current-state refetch), a *watermark* window
(``window_start``/``window_end``), or a *feed* version range
(``from_version``/``to_version``). A ``mode`` column records which, so the row is
self-describing; the range columns a run populates follow its mode. A sync
invocation produces many runs; incremental and backfill-chunk fetches alike record
one, so the ledger is the single coverage source (DESIGN §5). Runs after
``migrate_to_head`` — the ``runs`` table must already exist.

Two-phase lifecycle: one of ``start_snapshot_run`` / ``start_window_run`` /
``start_feed_run`` inserts a ``running`` row stamped from the injected ``Clock``
with the range shape its mode requires — three single-shape entry points, so an
impossible arm combination cannot be expressed; ``complete_run`` closes it
``succeeded`` with the row count (and a feed run's end ``toVersion``); ``fail_run``
closes it ``failed`` with an error detail. The mode-keyed arm shape, a non-negative
row count, and a well-ordered window are guarded both here (each entry point owns
its shape) and by the table's CHECK constraints (the structural backstop),
mirroring the cursor store's two-places-by-discipline split.

``coverage_frontier`` reads ``max(window_end)`` over an endpoint's ``succeeded``
runs — the implementation of DESIGN §4/§5 resume arm (2). It is watermark-only:
feed and snapshot endpoints never reach this arm (a feed endpoint holds a
committed cursor; a snapshot has no resume). A stored
``window_end`` that is not parseable ISO-8601 UTC is state-store corruption and
raises ``ConfigurationError``, the same stance as the cursor store. A crashed
run's stale ``running`` row is diagnostic only — the frontier filters
``succeeded`` — and reaping it is deferred.
"""

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Final

from fleetpull.exceptions import ConfigurationError
from fleetpull.state.database import SqliteScalar, StateDatabase
from fleetpull.timing import Clock, from_iso8601, to_iso8601
from fleetpull.vocabulary import Provider

__all__: list[str] = ['RunLedger', 'RunMode', 'RunStatus']

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


class RunMode(StrEnum):
    """
    The ``runs.mode`` discriminator: which sync mode produced the run.

    The persisted shadow of the endpoints layer's ``SyncMode`` variant —
    ``SnapshotMode`` -> ``SNAPSHOT``, ``WatermarkMode`` -> ``WATERMARK``,
    ``FeedMode`` -> ``FEED`` — recorded so the row is self-describing: a reader
    knows which range columns to expect without inferring from null patterns, and
    ``complete_run`` dispatches on it (only a feed run carries a ``to_version``). A
    StrEnum here, not the ``SyncMode`` union: ``SyncMode`` carries config
    (``WatermarkMode``'s lookback) and lives in ``endpoints/``, above ``state/``,
    so the ledger cannot import it; the orchestrator translates ``SyncMode`` to this
    tag when it records the run. The values equal the schema CHECK literals exactly,
    held in two places by the same boundary discipline as ``RunStatus`` and
    ``CursorKind``.
    """

    SNAPSHOT = 'snapshot'
    WATERMARK = 'watermark'
    FEED = 'feed'


_INSERT_RUN_SQL: Final[str] = """
INSERT INTO runs (
    provider, endpoint, status, mode,
    window_start, window_end, from_version, started_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

_SELECT_RUN_MODE_SQL: Final[str] = 'SELECT mode FROM runs WHERE run_id = ?'

_COMPLETE_NONFEED_RUN_SQL: Final[str] = """
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

_LAST_SUCCESS_SQL: Final[str] = """
SELECT max(ended_at) FROM runs
WHERE provider = ? AND endpoint = ? AND status = ?
"""


@dataclass(frozen=True, slots=True)
class _RunRange:
    """
    The mode-keyed range columns a ``runs`` row carries at insert.

    The three ``start_*_run`` entry points each build the range shape their mode
    requires — a watermark window, a feed start version, or (snapshot) nothing —
    and hand it to :meth:`RunLedger._insert_run` as one value instead of three
    parallel parameters. All fields default to ``None``; the table's mode-keyed
    CHECK is the structural backstop on the combination.

    Attributes:
        window_start_text: Serialized watermark window start, or ``None``.
        window_end_text: Serialized watermark window end, or ``None``.
        from_version: Feed start version, or ``None``.
    """

    window_start_text: str | None = None
    window_end_text: str | None = None
    from_version: str | None = None


def _read_run_mode(connection: sqlite3.Connection, run_id: int) -> RunMode:
    """
    Read a run's stored ``mode`` and narrow it to a :class:`RunMode`.

    The read half of :meth:`RunLedger.complete_run`'s mode dispatch, on the
    caller's already-open connection so the read and the closing UPDATE share
    one transaction.

    Args:
        connection: An open state-database connection; nothing is committed here.
        run_id: The run whose mode to read.

    Returns:
        The stored mode as a :class:`RunMode`.

    Raises:
        ValueError: No run has this ``run_id`` — a caller bug, kept stdlib.
        ConfigurationError: The stored ``mode`` is not a recognized ``RunMode``
            — state-store corruption, the same stance as the cursor store.
        RuntimeError: The stored ``mode`` came back non-text, violating the
            STRICT ``TEXT`` schema contract.

    Side Effects:
        Reads one row on ``connection``.
    """
    mode_row = connection.execute(_SELECT_RUN_MODE_SQL, (run_id,)).fetchone()
    if mode_row is None:
        raise ValueError(f'no run with run_id {run_id}')
    stored_mode: SqliteScalar = mode_row[0]
    if not isinstance(stored_mode, str):
        raise RuntimeError(f'runs.mode was not text: {stored_mode!r}')
    try:
        return RunMode(stored_mode)
    except ValueError as error:
        raise ConfigurationError(
            'state database holds an unrecognized run mode',
            detail=(
                f'run {run_id} mode {stored_mode!r} is not one of '
                f'{[member.value for member in RunMode]}'
            ),
        ) from error


def _max_over_succeeded_runs(
    database: StateDatabase,
    sql: str,
    provider: Provider,
    endpoint: str,
    *,
    column: str,
) -> datetime | None:
    """
    Run a ``max(...)``-over-``succeeded``-runs aggregate and parse its instant.

    The shared tail of :meth:`RunLedger.coverage_frontier` and
    :meth:`RunLedger.last_success_at`: execute ``sql`` bound to (provider,
    endpoint, ``succeeded``), narrow the aggregate's single nullable TEXT
    column, and parse it from ISO-8601 UTC. ``column`` names the aggregated
    ``runs`` column in the raised errors, so each caller keeps its own prose
    (``window_end`` / ``ended_at``).

    Args:
        database: The state database supplying the connection.
        sql: The aggregate query; must bind exactly ``(provider, endpoint,
            status)`` and return one row with one nullable TEXT column.
        provider: The provider whose runs to aggregate.
        endpoint: The endpoint whose runs to aggregate.
        column: The aggregated ``runs`` column's name, used in error prose.

    Returns:
        The parsed UTC datetime, or ``None`` when the aggregate's column is
        NULL (no succeeded run qualifies).

    Raises:
        ConfigurationError: The stored value is not parseable ISO-8601 UTC —
            state-store corruption, the same stance as the cursor store.
        RuntimeError: The aggregated value came back non-text, violating the
            STRICT ``TEXT`` schema contract.

    Side Effects:
        Opens a connection and reads one aggregate row.
    """
    with database.connect() as connection:
        aggregate_row = connection.execute(
            sql, (provider.value, endpoint, RunStatus.SUCCEEDED.value)
        ).fetchone()
    # A bare max() always returns exactly one row; its single column is NULL
    # when no succeeded run qualifies.
    instant_text: SqliteScalar = aggregate_row[0]
    if instant_text is None:
        return None
    if not isinstance(instant_text, str):
        raise RuntimeError(f'runs.{column} was not text: {instant_text!r}')
    try:
        return from_iso8601(instant_text)
    except ValueError as error:
        raise ConfigurationError(
            f'state database holds an unparseable run {column}',
            provider=provider.value,
            endpoint=endpoint,
            detail=f'run {column} {instant_text!r} is not ISO-8601 UTC',
        ) from error


class RunLedger:
    """
    Records each fetch run and answers the coverage frontier (DESIGN §5).

    One row per run — one fetch of one (provider, endpoint) in one of three sync
    modes (snapshot/watermark/feed), recorded in a ``mode`` column. The two-phase
    lifecycle is :meth:`start_snapshot_run` / :meth:`start_window_run` /
    :meth:`start_feed_run` → :meth:`complete_run` / :meth:`fail_run`. The mode-keyed
    arm shape, a non-negative row count, and a well-ordered window are enforced both
    by the per-mode entry points here and by the table's CHECK constraints (the
    structural backstop). :meth:`coverage_frontier` is the watermark-only
    implementation of resume arm (2); feed and snapshot endpoints never reach it.
    Runs after ``migrate_to_head`` (the ``runs`` table must exist).
    ``row_count`` uniformly means "records the run produced"; the sink those
    records landed in (a roster for a coordinator harvest, parquet for a
    runner-driven fetch) follows from the run's mode and origin.

    Args:
        database: The initialized, migrated state database supplying connections.
        clock: The clock stamping ``started_at`` and ``ended_at``.
    """

    def __init__(self, database: StateDatabase, clock: Clock) -> None:
        self._database: StateDatabase = database
        self._clock: Clock = clock

    def start_snapshot_run(self, provider: Provider, endpoint: str) -> int:
        """
        Open a snapshot run: a ``running`` row with no range (``mode='snapshot'``).

        A snapshot re-fetches the endpoint's full current state every run, so the
        row carries no window and no version. The mode-keyed CHECK is the structural
        backstop.

        Args:
            provider: The provider being fetched.
            endpoint: The endpoint being fetched.

        Returns:
            The new run's ``run_id`` (the table's rowid alias).

        Raises:
            RuntimeError: The INSERT returned no ``lastrowid`` — a SQLite contract
                violation, surfaced loudly.

        Side Effects:
            Opens a connection, inserts one row, and commits.
        """
        return self._insert_run(provider, endpoint, RunMode.SNAPSHOT, _RunRange())

    def start_window_run(
        self, provider: Provider, endpoint: str, *, window: tuple[datetime, datetime]
    ) -> int:
        """
        Open a watermark run over ``window`` (``mode='watermark'``).

        ``window`` is the half-open ``(window_start, window_end)`` the fetch covers,
        both timezone-aware UTC and serialized via the timing codec, with
        ``window_start`` strictly before ``window_end``.

        Args:
            provider: The provider being fetched.
            endpoint: The endpoint being fetched.
            window: ``(window_start, window_end)``, both UTC, ``window_start``
                strictly before ``window_end``.

        Returns:
            The new run's ``run_id`` (the table's rowid alias).

        Raises:
            ValueError: ``window_start`` is not strictly before ``window_end``; or a
                bound is naive or not UTC (surfaced from the timing codec) — caller
                bugs, kept stdlib.
            RuntimeError: The INSERT returned no ``lastrowid``.

        Side Effects:
            Opens a connection, inserts one row, and commits.
        """
        window_start, window_end = window
        if window_start >= window_end:
            raise ValueError('window_start must be strictly before window_end')
        return self._insert_run(
            provider,
            endpoint,
            RunMode.WATERMARK,
            _RunRange(
                window_start_text=to_iso8601(window_start),
                window_end_text=to_iso8601(window_end),
            ),
        )

    def start_feed_run(
        self, provider: Provider, endpoint: str, *, from_version: str
    ) -> int:
        """
        Open a feed run resuming from ``from_version`` (``mode='feed'``).

        ``from_version`` is the feed arm's opaque start token, stored verbatim
        (fleetpull never parses it). The run's end ``toVersion`` is recorded later
        by :meth:`complete_run`.

        Args:
            provider: The provider being fetched.
            endpoint: The endpoint being fetched.
            from_version: The feed arm's opaque start version.

        Returns:
            The new run's ``run_id`` (the table's rowid alias).

        Raises:
            RuntimeError: The INSERT returned no ``lastrowid``.

        Side Effects:
            Opens a connection, inserts one row, and commits.
        """
        return self._insert_run(
            provider, endpoint, RunMode.FEED, _RunRange(from_version=from_version)
        )

    def _insert_run(
        self, provider: Provider, endpoint: str, mode: RunMode, run_range: _RunRange
    ) -> int:
        """
        Insert one ``running`` row and return its ``run_id``.

        The shared tail of the three ``start_*_run`` entry points: each builds the
        ``_RunRange`` its mode requires, then delegates the stamp, insert, and
        ``lastrowid`` check here. The mode-keyed range shape is the caller's
        responsibility (and the CHECK's backstop); this helper persists what it is
        given.

        Raises:
            RuntimeError: The INSERT returned no ``lastrowid`` — a SQLite contract
                violation, surfaced loudly.
        """
        started_at: str = to_iso8601(self._clock.now_utc())
        with self._database.connect() as connection:
            run_id: int | None = connection.execute(
                _INSERT_RUN_SQL,
                (
                    provider.value,
                    endpoint,
                    RunStatus.RUNNING.value,
                    mode.value,
                    run_range.window_start_text,
                    run_range.window_end_text,
                    run_range.from_version,
                    started_at,
                ),
            ).lastrowid
            connection.commit()
        if run_id is None:
            raise RuntimeError('runs INSERT returned no lastrowid')
        logger.debug(
            'started run: run_id=%s provider=%s endpoint=%s mode=%s',
            run_id,
            provider.value,
            endpoint,
            mode.value,
        )
        return run_id

    def complete_run(
        self, run_id: int, *, row_count: int, to_version: str | None = None
    ) -> None:
        """
        Close a run ``succeeded`` with its row count (and a feed run's ``toVersion``).

        Reads the run's ``mode`` and refuses to cross it: snapshot and watermark
        runs reject a ``to_version``, a feed run requires one. The mode-keyed range
        CHECK is the structural backstop behind these guards.

        Args:
            run_id: The run to close, from one of the ``start_*_run`` methods.
            row_count: Rows fetched for the run; must be non-negative (zero is a
                valid empty fetch).
            to_version: The feed arm's end version — required for a feed run,
                refused for a snapshot or watermark run.

        Raises:
            ValueError: ``row_count`` is negative; ``run_id`` is unknown; a feed run
                was not given a ``to_version``; or a snapshot/watermark run was —
                all caller bugs, kept stdlib.
            ConfigurationError: the stored ``mode`` is not a recognized ``RunMode``
                — state-store corruption, the same stance as the cursor store.
            RuntimeError: the stored ``mode`` came back non-text, violating the
                STRICT ``TEXT`` schema contract.

        Side Effects:
            Opens a connection, reads the run's mode, updates the row, and commits.
        """
        if row_count < 0:
            raise ValueError(f'row_count must be non-negative, got {row_count}')
        ended_at: str = to_iso8601(self._clock.now_utc())
        with self._database.connect() as connection:
            mode: RunMode = _read_run_mode(connection, run_id)
            match mode:
                case RunMode.FEED:
                    if to_version is None:
                        raise ValueError(
                            'feed runs must record to_version on completion'
                        )
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
                case RunMode.SNAPSHOT | RunMode.WATERMARK:
                    if to_version is not None:
                        raise ValueError('to_version is only valid for feed runs')
                    connection.execute(
                        _COMPLETE_NONFEED_RUN_SQL,
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
            run_id: The run to close, from one of the ``start_*_run`` methods.
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
        zero-padded, ``Z``-suffixed form. Watermark-only by design: feed and
        snapshot endpoints never reach this arm (a feed endpoint holds a committed
        cursor; a snapshot has no resume).

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
        return _max_over_succeeded_runs(
            self._database,
            _COVERAGE_FRONTIER_SQL,
            provider,
            endpoint,
            column='window_end',
        )

    def last_success_at(self, provider: Provider, endpoint: str) -> datetime | None:
        """
        Return when this endpoint last completed successfully, or ``None``.

        ``max(ended_at)`` over this endpoint's ``succeeded`` runs -- the wall-clock
        completion time of its most recent success, across any sync mode (a snapshot
        feeder carries ``ended_at`` but no window). The roster's staleness bound reads
        this to decide whether a feeder re-list is due; it is not a resume arm, so
        unlike ``coverage_frontier`` it does not filter on ``window_end``. The lexical
        ``max`` over the TEXT column is the chronological one because ``to_iso8601``
        emits a fixed-width, zero-padded, ``Z``-suffixed form.

        Args:
            provider: The provider whose last success to read.
            endpoint: The endpoint whose last success to read.

        Returns:
            The latest ``ended_at`` of a succeeded run as a UTC datetime, or ``None``
            when no succeeded run exists for this (provider, endpoint).

        Raises:
            ConfigurationError: A stored ``ended_at`` is not parseable ISO-8601 UTC --
                state-store corruption, the same stance as ``coverage_frontier``.
            RuntimeError: The aggregated ``ended_at`` came back non-text, violating the
                STRICT ``TEXT`` schema contract.

        Side Effects:
            Opens a connection and reads one aggregate row.
        """
        return _max_over_succeeded_runs(
            self._database, _LAST_SUCCESS_SQL, provider, endpoint, column='ended_at'
        )
