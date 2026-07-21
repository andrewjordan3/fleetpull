"""The feed arm through the real runner, cursor store, and run ledger.

The four §14 invariants, each pinned against real state (a migrated SQLite
database) and real parquet:

- I1 -- a page's parquet always lands before its token commits (the
  ordering recorder snapshots the on-disk rows at every commit).
- I2 -- the token never moves past unwritten data (the crash simulation:
  a death between parquet and token leaves the PRIOR page's token stored).
- I3 -- append-only (the crash re-drive appends the duplicate page as new
  rows beside the first copy; the writer-level tripwire lives in
  tests/storage/test_append.py).
- I4 -- seed-once (the seed rides ONLY the tokenless first run; a stored
  token resumes with the token and never a seed).

Plus the drive's edges: the at-head empty terminal, the seeded empty cold
call, the cross-mode stored-watermark rejection, the no-durable-progress
wiring guard, the ledger row shapes, narration, and the metadata
projection.
"""

import json
import logging
import sqlite3
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from fleetpull.config import (
    FleetpullConfig,
    ProvidersConfig,
    StorageConfig,
    SyncConfig,
)
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    FeedMode,
    ResumeValue,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.exceptions import ConfigurationError
from fleetpull.incremental import FeedSeed, FeedToken, IncrementalCursor
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.orchestrator.outcome import Executed
from fleetpull.orchestrator.runner import CursorAccess, EndpointRunner, RunStateAccess
from fleetpull.state import (
    CursorStore,
    RunLedger,
    StateDatabase,
    WorkUnitStore,
    migrate_to_head,
)
from fleetpull.state.database import SqliteScalar
from fleetpull.timing import FrozenClock
from fleetpull.vocabulary import JsonObject, Provider, QuotaScope
from tests.orchestrator.doubles import StubClientSource, StubPageDecoder

_CLOCK_NOW = datetime(2026, 7, 21, tzinfo=UTC)
_DEFAULT_START = date(2024, 1, 1)
_SEED_LABEL = 'seed:2024-01-01T00:00:00Z'

_DAY_ONE = '2026-07-01T08:00:00Z'
_DAY_TWO = '2026-07-02T09:30:00Z'


class _FeedModel(ResponseModel):
    occurred_at: datetime
    reading: int


def _feed_definition() -> EndpointDefinition[_FeedModel]:
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='log_records',
        spec_builder=StaticGetSpecBuilder(base_url='https://x.test', path='/apiv1'),
        page_decoder=StubPageDecoder(),
        response_model=_FeedModel,
        quota_scope=QuotaScope.GEOTAB_FEED,
        storage_kind=StorageKind.APPEND_LOG,
        sync_mode=FeedMode(),
        event_time_column='occurred_at',
    )


def _records(*rows: tuple[str, int]) -> list[JsonObject]:
    return [
        {'occurred_at': occurred_at, 'reading': reading}
        for occurred_at, reading in rows
    ]


class ScriptedFeedDriver:
    """A RequestDriver double serving a scripted page list per resume value.

    The script keys on the resume shape: ``None`` serves the seeded first
    run, a token string serves the run resuming from that token. Every
    resume value handed to the driver is recorded, so the seed-once
    invariant (I4) is assertable on exactly what the drive passed down.
    """

    def __init__(self, script: dict[str | None, list[FetchedPage]]) -> None:
        self._script = script
        self.resumes: list[ResumeValue] = []

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[FetchedPage]:
        self.resumes.append(resume)
        match resume:
            case FeedSeed():
                yield from self._script[None]
            case FeedToken(from_version=from_version):
                yield from self._script[from_version]
            case _:
                raise AssertionError(f'feed drive passed a non-feed resume: {resume!r}')


class _StateBundle:
    """The real state surfaces over one migrated database, bundled for tests."""

    def __init__(self, root: Path, clock: FrozenClock) -> None:
        self.database_path = root / 'state.sqlite3'
        database = StateDatabase(self.database_path)
        database.initialize()
        migrate_to_head(database)
        self.cursors = CursorStore(database, clock)
        self.ledger = RunLedger(database, clock)
        self.units = WorkUnitStore(database, clock)


def _make_runner(
    state: _StateBundle,
    dataset_root: Path,
    clock: FrozenClock,
    cursors: CursorAccess | None = None,
) -> EndpointRunner:
    return EndpointRunner(
        StubClientSource(),
        RunStateAccess(
            recorder=state.ledger,
            cursors=cursors or state.cursors,
            units=state.units,
        ),
        clock,
        FleetpullConfig(
            sync=SyncConfig(default_start_date=_DEFAULT_START),
            storage=StorageConfig(dataset_root=dataset_root),
            providers=ProvidersConfig(),
        ),
    )


def _read_runs(database_path: Path) -> list[dict[str, SqliteScalar]]:
    """Every runs row as a dict, ascending run_id, via a bare connection."""
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute('SELECT * FROM runs ORDER BY run_id').fetchall()
    finally:
        connection.close()
    return [dict(row) for row in rows]


def _dataset_rows(endpoint_dir: Path) -> pl.DataFrame:
    """Every appended part file's rows, combined."""
    part_files = sorted(endpoint_dir.rglob('part-*.parquet'))
    assert part_files, f'no part files under {endpoint_dir}'
    return pl.concat([pl.read_parquet(part_file) for part_file in part_files])


@pytest.fixture
def clock() -> FrozenClock:
    return FrozenClock(start_time_utc=_CLOCK_NOW)


@pytest.fixture
def state(tmp_path: Path, clock: FrozenClock) -> _StateBundle:
    return _StateBundle(tmp_path, clock)


class TestSeedAndResume:
    """I4: the seed rides ONLY the tokenless first run."""

    def test_cold_run_seeds_from_the_default_start(
        self, tmp_path: Path, state: _StateBundle, clock: FrozenClock
    ) -> None:
        driver = ScriptedFeedDriver(
            {
                None: [
                    FetchedPage(
                        records=_records((_DAY_ONE, 1), (_DAY_ONE, 2)),
                        durable_progress='aaaa000000000001',
                    )
                ]
            }
        )
        runner = _make_runner(state, tmp_path, clock)
        outcome = runner.run(_feed_definition(), driver)
        assert isinstance(outcome, Executed)
        assert driver.resumes == [FeedSeed(start=datetime(2024, 1, 1, tzinfo=UTC))]
        assert state.cursors.get_cursor(Provider.GEOTAB, 'log_records') == FeedToken(
            from_version='aaaa000000000001'
        )

    def test_resumed_run_carries_the_token_and_never_a_seed(
        self, tmp_path: Path, state: _StateBundle, clock: FrozenClock
    ) -> None:
        state.cursors.commit_feed_token(Provider.GEOTAB, 'log_records', 'v5')
        driver = ScriptedFeedDriver(
            {
                'v5': [
                    FetchedPage(records=_records((_DAY_ONE, 1)), durable_progress='v6')
                ]
            }
        )
        runner = _make_runner(state, tmp_path, clock)
        runner.run(_feed_definition(), driver)
        assert driver.resumes == [FeedToken(from_version='v5')]

    def test_ledger_rows_carry_the_resume_and_end_versions(
        self, tmp_path: Path, state: _StateBundle, clock: FrozenClock
    ) -> None:
        driver = ScriptedFeedDriver(
            {
                None: [
                    FetchedPage(records=_records((_DAY_ONE, 1)), durable_progress='v1'),
                    FetchedPage(records=[], durable_progress='v1'),
                ]
            }
        )
        runner = _make_runner(state, tmp_path, clock)
        runner.run(_feed_definition(), driver)
        (run,) = _read_runs(state.database_path)
        assert run['mode'] == 'feed'
        assert run['status'] == 'succeeded'
        assert run['from_version'] == _SEED_LABEL
        assert run['to_version'] == 'v1'
        assert run['row_count'] == 1
        assert run['window_start'] is None
        assert run['window_end'] is None


class _OrderRecordingCursors:
    """A CursorAccess proxy pinning I1: parquet on disk BEFORE each commit.

    At every ``commit_feed_token`` it counts the rows already appended
    under the endpoint directory, then delegates to the real store -- so
    the recorded ``(token, rows_on_disk)`` pairs prove the page's parquet
    landed before its token committed, at the exact interleaving point.
    """

    def __init__(self, real: CursorStore, endpoint_dir: Path) -> None:
        self._real = real
        self._endpoint_dir = endpoint_dir
        self.commits: list[tuple[str, int]] = []

    def get_cursor(self, provider: Provider, endpoint: str) -> IncrementalCursor | None:
        return self._real.get_cursor(provider, endpoint)

    def advance_watermark_forward(
        self, provider: Provider, endpoint: str, observed: datetime
    ) -> bool:
        raise AssertionError('the feed drive must never advance a watermark')

    def commit_feed_token(
        self, provider: Provider, endpoint: str, to_version: str
    ) -> None:
        rows_on_disk = sum(
            pl.read_parquet(part_file).height
            for part_file in self._endpoint_dir.rglob('part-*.parquet')
        )
        self.commits.append((to_version, rows_on_disk))
        self._real.commit_feed_token(provider, endpoint, to_version)


class TestPerPageCrashOrder:
    """I1/I2: parquet before token, per page; a crash loses at most a token."""

    def test_each_pages_parquet_is_on_disk_when_its_token_commits(
        self, tmp_path: Path, state: _StateBundle, clock: FrozenClock
    ) -> None:
        driver = ScriptedFeedDriver(
            {
                None: [
                    FetchedPage(
                        records=_records((_DAY_ONE, 1), (_DAY_ONE, 2)),
                        durable_progress='v1',
                    ),
                    FetchedPage(records=_records((_DAY_TWO, 3)), durable_progress='v2'),
                ]
            }
        )
        endpoint_dir = tmp_path / 'geotab' / 'log_records'
        recording = _OrderRecordingCursors(state.cursors, endpoint_dir)
        # typing-justified: the proxy satisfies CursorAccess structurally
        runner = _make_runner(state, tmp_path, clock, cursors=recording)
        runner.run(_feed_definition(), driver)
        # At each commit, every row of that page (and all before it) is
        # already durable: 2 rows at v1's commit, 3 at v2's.
        assert recording.commits == [('v1', 2), ('v2', 3)]

    def test_crash_between_parquet_and_token_holds_the_prior_token(
        self, tmp_path: Path, state: _StateBundle, clock: FrozenClock
    ) -> None:
        # Simulate death between page 2's parquet and its token: the commit
        # raises before touching the store. Page 2's rows are on disk
        # (harmless -- stored-as-emitted), the stored token is page 1's
        # (I2: the token is never past unwritten data, only behind written
        # data), and the run is recorded failed.
        class _DiesOnSecondCommit:
            def __init__(self, real: CursorStore) -> None:
                self._real = real
                self._commit_count = 0

            def get_cursor(
                self, provider: Provider, endpoint: str
            ) -> IncrementalCursor | None:
                return self._real.get_cursor(provider, endpoint)

            def advance_watermark_forward(
                self, provider: Provider, endpoint: str, observed: datetime
            ) -> bool:
                raise AssertionError('unreachable on the feed arm')

            def commit_feed_token(
                self, provider: Provider, endpoint: str, to_version: str
            ) -> None:
                self._commit_count += 1
                if self._commit_count == 2:
                    raise RuntimeError('simulated crash before the token commit')
                self._real.commit_feed_token(provider, endpoint, to_version)

        driver = ScriptedFeedDriver(
            {
                None: [
                    FetchedPage(
                        records=_records((_DAY_ONE, 1), (_DAY_ONE, 2)),
                        durable_progress='v1',
                    ),
                    FetchedPage(
                        records=_records((_DAY_TWO, 3), (_DAY_TWO, 4)),
                        durable_progress='v2',
                    ),
                ]
            }
        )
        dying = _DiesOnSecondCommit(state.cursors)
        # typing-justified: the proxy satisfies CursorAccess structurally
        runner = _make_runner(state, tmp_path, clock, cursors=dying)
        with pytest.raises(RuntimeError, match='simulated crash'):
            runner.run(_feed_definition(), driver)
        assert state.cursors.get_cursor(Provider.GEOTAB, 'log_records') == FeedToken(
            from_version='v1'
        )
        endpoint_dir = tmp_path / 'geotab' / 'log_records'
        assert _dataset_rows(endpoint_dir).height == 4
        (run,) = _read_runs(state.database_path)
        assert run['status'] == 'failed'

    def test_the_redrive_appends_the_duplicate_page_and_lands_the_token(
        self, tmp_path: Path, state: _StateBundle, clock: FrozenClock
    ) -> None:
        # The full crash-recovery story: run 1 dies between page 2's parquet
        # and its token; run 2 resumes from page 1's token, refetches exactly
        # that one page, appends its rows AGAIN as new parts (I3 -- the first
        # copy is untouched), and the token lands. Exactly-once data is the
        # consumer's (id, max version) reconcile, not the store's.
        class _DiesOnce:
            def __init__(self, real: CursorStore) -> None:
                self._real = real
                self._commit_count = 0
                self.armed = True

            def get_cursor(
                self, provider: Provider, endpoint: str
            ) -> IncrementalCursor | None:
                return self._real.get_cursor(provider, endpoint)

            def advance_watermark_forward(
                self, provider: Provider, endpoint: str, observed: datetime
            ) -> bool:
                raise AssertionError('unreachable on the feed arm')

            def commit_feed_token(
                self, provider: Provider, endpoint: str, to_version: str
            ) -> None:
                self._commit_count += 1
                if self.armed and self._commit_count == 2:
                    raise RuntimeError('simulated crash before the token commit')
                self._real.commit_feed_token(provider, endpoint, to_version)

        page_two_records = _records((_DAY_TWO, 3), (_DAY_TWO, 4))
        first_script: dict[str | None, list[FetchedPage]] = {
            None: [
                FetchedPage(records=_records((_DAY_ONE, 1)), durable_progress='v1'),
                FetchedPage(records=page_two_records, durable_progress='v2'),
            ]
        }
        dying = _DiesOnce(state.cursors)
        # typing-justified: the proxy satisfies CursorAccess structurally
        crashing_runner = _make_runner(state, tmp_path, clock, cursors=dying)
        with pytest.raises(RuntimeError, match='simulated crash'):
            crashing_runner.run(_feed_definition(), ScriptedFeedDriver(first_script))

        # Run 2: the protocol re-serves page 2 from v1, then the at-head
        # empty page.
        redrive_script: dict[str | None, list[FetchedPage]] = {
            'v1': [
                FetchedPage(records=page_two_records, durable_progress='v2'),
                FetchedPage(records=[], durable_progress='v2'),
            ]
        }
        redrive_driver = ScriptedFeedDriver(redrive_script)
        runner = _make_runner(state, tmp_path, clock)
        outcome = runner.run(_feed_definition(), redrive_driver)
        assert isinstance(outcome, Executed)
        assert redrive_driver.resumes == [FeedToken(from_version='v1')]
        assert state.cursors.get_cursor(Provider.GEOTAB, 'log_records') == FeedToken(
            from_version='v2'
        )
        endpoint_dir = tmp_path / 'geotab' / 'log_records'
        combined = _dataset_rows(endpoint_dir)
        # 1 (page one) + 2 (crashed page two) + 2 (its re-driven duplicate).
        assert combined.height == 5
        day_two_readings = sorted(
            combined.filter(pl.col('reading') >= 3)['reading'].to_list()
        )
        assert day_two_readings == [3, 3, 4, 4]
        runs = _read_runs(state.database_path)
        assert [run['status'] for run in runs] == ['failed', 'succeeded']
        assert runs[1]['from_version'] == 'v1'
        assert runs[1]['to_version'] == 'v2'


class TestTerminalEdges:
    def test_at_head_empty_page_recommits_the_unchanged_token(
        self, tmp_path: Path, state: _StateBundle, clock: FrozenClock
    ) -> None:
        state.cursors.commit_feed_token(Provider.GEOTAB, 'log_records', 'v9')
        driver = ScriptedFeedDriver(
            {'v9': [FetchedPage(records=[], durable_progress='v9')]}
        )
        runner = _make_runner(state, tmp_path, clock)
        outcome = runner.run(_feed_definition(), driver)
        assert isinstance(outcome, Executed)
        assert outcome.records_fetched == 0
        assert outcome.write.files_written == 0
        assert state.cursors.get_cursor(Provider.GEOTAB, 'log_records') == FeedToken(
            from_version='v9'
        )
        (run,) = _read_runs(state.database_path)
        assert run['status'] == 'succeeded'
        assert run['row_count'] == 0
        assert run['to_version'] == 'v9'
        assert not (tmp_path / 'geotab' / 'log_records').exists() or not list(
            (tmp_path / 'geotab' / 'log_records').rglob('*.parquet')
        )

    def test_seeded_empty_cold_call_commits_the_head_token(
        self, tmp_path: Path, state: _StateBundle, clock: FrozenClock
    ) -> None:
        # A seed at (or past) the head: one empty page carrying the head
        # toVersion. The feed always has a cursor to write (DESIGN §5).
        driver = ScriptedFeedDriver(
            {None: [FetchedPage(records=[], durable_progress='head0000000000ff')]}
        )
        runner = _make_runner(state, tmp_path, clock)
        outcome = runner.run(_feed_definition(), driver)
        assert isinstance(outcome, Executed)
        assert outcome.records_fetched == 0
        assert state.cursors.get_cursor(Provider.GEOTAB, 'log_records') == FeedToken(
            from_version='head0000000000ff'
        )
        (run,) = _read_runs(state.database_path)
        assert run['from_version'] == _SEED_LABEL
        assert run['to_version'] == 'head0000000000ff'


class TestGuards:
    def test_watermark_cursor_on_a_feed_endpoint_raises_before_any_run(
        self, tmp_path: Path, state: _StateBundle, clock: FrozenClock
    ) -> None:
        state.cursors.advance_watermark_forward(
            Provider.GEOTAB, 'log_records', datetime(2026, 6, 1, tzinfo=UTC)
        )
        runner = _make_runner(state, tmp_path, clock)
        with pytest.raises(ConfigurationError, match='watermark cursor'):
            runner.run(_feed_definition(), ScriptedFeedDriver({}))
        assert _read_runs(state.database_path) == []

    def test_page_without_durable_progress_raises_before_writing(
        self, tmp_path: Path, state: _StateBundle, clock: FrozenClock
    ) -> None:
        # A non-feed decoder wired to a feed endpoint: no token, no write,
        # run failed, cursor untouched.
        driver = ScriptedFeedDriver(
            {
                None: [
                    FetchedPage(records=_records((_DAY_ONE, 1)), durable_progress=None)
                ]
            }
        )
        runner = _make_runner(state, tmp_path, clock)
        with pytest.raises(ConfigurationError, match='durable progress'):
            runner.run(_feed_definition(), driver)
        assert state.cursors.get_cursor(Provider.GEOTAB, 'log_records') is None
        assert not (tmp_path / 'geotab' / 'log_records').exists()
        (run,) = _read_runs(state.database_path)
        assert run['status'] == 'failed'


class TestObserver:
    def test_feed_run_hands_each_validated_frame_to_the_observer(
        self, tmp_path: Path, state: _StateBundle, clock: FrozenClock
    ) -> None:
        observed: list[pl.DataFrame] = []
        driver = ScriptedFeedDriver(
            {
                None: [
                    FetchedPage(
                        records=_records((_DAY_ONE, 1), (_DAY_TWO, 2)),
                        durable_progress='v1',
                    )
                ]
            }
        )
        runner = _make_runner(state, tmp_path, clock)
        runner.run(_feed_definition(), driver, observed.append)
        assert len(observed) == 1
        assert observed[0].height == 2
        assert 'occurred_at' in observed[0].columns


class TestNarration:
    def test_seeded_run_narrates_seed_pages_and_completion(
        self,
        tmp_path: Path,
        state: _StateBundle,
        clock: FrozenClock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        driver = ScriptedFeedDriver(
            {
                None: [
                    FetchedPage(records=_records((_DAY_ONE, 1)), durable_progress='v1'),
                    FetchedPage(records=[], durable_progress='v1'),
                ]
            }
        )
        runner = _make_runner(state, tmp_path, clock)
        with caplog.at_level(logging.DEBUG, logger='fleetpull'):
            runner.run(_feed_definition(), driver)
        info_lines = [
            record.getMessage()
            for record in caplog.records
            if record.levelno == logging.INFO
        ]
        assert any(
            line.startswith('feed run seeded:') and 'from_date=2024-01-01' in line
            for line in info_lines
        )
        assert any(
            line.startswith('feed complete:') and 'pages=2' in line
            for line in info_lines
        )
        debug_lines = [
            record.getMessage()
            for record in caplog.records
            if record.levelno == logging.DEBUG
        ]
        assert (
            len([line for line in debug_lines if line.startswith('feed page appended')])
            == 2
        )

    def test_resumed_run_narrates_the_from_version(
        self,
        tmp_path: Path,
        state: _StateBundle,
        clock: FrozenClock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        state.cursors.commit_feed_token(Provider.GEOTAB, 'log_records', 'v5')
        driver = ScriptedFeedDriver(
            {'v5': [FetchedPage(records=[], durable_progress='v5')]}
        )
        runner = _make_runner(state, tmp_path, clock)
        with caplog.at_level(logging.INFO, logger='fleetpull'):
            runner.run(_feed_definition(), driver)
        assert any(
            record.getMessage().startswith('feed run resumed:')
            and 'from_version=v5' in record.getMessage()
            for record in caplog.records
        )


class TestMetadataProjection:
    def test_feed_run_projects_the_token_cursor_and_no_window(
        self, tmp_path: Path, state: _StateBundle, clock: FrozenClock
    ) -> None:
        driver = ScriptedFeedDriver(
            {
                None: [
                    FetchedPage(records=_records((_DAY_ONE, 1)), durable_progress='v1')
                ]
            }
        )
        runner = _make_runner(state, tmp_path, clock)
        runner.run(_feed_definition(), driver)
        metadata_path = tmp_path / 'geotab' / 'log_records' / 'metadata.json'
        metadata = json.loads(metadata_path.read_text())
        assert metadata['sync_mode'] == 'feed'
        assert metadata['cursor'] == {'kind': 'feed_token', 'value': 'v1'}
        assert metadata['last_run']['window_start'] is None
        assert metadata['last_run']['window_end'] is None
        assert metadata['last_run']['records_fetched'] == 1
