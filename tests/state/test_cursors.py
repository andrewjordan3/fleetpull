"""Tests for fleetpull.state.cursors."""

import sqlite3
import threading
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import pytest

from fleetpull.exceptions import ConfigurationError
from fleetpull.incremental import DateWatermark, FeedToken
from fleetpull.state.cursors import (
    CursorStore,
    _deserialize_cursor,
)
from fleetpull.state.database import StateDatabase
from fleetpull.state.migrations import migrate_to_head
from fleetpull.timing.clock import FrozenClock
from fleetpull.timing.codec import to_iso8601
from fleetpull.vocabulary import Provider
from tests.state.conftest import FROZEN_INSTANT

# A whole-second watermark: to_iso8601 drops sub-second precision, so a
# microsecond-bearing instant would not compare equal after a round-trip.
WATERMARK_INSTANT: datetime = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
WATERMARK_ISO: str = '2026-06-01T12:00:00Z'


def _read_updated_at(database_path: Path, provider: Provider, endpoint: str) -> str:
    """Read the raw ``updated_at`` column for one cursor row via a bare connection."""
    connection = sqlite3.connect(database_path)
    try:
        row = connection.execute(
            'SELECT updated_at FROM cursors WHERE provider = ? AND endpoint = ?',
            (provider.value, endpoint),
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    updated_at = row[0]
    assert isinstance(updated_at, str)
    return updated_at


def _insert_raw_cursor(
    database_path: Path, provider: str, endpoint: str, kind: str, value: str
) -> None:
    """Insert a cursor row directly, bypassing the store (for corruption fixtures)."""
    connection = sqlite3.connect(database_path)
    try:
        connection.execute(
            'INSERT INTO cursors (provider, endpoint, kind, value, updated_at) '
            'VALUES (?, ?, ?, ?, ?)',
            (provider, endpoint, kind, value, '2026-06-16T00:00:00Z'),
        )
        connection.commit()
    finally:
        connection.close()


@pytest.fixture
def cursor_store(database_path: Path, frozen_clock: FrozenClock) -> CursorStore:
    """A CursorStore over a freshly initialized, migrated state database."""
    database = StateDatabase(database_path)
    database.initialize()
    migrate_to_head(database)
    return CursorStore(database, frozen_clock)


class TestRoundTrip:
    def test_absent_cursor_reads_none(self, cursor_store: CursorStore) -> None:
        assert cursor_store.get_cursor(Provider.MOTIVE, 'vehicles') is None

    def test_date_watermark_round_trips(self, cursor_store: CursorStore) -> None:
        cursor_store.advance_watermark_forward(
            Provider.MOTIVE, 'vehicles', WATERMARK_INSTANT
        )
        assert cursor_store.get_cursor(Provider.MOTIVE, 'vehicles') == DateWatermark(
            watermark=WATERMARK_INSTANT
        )

    def test_feed_token_round_trips(self, cursor_store: CursorStore) -> None:
        cursor_store.commit_feed_token(
            Provider.GEOTAB, 'log_records', 'toVersion-000123abc'
        )
        assert cursor_store.get_cursor(Provider.GEOTAB, 'log_records') == FeedToken(
            from_version='toVersion-000123abc'
        )


class TestKindGuardBothDirections:
    """A cursor row can never silently change arm (§5's kind-guard doctrine).

    Both writes are kind-guarded in-statement and no unguarded general
    write exists (``set_cursor`` was deleted with the feed arm, 2026-07-21),
    so an arm flip is impossible by construction -- each direction is a
    loud ``ConfigurationError`` with the stored cursor untouched.
    """

    def test_feed_token_never_overwrites_a_watermark(
        self, cursor_store: CursorStore
    ) -> None:
        cursor_store.advance_watermark_forward(
            Provider.GEOTAB, 'trips', WATERMARK_INSTANT
        )
        with pytest.raises(ConfigurationError, match='cross-mode'):
            cursor_store.commit_feed_token(Provider.GEOTAB, 'trips', 'flip-attempt')
        assert cursor_store.get_cursor(Provider.GEOTAB, 'trips') == DateWatermark(
            watermark=WATERMARK_INSTANT
        )

    def test_watermark_never_overwrites_a_feed_token(
        self, cursor_store: CursorStore
    ) -> None:
        cursor_store.commit_feed_token(Provider.GEOTAB, 'log_records', 'v7')
        with pytest.raises(ConfigurationError, match='cross-mode'):
            cursor_store.advance_watermark_forward(
                Provider.GEOTAB, 'log_records', WATERMARK_INSTANT
            )
        assert cursor_store.get_cursor(Provider.GEOTAB, 'log_records') == FeedToken(
            from_version='v7'
        )


class TestCommitFeedToken:
    """The feed arm's kind-guarded last-write-wins commit (§5, 2026-07-21)."""

    def test_inserts_when_no_cursor_exists(self, cursor_store: CursorStore) -> None:
        cursor_store.commit_feed_token(Provider.GEOTAB, 'log_records', 'v1')
        assert cursor_store.get_cursor(Provider.GEOTAB, 'log_records') == FeedToken(
            from_version='v1'
        )

    def test_a_later_commit_overwrites_the_stored_token(
        self, cursor_store: CursorStore
    ) -> None:
        cursor_store.commit_feed_token(Provider.GEOTAB, 'log_records', 'v1')
        cursor_store.commit_feed_token(Provider.GEOTAB, 'log_records', 'v2')
        assert cursor_store.get_cursor(Provider.GEOTAB, 'log_records') == FeedToken(
            from_version='v2'
        )

    def test_recommitting_the_stored_token_is_a_valid_rewrite(
        self, cursor_store: CursorStore
    ) -> None:
        # The at-head empty page re-emits its unchanged toVersion; the
        # re-commit must succeed, not be refused as non-forward.
        cursor_store.commit_feed_token(Provider.GEOTAB, 'log_records', 'v7')
        cursor_store.commit_feed_token(Provider.GEOTAB, 'log_records', 'v7')
        assert cursor_store.get_cursor(Provider.GEOTAB, 'log_records') == FeedToken(
            from_version='v7'
        )

    def test_last_write_wins_is_the_documented_semantic(
        self, cursor_store: CursorStore
    ) -> None:
        # PINNED: a lexically-backward token still overwrites. The store
        # deliberately applies NO monotonicity ordering to the opaque token
        # (the doctrine and the serial-caller reasoning live on
        # commit_feed_token); any future in-store ordering guard must
        # consciously break this test and re-derive the opacity stance.
        cursor_store.commit_feed_token(
            Provider.GEOTAB, 'log_records', 'ffffffffffffffff'
        )
        cursor_store.commit_feed_token(
            Provider.GEOTAB, 'log_records', '0000000000000001'
        )
        assert cursor_store.get_cursor(Provider.GEOTAB, 'log_records') == FeedToken(
            from_version='0000000000000001'
        )


class TestAdvanceWatermarkForwardGuardPlacement:
    """The monotonicity guard must live INSIDE the statement (§5).

    A method-internal read-compare-write is serially indistinguishable
    from the in-statement guard, so this test interleaves at the
    CONNECTION's execute boundary: the stale thread's first execute
    proceeds, then it blocks until the fresh commit lands. The
    in-statement guard performs its whole advance in one execute and
    never reaches the gate; a read-compare-write regression straddles
    it — its stale read happens before the fresh commit and its write
    after — and the final cursor regresses, failing the assertion.
    """

    def test_in_statement_guard_survives_interleaved_read_write(
        self, tmp_path: Path
    ) -> None:
        stale = datetime(2026, 6, 11, 9, 0, 0, tzinfo=UTC)
        fresh = datetime(2026, 6, 14, 12, 0, 0, tzinfo=UTC)
        first_execute_done = threading.Event()
        fresh_committed = threading.Event()

        class _ExecuteGatedDatabase(StateDatabase):
            """Gates the armed thread's SECOND execute behind the fresh commit."""

            armed_thread_id: int | None = None

            @contextmanager
            def connect(self):  # type: ignore[no-untyped-def]  # typing-justified: mirrors the parent contextmanager signature the proxy narrows
                with super().connect() as real_connection:
                    if threading.get_ident() != self.armed_thread_id:
                        yield real_connection
                        return
                    calls = {'count': 0}

                    class _GatedConnection:
                        def execute(
                            self,
                            sql: str,
                            # typing-justified: sqlite3's heterogeneous params
                            parameters: tuple[object, ...] = (),
                        ) -> sqlite3.Cursor:
                            calls['count'] += 1
                            if calls['count'] == 2:
                                assert fresh_committed.wait(timeout=5)
                            result = real_connection.execute(sql, parameters)
                            if calls['count'] == 1:
                                first_execute_done.set()
                            return result

                        # typing-justified: attribute passthrough proxy
                        def __getattr__(self, name: str) -> object:
                            return getattr(real_connection, name)

                    yield _GatedConnection()

        database = _ExecuteGatedDatabase(tmp_path / 'state.sqlite3')
        database.initialize()
        migrate_to_head(database)
        store = CursorStore(database, FrozenClock(start_time_utc=FROZEN_INSTANT))

        def advance_stale() -> None:
            _ExecuteGatedDatabase.armed_thread_id = threading.get_ident()
            store.advance_watermark_forward(Provider.MOTIVE, 'locations', stale)

        stale_thread = threading.Thread(target=advance_stale, name='stale-advance')
        stale_thread.start()
        assert first_execute_done.wait(timeout=5)
        store.advance_watermark_forward(Provider.MOTIVE, 'locations', fresh)
        fresh_committed.set()
        stale_thread.join(timeout=5)
        assert not stale_thread.is_alive()
        assert store.get_cursor(Provider.MOTIVE, 'locations') == DateWatermark(
            watermark=fresh
        )


class TestAdvanceWatermarkForward:
    """The atomic forward-only advance (DESIGN section 5, 2026-07-20)."""

    def test_inserts_when_no_cursor_exists(self, cursor_store: CursorStore) -> None:
        assert (
            cursor_store.advance_watermark_forward(
                Provider.MOTIVE, 'locations', WATERMARK_INSTANT
            )
            is True
        )
        assert cursor_store.get_cursor(Provider.MOTIVE, 'locations') == DateWatermark(
            watermark=WATERMARK_INSTANT
        )

    def test_advances_when_strictly_forward(self, cursor_store: CursorStore) -> None:
        cursor_store.advance_watermark_forward(
            Provider.MOTIVE, 'locations', WATERMARK_INSTANT
        )
        forward = datetime(2026, 6, 2, 12, 0, 0, tzinfo=UTC)
        assert (
            cursor_store.advance_watermark_forward(
                Provider.MOTIVE, 'locations', forward
            )
            is True
        )
        assert cursor_store.get_cursor(Provider.MOTIVE, 'locations') == DateWatermark(
            watermark=forward
        )

    def test_refuses_an_equal_observation(self, cursor_store: CursorStore) -> None:
        cursor_store.advance_watermark_forward(
            Provider.MOTIVE, 'locations', WATERMARK_INSTANT
        )
        assert (
            cursor_store.advance_watermark_forward(
                Provider.MOTIVE, 'locations', WATERMARK_INSTANT
            )
            is False
        )
        assert cursor_store.get_cursor(Provider.MOTIVE, 'locations') == DateWatermark(
            watermark=WATERMARK_INSTANT
        )

    def test_refuses_a_backward_observation(self, cursor_store: CursorStore) -> None:
        cursor_store.advance_watermark_forward(
            Provider.MOTIVE, 'locations', WATERMARK_INSTANT
        )
        backward = datetime(2026, 5, 30, 12, 0, 0, tzinfo=UTC)
        assert (
            cursor_store.advance_watermark_forward(
                Provider.MOTIVE, 'locations', backward
            )
            is False
        )
        assert cursor_store.get_cursor(Provider.MOTIVE, 'locations') == DateWatermark(
            watermark=WATERMARK_INSTANT
        )

    def test_a_stored_feed_token_raises(self, cursor_store: CursorStore) -> None:
        # A cross-mode advance is a wiring bug upstream: refused loudly, and
        # the feed cursor is left untouched.
        cursor_store.commit_feed_token(Provider.GEOTAB, 'log_records', 'v7')
        with pytest.raises(ConfigurationError, match='cross-mode'):
            cursor_store.advance_watermark_forward(
                Provider.GEOTAB, 'log_records', WATERMARK_INSTANT
            )
        assert cursor_store.get_cursor(Provider.GEOTAB, 'log_records') == FeedToken(
            from_version='v7'
        )


class TestKeyIsolation:
    def test_a_set_sibling_does_not_affect_an_unset_key(
        self, cursor_store: CursorStore
    ) -> None:
        cursor_store.advance_watermark_forward(
            Provider.MOTIVE, 'vehicles', WATERMARK_INSTANT
        )
        assert cursor_store.get_cursor(Provider.MOTIVE, 'vehicles') == DateWatermark(
            watermark=WATERMARK_INSTANT
        )
        assert cursor_store.get_cursor(Provider.SAMSARA, 'trips') is None


class TestUpdatedAt:
    def test_updated_at_is_stamped_from_the_clock(
        self, cursor_store: CursorStore, database_path: Path
    ) -> None:
        cursor_store.commit_feed_token(Provider.MOTIVE, 'vehicles', 'v1')
        stored = _read_updated_at(database_path, Provider.MOTIVE, 'vehicles')
        assert stored == to_iso8601(FROZEN_INSTANT)


class TestDurability:
    def test_a_separate_store_reads_the_committed_cursor(
        self, cursor_store: CursorStore, database_path: Path
    ) -> None:
        cursor_store.commit_feed_token(Provider.GEOTAB, 'log_records', 'feed-version-7')

        reopened_store = CursorStore(
            StateDatabase(database_path),
            FrozenClock(start_time_utc=FROZEN_INSTANT),
        )
        assert reopened_store.get_cursor(Provider.GEOTAB, 'log_records') == FeedToken(
            from_version='feed-version-7'
        )


class TestCorruptCursor:
    def test_unparseable_watermark_value_raises(
        self, cursor_store: CursorStore, database_path: Path
    ) -> None:
        # 'not-a-date' passes the CHECK (which constrains kind, not value) but is
        # not ISO-8601, so the read path surfaces it as state-store corruption.
        _insert_raw_cursor(
            database_path,
            'motive',
            'vehicles',
            'date_watermark',
            'not-a-date',
        )
        with pytest.raises(ConfigurationError, match='unparseable watermark'):
            cursor_store.get_cursor(Provider.MOTIVE, 'vehicles')


class TestSerializedForms:
    def test_watermark_is_stored_as_iso_text(
        self, cursor_store: CursorStore, database_path: Path
    ) -> None:
        cursor_store.advance_watermark_forward(
            Provider.MOTIVE, 'vehicles', WATERMARK_INSTANT
        )
        connection = sqlite3.connect(database_path)
        try:
            row = connection.execute(
                'SELECT kind, value FROM cursors WHERE provider = ? AND endpoint = ?',
                ('motive', 'vehicles'),
            ).fetchone()
        finally:
            connection.close()
        assert row == ('date_watermark', WATERMARK_ISO)

    def test_feed_token_is_stored_verbatim(
        self, cursor_store: CursorStore, database_path: Path
    ) -> None:
        cursor_store.commit_feed_token(Provider.GEOTAB, 'trips', 'opaque-token-xyz')
        connection = sqlite3.connect(database_path)
        try:
            row = connection.execute(
                'SELECT kind, value FROM cursors WHERE provider = ? AND endpoint = ?',
                ('geotab', 'trips'),
            ).fetchone()
        finally:
            connection.close()
        assert row == ('feed_token', 'opaque-token-xyz')


class TestDeserializeCursor:
    def test_unknown_kind_raises(self) -> None:
        with pytest.raises(ConfigurationError, match='unrecognized cursor kind'):
            _deserialize_cursor(Provider.MOTIVE, 'vehicles', 'garbage', 'whatever')

    def test_unparseable_watermark_value_raises(self) -> None:
        with pytest.raises(ConfigurationError, match='unparseable watermark'):
            _deserialize_cursor(
                Provider.MOTIVE, 'vehicles', 'date_watermark', 'not-a-date'
            )
