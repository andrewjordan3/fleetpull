# src/fleetpull/state/cursors.py
"""The cursor persistence layer: translator between cursors and ``cursors``-table rows.

Owns the serialization the pure cursor leaf (``incremental/``) and the migration
runner (``state/migrations.py``) deliberately don't (DESIGN §4/§5). A
``DateWatermark`` serializes its ``watermark`` to ISO-8601 UTC text via the timing
codec; a ``FeedToken`` stores its opaque token verbatim (fleetpull never parses
it). The ``kind`` column discriminates the union arm on read; ``updated_at`` is
written from the injected ``Clock``. Runs after ``migrate_to_head`` — the
``cursors`` table must already exist.

A row read with an unrecognized ``kind``, or a ``date_watermark`` ``value`` that is
not parseable ISO-8601 UTC, is state-store corruption and raises
``ConfigurationError``, consistent with the other §5 corruption stances.
``get_cursor`` returning ``None`` means exactly "no cursor has been persisted for
this (provider, endpoint)" — the store never fabricates one and never interprets
absence; that decision lives in the caller. Two writes, one arm each, both
kind-guarded inside their statements (so a cursor row can never silently
change arm — the §5 kind-guard doctrine, total since the feed arm landed
2026-07-21 and the earlier unguarded general upsert was deleted with it):
``advance_watermark_forward`` is the watermark arm's write, additionally
carrying the strictly-forward monotonicity guard in-statement (the recorded
§5 exception to the dumb-store stance, 2026-07-20), because its concurrent
prefix-committing callers cannot enforce monotonicity race-free from outside
the statement; ``commit_feed_token`` is the feed arm's write —
kind-guarded last-write-wins, with monotonicity deliberately left to the
caller's serial per-page sequencing (the reasoning on the method).
"""

import logging
import sqlite3
from datetime import datetime
from enum import StrEnum
from typing import Final

from fleetpull.exceptions import ConfigurationError
from fleetpull.incremental import DateWatermark, FeedToken, IncrementalCursor
from fleetpull.state.database import (
    SqliteScalar,
    StateDatabase,
    expect_text,
    parse_stored_instant,
)
from fleetpull.timing import Clock, to_iso8601
from fleetpull.vocabulary import Provider

__all__: list[str] = ['CursorKind', 'CursorStore']

logger = logging.getLogger(__name__)


class CursorKind(StrEnum):
    """
    The ``cursors.kind`` discriminator: which arm of the union a row holds.

    The read path dispatches over this discriminator to reconstruct the arm, and
    ``CursorKind(kind_text)`` gives a free corrupt-discriminator guard. The string
    values equal the migration's CHECK literals exactly; the two are held in two
    places by deliberate boundary discipline (the migration runner owns its DDL,
    the store owns serialization, and neither imports the other), pinned by the
    round-trip tests that run against a real migrated table with the CHECK active.
    """

    DATE_WATERMARK = 'date_watermark'
    FEED_TOKEN = 'feed_token'


def _deserialize_cursor(
    provider: Provider, endpoint: str, kind_text: str, value_text: str
) -> IncrementalCursor:
    """
    Reconstruct an incremental cursor from its stored ``(kind, value)`` row.

    The raising read-side half of the codec: ``kind_text`` discriminates the arm
    and ``value_text`` is deserialized into it. A ``date_watermark`` value is
    parsed back from ISO-8601 UTC via the timing codec; a ``feed_token`` value is
    the opaque token, returned verbatim. A ``kind_text`` outside the two known
    discriminators, or a ``date_watermark`` ``value_text`` that is not parseable
    ISO-8601 UTC, is state-store corruption (the §5 stance) and raises
    ``ConfigurationError``.

    Args:
        provider: The provider whose cursor row this is; identifies the corrupt
            row in a raised error, not otherwise consulted.
        endpoint: The endpoint whose cursor row this is; identifies the corrupt
            row in a raised error, not otherwise consulted.
        kind_text: The row's ``kind`` discriminator column.
        value_text: The row's ``value`` column.

    Returns:
        The reconstructed ``DateWatermark`` or ``FeedToken``.

    Raises:
        ConfigurationError: ``kind_text`` is not a known cursor kind, or a
            ``date_watermark`` ``value_text`` is not parseable ISO-8601 UTC —
            either is state-store corruption.
    """
    try:
        kind: CursorKind = CursorKind(kind_text)
    except ValueError as error:
        raise ConfigurationError(
            'state database holds an unrecognized cursor kind',
            provider=provider.value,
            endpoint=endpoint,
            detail=(
                f'cursor kind {kind_text!r} is not one of '
                f'{[member.value for member in CursorKind]}'
            ),
        ) from error
    match kind:
        case CursorKind.DATE_WATERMARK:
            watermark: datetime = parse_stored_instant(
                value_text,
                provider=provider,
                endpoint=endpoint,
                column='watermark cursor value',
            )
            return DateWatermark(watermark=watermark)
        case CursorKind.FEED_TOKEN:
            return FeedToken(from_version=value_text)


_SELECT_CURSOR_SQL: Final[str] = (
    'SELECT kind, value FROM cursors WHERE provider = ? AND endpoint = ?'
)

# The feed arm's kind-guarded last-write-wins commit (DESIGN section 5,
# 2026-07-21) — and the shared upsert skeleton both writes are stated on:
# the in-statement kind guard keeps a stored watermark
# untouched (the caller distinguishes that refusal loudly — see
# commit_feed_token); the value is otherwise overwritten unconditionally.
# Deliberately NO monotonicity guard, unlike the watermark's: the token is
# opaque by doctrine (section 8's probe-settled decision 4 — the version
# order is the provider's, never fleetpull's to compare), a lexical guard
# would bet on the observed-but-uncontracted 16-hex encoding, and the feed
# drive is the only writer and strictly serial (one page after another under
# the single-driver-per-state-database assumption), so there is no
# interleaving for an in-statement guard to defend against — the situation
# that justified the watermark exception does not exist here.
_COMMIT_FEED_TOKEN_SQL: Final[str] = """
INSERT INTO cursors (provider, endpoint, kind, value, updated_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (provider, endpoint) DO UPDATE SET
    value = excluded.value,
    updated_at = excluded.updated_at
WHERE cursors.kind = excluded.kind
"""

# The atomic forward-only advance (DESIGN section 5, 2026-07-20): the feed
# write's kind-guarded upsert plus the monotonicity conjunct, derived so the
# skeleton is stated once and only the conjunct distinguishes the arms. The
# guard lives INSIDE the statement, so concurrent unit
# completions racing their prefix commits can never interleave a stale
# read into a backward write. Lexical > on ``to_iso8601``'s fixed-width
# Z-form is chronological. The kind guard keeps a feed cursor untouched;
# the caller distinguishes that case loudly (see advance_watermark_forward).
_ADVANCE_WATERMARK_SQL: Final[str] = (
    _COMMIT_FEED_TOKEN_SQL.rstrip('\n') + ' AND excluded.value > cursors.value\n'
)


def _stored_kind(
    connection: sqlite3.Connection, provider: Provider, endpoint: str
) -> SqliteScalar:
    """Read the stored cursor row's ``kind`` for a refused write's diagnostic.

    Runs on the refusing write's own connection, inside its still-open
    transaction — the guarded upsert already refused and changed nothing,
    so the read sees exactly the row the guard compared against.

    Args:
        connection: The refusing write's open connection.
        provider: The provider whose row to read.
        endpoint: The endpoint whose row to read.

    Returns:
        The stored ``kind`` scalar, or ``None`` when no row exists.
    """
    row: tuple[SqliteScalar, SqliteScalar] | None = connection.execute(
        _SELECT_CURSOR_SQL, (provider.value, endpoint)
    ).fetchone()
    return None if row is None else row[0]


def _guarded_upsert(
    connection: sqlite3.Connection, sql: str, params: tuple[str, ...]
) -> bool:
    """Execute one guarded cursor upsert and report whether it wrote.

    The Python half of the shared upsert skeleton: the guard semantics stay
    entirely inside ``sql`` (the §5 guard-placement doctrine); this helper
    only executes and detects whether the guarded statement changed a row.

    Args:
        connection: The write's open connection.
        sql: The guarded upsert statement.
        params: The statement's positional bindings.

    Returns:
        ``True`` when a row was inserted or updated; ``False`` when the
        in-statement guard refused the write.
    """
    changes_before: int = connection.total_changes
    connection.execute(sql, params)
    return connection.total_changes > changes_before


class CursorStore:
    """
    Persists and reads per-(provider, endpoint) incremental cursors.

    The translator between the ``IncrementalCursor`` union (§4) and
    ``cursors``-table rows: it owns the serialization the cursor leaf and the
    migration runner deliberately don't. Runs after ``migrate_to_head`` (the
    table must exist). ``get_cursor`` reconstructs the tagged-union arm from
    the row's ``kind`` discriminator; the two writes — one per arm, both
    stamped with the injected ``Clock`` — each carry the kind guard inside
    their statement, so a cursor row can never silently change arm (§5's
    kind-guard doctrine): ``advance_watermark_forward`` is the watermark
    arm's strictly-forward advance, and ``commit_feed_token`` is the feed
    arm's last-write-wins commit. No unguarded general write exists (the
    earlier ``set_cursor`` upsert, scaffolding for the then-unbuilt feed arm,
    was deleted when the guarded feed commit landed, 2026-07-21).

    The store stays deliberately dumb otherwise: it never fabricates a
    cursor and never interprets absence; resume-on-absence policy lives in
    the orchestrator (§5).

    Args:
        database: The initialized, migrated state database supplying connections.
        clock: The clock stamping ``updated_at`` on every write.
    """

    def __init__(self, database: StateDatabase, clock: Clock) -> None:
        self._database: StateDatabase = database
        self._clock: Clock = clock

    def get_cursor(self, provider: Provider, endpoint: str) -> IncrementalCursor | None:
        """
        Read the persisted cursor for one (provider, endpoint), if any.

        Args:
            provider: The provider whose cursor to read.
            endpoint: The endpoint whose cursor to read.

        Returns:
            The reconstructed ``DateWatermark`` or ``FeedToken``, or ``None`` when
            no cursor has been persisted for this (provider, endpoint). ``None``
            means exactly that absence — the store neither fabricates a cursor nor
            interprets the gap; the resume-on-absence decision lives above it (§5).

        Raises:
            ConfigurationError: The stored row is corrupt — an unrecognized
                ``kind`` or an unparseable ``date_watermark`` value.
            RuntimeError: A ``kind`` or ``value`` column came back non-text,
                violating the STRICT ``TEXT NOT NULL`` schema contract.

        Side Effects:
            Opens a connection and reads one row.
        """
        with self._database.connect() as connection:
            row: tuple[SqliteScalar, SqliteScalar] | None = connection.execute(
                _SELECT_CURSOR_SQL, (provider.value, endpoint)
            ).fetchone()
        if row is None:
            return None
        kind_text, value_text = row
        # The columns are TEXT NOT NULL under STRICT, so non-text is a SQLite
        # contract violation, surfaced loudly (the database.py narrowing pattern).
        return _deserialize_cursor(
            provider,
            endpoint,
            expect_text(kind_text, 'cursors.kind'),
            expect_text(value_text, 'cursors.value'),
        )

    def commit_feed_token(
        self, provider: Provider, endpoint: str, to_version: str
    ) -> None:
        """
        Commit the feed cursor to ``to_version`` — kind-guarded last-write-wins.

        The feed arm's only write (DESIGN §5, 2026-07-21). The kind guard
        lives inside the statement, mirroring the watermark advance: a feed
        token never overwrites a stored watermark (a refused write here is
        always a cross-mode wiring bug, surfaced loudly). There is
        deliberately NO monotonicity guard, unlike the watermark's two
        reasons deep: the token is opaque by doctrine (§8's probe-settled
        decision 4 — a lexical comparison would bet on the observed 16-hex
        encoding GeoTab never contracted), and the feed drive is the only
        writer and strictly serial (per-page commits of a version-ordered
        stream under the single-driver assumption), so no interleaving
        exists for a guard to defend against. Forward motion is therefore
        the protocol's and the caller's property, not the store's;
        last-write-wins is the documented semantic.

        Args:
            provider: The provider whose feed cursor to commit.
            endpoint: The endpoint whose feed cursor to commit.
            to_version: The page's ``toVersion`` — the opaque resume token,
                stored verbatim (fleetpull never parses it). Re-committing
                the stored value (the at-head empty page) is a valid no-op
                rewrite.

        Raises:
            ConfigurationError: The stored cursor is a date watermark — a
                cross-mode write is a wiring bug upstream, surfaced loudly
                rather than silently skipped.

        Side Effects:
            Opens a connection; inserts or overwrites at most one row; commits.
        """
        updated_at: str = to_iso8601(self._clock.now_utc())
        with self._database.transaction() as connection:
            committed: bool = _guarded_upsert(
                connection,
                _COMMIT_FEED_TOKEN_SQL,
                (
                    provider.value,
                    endpoint,
                    CursorKind.FEED_TOKEN.value,
                    to_version,
                    updated_at,
                ),
            )
            if not committed:
                # Last-write-wins can only be refused by the kind guard, so a
                # refusal is always the cross-mode bug; the diagnostic read
                # names the stored kind.
                stored_kind = _stored_kind(connection, provider, endpoint)
                raise ConfigurationError(
                    'cross-mode feed-token commit refused',
                    provider=provider.value,
                    endpoint=endpoint,
                    detail=f'stored cursor kind is {stored_kind!r}, not a feed token',
                )
        logger.debug(
            'feed token committed: provider=%s endpoint=%s to_version=%s',
            provider.value,
            endpoint,
            to_version,
        )

    def advance_watermark_forward(
        self, provider: Provider, endpoint: str, observed: datetime
    ) -> bool:
        """
        Advance the date watermark to ``observed`` iff that is strictly forward.

        The one write with the monotonicity guard INSIDE the statement (the
        deliberate exception to this store's no-discipline stance, added with
        the prefix-advance rule -- DESIGN §5, 2026-07-20): concurrent unit
        completions race their prefix commits, and a read-compare-write in the
        caller could interleave a stale read into a backward write. The
        guarded upsert inserts when no cursor exists, advances when
        ``observed`` is strictly beyond the stored watermark, and changes
        nothing otherwise -- atomically, whatever the caller interleaving.

        Args:
            provider: The provider whose watermark to advance.
            endpoint: The endpoint whose watermark to advance.
            observed: The candidate watermark -- a folded in-window maximum
                event time.

        Returns:
            ``True`` when the cursor row was inserted or advanced; ``False``
            when ``observed`` was not strictly forward of the stored value.

        Raises:
            ValueError: ``observed`` is naive or not UTC (surfaced from the
                timing codec).
            ConfigurationError: The stored cursor is a feed token -- a
                cross-mode write is a wiring bug upstream, surfaced loudly
                rather than silently skipped.

        Side Effects:
            Opens a connection; inserts or updates at most one row; commits.
        """
        value: str = to_iso8601(observed)
        updated_at: str = to_iso8601(self._clock.now_utc())
        with self._database.transaction() as connection:
            advanced: bool = _guarded_upsert(
                connection,
                _ADVANCE_WATERMARK_SQL,
                (
                    provider.value,
                    endpoint,
                    CursorKind.DATE_WATERMARK.value,
                    value,
                    updated_at,
                ),
            )
            if not advanced:
                # Not-forward is normal; a kind mismatch is a bug. One
                # diagnostic read distinguishes them.
                stored_kind = _stored_kind(connection, provider, endpoint)
                if (
                    stored_kind is not None
                    and stored_kind != CursorKind.DATE_WATERMARK.value
                ):
                    raise ConfigurationError(
                        'cross-mode watermark advance refused',
                        provider=provider.value,
                        endpoint=endpoint,
                        detail=(
                            f'stored cursor kind is {stored_kind!r}, not a '
                            f'date watermark'
                        ),
                    )
        if advanced:
            logger.debug(
                'watermark advanced: provider=%s endpoint=%s watermark=%s',
                provider.value,
                endpoint,
                value,
            )
        return advanced
