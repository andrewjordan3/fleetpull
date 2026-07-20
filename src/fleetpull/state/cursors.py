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
absence; that decision lives in the caller. ``set_cursor`` is an unconditional
single-row upsert; the only-persist-a-strictly-forward-watermark advance discipline
likewise lives in the caller (the orchestrator), not here.
"""

import logging
from datetime import datetime
from enum import StrEnum
from typing import Final

from fleetpull.exceptions import ConfigurationError
from fleetpull.incremental import DateWatermark, FeedToken, IncrementalCursor
from fleetpull.state.database import SqliteScalar, StateDatabase
from fleetpull.timing import Clock, from_iso8601, to_iso8601
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


def _serialize_cursor(cursor: IncrementalCursor) -> tuple[CursorKind, str]:
    """
    Serialize an incremental cursor to its ``(kind, value)`` row form.

    The write-side half of the codec. A ``DateWatermark`` renders its
    ``watermark`` to seconds-precision ISO-8601 UTC text via the timing codec; a
    ``FeedToken`` stores its opaque token verbatim (fleetpull never parses it).

    Args:
        cursor: The tagged-union cursor to serialize.

    Returns:
        The ``(kind, value)`` pair: the discriminator naming the arm and the
        arm's serialized ``value`` column text.

    Raises:
        ValueError: A ``DateWatermark`` whose ``watermark`` is naive or not UTC —
            surfaced from the timing codec, kept stdlib as a caller bug.
    """
    match cursor:
        case DateWatermark():
            return CursorKind.DATE_WATERMARK, to_iso8601(cursor.watermark)
        case FeedToken():
            return CursorKind.FEED_TOKEN, cursor.from_version


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
            try:
                watermark: datetime = from_iso8601(value_text)
            except ValueError as error:
                raise ConfigurationError(
                    'state database holds an unparseable watermark cursor',
                    provider=provider.value,
                    endpoint=endpoint,
                    detail=f'cursor value {value_text!r} is not ISO-8601 UTC',
                ) from error
            return DateWatermark(watermark=watermark)
        case CursorKind.FEED_TOKEN:
            return FeedToken(from_version=value_text)


_SELECT_CURSOR_SQL: Final[str] = (
    'SELECT kind, value FROM cursors WHERE provider = ? AND endpoint = ?'
)

_UPSERT_CURSOR_SQL: Final[str] = """
INSERT INTO cursors (provider, endpoint, kind, value, updated_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (provider, endpoint) DO UPDATE SET
    kind = excluded.kind,
    value = excluded.value,
    updated_at = excluded.updated_at
"""

# The atomic forward-only advance (DESIGN section 5, 2026-07-20): the
# monotonicity guard lives INSIDE the statement, so concurrent unit
# completions racing their prefix commits can never interleave a stale
# read into a backward write. Lexical > on ``to_iso8601``'s fixed-width
# Z-form is chronological. The kind guard keeps a feed cursor untouched;
# the caller distinguishes that case loudly (see advance_watermark_forward).
_ADVANCE_WATERMARK_SQL: Final[str] = """
INSERT INTO cursors (provider, endpoint, kind, value, updated_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (provider, endpoint) DO UPDATE SET
    value = excluded.value,
    updated_at = excluded.updated_at
WHERE cursors.kind = excluded.kind AND excluded.value > cursors.value
"""


class CursorStore:
    """
    Persists and reads per-(provider, endpoint) incremental cursors.

    The translator between the ``IncrementalCursor`` union (§4) and
    ``cursors``-table rows: it owns the serialization the cursor leaf and the
    migration runner deliberately don't. Runs after ``migrate_to_head`` (the
    table must exist). ``get_cursor`` reconstructs the tagged-union arm from the
    row's ``kind`` discriminator; ``set_cursor`` is an unconditional single-row
    upsert stamped with the injected ``Clock``.

    The store is deliberately dumb: it never fabricates a cursor and never
    interprets absence. ``set_cursor`` applies no advance/monotonicity
    discipline — that rule lives in the caller (§5) — with one deliberate
    exception: ``advance_watermark_forward`` carries the strictly-forward
    guard inside its statement, because the prefix-advance rule's concurrent
    callers cannot enforce monotonicity race-free from outside (§5,
    2026-07-20).

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
        if not isinstance(kind_text, str):
            raise RuntimeError(f'cursors.kind was not text: {kind_text!r}')
        if not isinstance(value_text, str):
            raise RuntimeError(f'cursors.value was not text: {value_text!r}')
        return _deserialize_cursor(provider, endpoint, kind_text, value_text)

    def set_cursor(
        self, provider: Provider, endpoint: str, cursor: IncrementalCursor
    ) -> None:
        """
        Upsert the cursor for one (provider, endpoint).

        An unconditional single-row upsert: the existing row for the key, if any,
        is overwritten with this cursor's serialized ``kind``/``value`` and a fresh
        ``updated_at`` from the injected clock. No advance or monotonicity check
        happens here — the caller decides whether a write is warranted (§5).

        Args:
            provider: The provider whose cursor to persist.
            endpoint: The endpoint whose cursor to persist.
            cursor: The tagged-union cursor to store.

        Raises:
            ValueError: A ``DateWatermark`` whose ``watermark`` is naive or not
                UTC — surfaced from the timing codec during serialization.

        Side Effects:
            Opens a connection, upserts one row, and commits.
        """
        kind, value = _serialize_cursor(cursor)
        updated_at: str = to_iso8601(self._clock.now_utc())
        with self._database.connect() as connection:
            # connect() yields a DEFAULT-isolation connection: the INSERT opens an
            # implicit transaction and commit() ends it. Do NOT add an explicit
            # BEGIN here — under default isolation it raises "cannot start a
            # transaction within a transaction" (the recorded migration-runner
            # finding). Single statement, then commit.
            connection.execute(
                _UPSERT_CURSOR_SQL,
                (provider.value, endpoint, kind.value, value, updated_at),
            )
            connection.commit()
        logger.debug(
            'persisted cursor: provider=%s endpoint=%s kind=%s',
            provider.value,
            endpoint,
            kind.value,
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
        with self._database.connect() as connection:
            changes_before: int = connection.total_changes
            connection.execute(
                _ADVANCE_WATERMARK_SQL,
                (
                    provider.value,
                    endpoint,
                    CursorKind.DATE_WATERMARK.value,
                    value,
                    updated_at,
                ),
            )
            advanced: bool = connection.total_changes > changes_before
            if not advanced:
                # Not-forward is normal; a kind mismatch is a bug. One
                # diagnostic read distinguishes them, outside the write's
                # atomicity (the write already refused, nothing raced).
                row: tuple[SqliteScalar, SqliteScalar] | None = connection.execute(
                    _SELECT_CURSOR_SQL, (provider.value, endpoint)
                ).fetchone()
                if row is not None and row[0] != CursorKind.DATE_WATERMARK.value:
                    raise ConfigurationError(
                        'cross-mode watermark advance refused',
                        provider=provider.value,
                        endpoint=endpoint,
                        detail=f'stored cursor kind is {row[0]!r}, not a date watermark',
                    )
            connection.commit()
        if advanced:
            logger.debug(
                'watermark advanced: provider=%s endpoint=%s watermark=%s',
                provider.value,
                endpoint,
                value,
            )
        return advanced
