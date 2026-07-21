# src/fleetpull/storage/metadata.py
"""The per-endpoint ``metadata.json`` projection: render and write the snapshot.

``metadata.json`` is a generated human-readable snapshot of one endpoint's
last successful run, written beside its data after the run commits (DESIGN
§3). It is never read by the program — SQLite stays the single source of
truth (§5) — so this module is a pure projection surface: a frozen carrier
of already-committed facts (``MetadataSnapshot``), a deterministic JSON
render, and an atomic file write. The orchestrator supplies every fact as a
plain value (strings, counts, datetimes); no SQLite, no state types, no
cursor union crosses this boundary, which is what keeps the §11
storage/state separation intact.

The write is temp-then-rename atomic (the same same-filesystem rename
doctrine as ``atomic.py``) and deliberately does not create the endpoint
directory: an absent directory means no data ever landed there, an upstream
bug that must surface as the ``OSError`` the caller's posture handles, not
be papered over with a metadata file for a dataset that does not exist.
"""

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from fleetpull.storage.atomic import atomic_write_text
from fleetpull.timing import to_iso8601

__all__: list[str] = [
    'MetadataSnapshot',
    'render_metadata_json',
    'write_metadata_json',
]

# The per-endpoint snapshot file name (DESIGN §3).
_METADATA_FILE_NAME: str = 'metadata.json'


@dataclass(frozen=True, slots=True)
class MetadataSnapshot:
    """One endpoint's last-successful-run facts, ready to render.

    Every field is a plain value the orchestrator has already committed
    elsewhere (ledger, cursor store, write report) — this carrier holds the
    projection, never the state types themselves.

    Attributes:
        provider: The provider directory name (e.g. ``'motive'``).
        endpoint: The endpoint name (e.g. ``'vehicles'``).
        sync_mode: The endpoint's sync-mode label — ``'snapshot'``,
            ``'watermark'``, or ``'feed'``.
        generated_at: The projection instant, timezone-aware UTC.
        records_fetched: Records fetched across the run.
        rows_written: Rows written to disk this run.
        duplicates_dropped: Exact-duplicate rows removed at write time.
        files_written: Parquet files written this run.
        deleted_partitions: The date partitions pruned this run.
        window_start: The run's resolved window start, or ``None`` when the
            run had no window (a snapshot run).
        window_end: The run's resolved window end, or ``None`` when the run
            had no window.
        cursor_kind: The stored cursor's kind (``'date_watermark'`` or
            ``'feed_token'``), or ``None`` when no cursor is stored.
        cursor_value: The stored cursor's serialized value, or ``None`` when
            no cursor is stored.
    """

    provider: str
    endpoint: str
    sync_mode: str
    generated_at: datetime
    records_fetched: int
    rows_written: int
    duplicates_dropped: int
    files_written: int
    deleted_partitions: tuple[date, ...]
    window_start: datetime | None
    window_end: datetime | None
    cursor_kind: str | None
    cursor_value: str | None


def _optional_iso8601(moment: datetime | None) -> str | None:
    """Render an optional UTC datetime as ISO-8601, passing ``None`` through.

    Args:
        moment: A timezone-aware UTC datetime, or ``None``.

    Returns:
        The ISO-8601 ``Z`` string, or ``None``.

    Raises:
        ValueError: ``moment`` is naive or non-UTC (from ``to_iso8601``).
    """
    return None if moment is None else to_iso8601(moment)


def render_metadata_json(snapshot: MetadataSnapshot) -> str:
    """Render a snapshot as the ``metadata.json`` document text.

    A pure, deterministic render: datetimes via the timing codec's ISO-8601
    form, dates via ``isoformat``, two-space indentation, one trailing
    newline. ``schema_version`` names the document shape so a human diffing
    files across package versions can tell a shape change from a data change.

    Args:
        snapshot: The committed run facts to render.

    Returns:
        The complete JSON document text, trailing newline included.

    Raises:
        ValueError: A snapshot datetime is naive or non-UTC (from the codec).
    """
    cursor = (
        None
        if snapshot.cursor_kind is None
        else {'kind': snapshot.cursor_kind, 'value': snapshot.cursor_value}
    )
    document = {
        'schema_version': 1,
        'provider': snapshot.provider,
        'endpoint': snapshot.endpoint,
        'sync_mode': snapshot.sync_mode,
        'generated_at': to_iso8601(snapshot.generated_at),
        'last_run': {
            'records_fetched': snapshot.records_fetched,
            'rows_written': snapshot.rows_written,
            'duplicates_dropped': snapshot.duplicates_dropped,
            'files_written': snapshot.files_written,
            'deleted_partitions': [
                deleted_date.isoformat() for deleted_date in snapshot.deleted_partitions
            ],
            'window_start': _optional_iso8601(snapshot.window_start),
            'window_end': _optional_iso8601(snapshot.window_end),
        },
        'cursor': cursor,
    }
    return json.dumps(document, indent=2) + '\n'


def write_metadata_json(endpoint_directory: Path, text: str) -> None:
    """Atomically write ``text`` as the endpoint directory's ``metadata.json``.

    ``atomic_write_text``'s temp-then-rename, aimed at the projection's fixed
    file name. The endpoint directory is deliberately not created: an absent
    directory means no data ever landed — an upstream bug surfaced here as
    ``OSError``, not silenced by a ``mkdir``.

    Args:
        endpoint_directory: The endpoint's existing output directory.
        text: The complete document text (from ``render_metadata_json``).

    Raises:
        OSError: The write or rename failed — including a missing endpoint
            directory (the temp is cleaned up first).

    Side Effects:
        Writes and renames files inside ``endpoint_directory``.
    """
    atomic_write_text(text, endpoint_directory / _METADATA_FILE_NAME)
