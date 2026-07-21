# src/fleetpull/orchestrator/metadata_projection.py
"""The ``metadata.json`` projection: a committed run's facts, humanly readable.

``MetadataProjection`` writes the per-endpoint ``metadata.json`` snapshot
after a successful run fully commits (DESIGN §3) -- post-commit and
best-effort, never part of the run's transaction: the outcome's counts, the
run's resolved window, and a cursor read-back from the store flatten into a
``MetadataSnapshot`` the storage face renders and atomically writes. The
program never reads the file back (SQLite stays the single source of truth,
§5). ``sync_mode_label`` also serves the runner's endpoint-start narration,
so renaming a label changes both surfaces together.
"""

import logging
from typing import Protocol

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    FeedMode,
    SnapshotMode,
    SyncMode,
    WatermarkMode,
)
from fleetpull.incremental import (
    DateWatermark,
    DateWindow,
    FeedToken,
    IncrementalCursor,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.orchestrator.outcome import Executed
from fleetpull.paths import PathInput, endpoint_directory
from fleetpull.state import CursorKind
from fleetpull.storage import (
    MetadataSnapshot,
    render_metadata_json,
    write_metadata_json,
)
from fleetpull.timing import Clock, to_iso8601
from fleetpull.vocabulary import Provider

__all__: list[str] = ['CursorReader', 'MetadataProjection', 'sync_mode_label']

logger = logging.getLogger(__name__)


class CursorReader(Protocol):
    """The one-method cursor read-back the projection needs."""

    def get_cursor(self, provider: Provider, endpoint: str) -> IncrementalCursor | None:
        """Return the persisted cursor for a (provider, endpoint), or None."""
        ...


def sync_mode_label(sync_mode: SyncMode) -> str:
    """The sync mode's human-readable label.

    Shared by the ``metadata.json`` projection and the endpoint-start
    narration line -- renaming a label changes both surfaces.

    Args:
        sync_mode: The endpoint's declared sync mode.

    Returns:
        ``'snapshot'``, ``'watermark'``, or ``'feed'``.
    """
    match sync_mode:
        case SnapshotMode():
            return 'snapshot'
        case WatermarkMode():
            return 'watermark'
        case FeedMode():
            return 'feed'


def _serialize_cursor(
    cursor: IncrementalCursor | None,
) -> tuple[str | None, str | None]:
    """Serialize a stored cursor to the metadata projection's plain pair.

    The storage face never sees the cursor union (the §11 storage/state
    boundary), so the projection flattens it here; the kind labels are the
    cursor store's own ``CursorKind`` discriminators.

    Args:
        cursor: The stored cursor, or ``None`` when none is persisted.

    Returns:
        ``(kind, value)`` -- ``('date_watermark', <iso8601>)``,
        ``('feed_token', <token>)``, or ``(None, None)``.
    """
    match cursor:
        case DateWatermark(watermark=watermark):
            return (CursorKind.DATE_WATERMARK.value, to_iso8601(watermark))
        case FeedToken(from_version=from_version):
            return (CursorKind.FEED_TOKEN.value, from_version)
        case None:
            return (None, None)


class MetadataProjection:
    """Projects one committed run's facts into its ``metadata.json``.

    Constructed once beside the runner with the cursor read-back surface,
    the clock, and the dataset root; ``project`` runs after each arm's
    successful commit.
    """

    def __init__(
        self, cursors: CursorReader, clock: Clock, dataset_root: PathInput
    ) -> None:
        """
        Args:
            cursors: The cursor store's read surface, for the post-commit
                cursor read-back.
            clock: Supplies the ``generated_at`` instant.
            dataset_root: Where the endpoint output directories live.
        """
        self._cursors = cursors
        self._clock = clock
        self._dataset_root = dataset_root

    def project(
        self,
        definition: EndpointDefinition[ResponseModel],
        outcome: Executed,
        *,
        window: DateWindow | None,
    ) -> None:
        """Project a committed run's facts into the endpoint's ``metadata.json``.

        Runs only after a successful run has fully committed (parquet, the
        ledger rows, the unit done-marks, the watermark prefix): the
        outcome's counts, the run's resolved window,
        and a cursor read-back from the store flatten into a
        ``MetadataSnapshot`` the storage face renders and atomically writes
        (DESIGN §3).

        Args:
            definition: The endpoint that just ran.
            outcome: The run's merged ``Executed`` outcome.
            window: The run's resolved window, or ``None`` when it had none
                (a snapshot or feed run, or a watermark run that only
                re-drove leftover units).

        Side Effects:
            Writes ``metadata.json`` in the endpoint's output directory; on
            an ``OSError``, logs at ERROR and continues.
        """
        cursor_kind, cursor_value = _serialize_cursor(
            self._cursors.get_cursor(definition.provider, definition.name)
        )
        snapshot = MetadataSnapshot(
            provider=definition.provider.value,
            endpoint=definition.name,
            sync_mode=sync_mode_label(definition.sync_mode),
            generated_at=self._clock.now_utc(),
            records_fetched=outcome.records_fetched,
            rows_written=outcome.write.rows_written,
            duplicates_dropped=outcome.write.duplicates_dropped,
            files_written=outcome.write.files_written,
            deleted_partitions=outcome.write.deleted_partitions,
            window_start=None if window is None else window.start,
            window_end=None if window is None else window.end,
            cursor_kind=cursor_kind,
            cursor_value=cursor_value,
        )
        directory = endpoint_directory(
            self._dataset_root, definition.provider.value, definition.name
        )
        # Only the file write is guarded, and only for OSError: the run is
        # already committed (parquet, ledger, units, watermark), and the file is a
        # cosmetic projection the next successful run rewrites -- failing a
        # committed run over it would be worse than a stale file. A render
        # failure is a bug and propagates. An absent endpoint directory is
        # a healthy no-data state (a seeded-at-head feed, an empty
        # watermark cold run) -- there is nothing to project onto, so the
        # write is skipped quietly rather than alarmed over.
        if not directory.exists():
            logger.debug(
                'metadata.json skipped: provider=%s endpoint=%s '
                '(no data has ever landed)',
                definition.provider.value,
                definition.name,
            )
            return
        text = render_metadata_json(snapshot)
        try:
            write_metadata_json(directory, text)
        except OSError:
            logger.exception(
                'metadata.json write failed: provider=%s endpoint=%s',
                definition.provider.value,
                definition.name,
            )
