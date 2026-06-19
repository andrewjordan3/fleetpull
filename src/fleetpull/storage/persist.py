# src/fleetpull/storage/persist.py
"""The storage entry point: persist one run's records for one endpoint.

Composes the two orthogonal axes -- pick the layout from ``StorageKind``, pick the
merge from ``SyncMode``, and run the layout's read-merge-dedup-write over the new
frame. Storage's whole public surface, and deliberately stateless: it reads and
writes parquet and nothing else -- no SQLite, no watermark commit, no
``metadata.json`` (those are the orchestrator's, sequenced after a successful
persist; DESIGN §5). Only ``snapshot`` + ``single`` is wired; the other axis arms
raise explicitly until their consumers arrive.
"""

import polars as pl

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    FeedMode,
    SnapshotMode,
    StorageKind,
    SyncMode,
    WatermarkMode,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.paths import PathInput, endpoint_directory
from fleetpull.storage.layout import Layout, SingleFileLayout
from fleetpull.storage.merge import MergeFn, merge_snapshot
from fleetpull.storage.result import PersistResult

__all__: list[str] = ['persist']


def persist(
    definition: EndpointDefinition[ResponseModel],
    new_frame: pl.DataFrame,
    dataset_root: PathInput,
) -> PersistResult:
    """Persist one run's records for one endpoint.

    Locates the endpoint's directory under ``dataset_root``, selects the layout
    and merge from the definition's declared axes, and writes.

    Args:
        definition: The endpoint binding; supplies the provider / endpoint
            directory names and the storage-kind / sync-mode axes. Only those
            fields are read -- storage needs nothing else from the binding.
        new_frame: This run's validated, flattened records (from the records
            layer).
        dataset_root: The dataset root directory.

    Returns:
        The write report.

    Raises:
        NotImplementedError: If the endpoint declares a sync mode or layout not
            yet built (anything but ``snapshot`` + ``single``).
    """
    target_dir = endpoint_directory(
        dataset_root, definition.provider.value, definition.name
    )
    merge: MergeFn = _select_merge(definition.sync_mode)
    layout: Layout = _select_layout(definition.storage_kind)
    return layout.write_dataset(target_dir, new_frame, merge)


def _select_merge(sync_mode: SyncMode) -> MergeFn:
    """Pick the merge function for a sync mode."""
    match sync_mode:
        case SnapshotMode():
            return merge_snapshot
        case WatermarkMode():
            raise NotImplementedError(
                'watermark merge lands with the vehicle_locations slice (DESIGN §4)'
            )
        case FeedMode():
            raise NotImplementedError(
                'feed merge lands with the GeoTab slice (DESIGN §4)'
            )


def _select_layout(storage_kind: StorageKind) -> Layout:
    """Pick the layout for a storage kind."""
    match storage_kind:
        case StorageKind.SINGLE:
            return SingleFileLayout()
        case StorageKind.DATE_PARTITIONED:
            raise NotImplementedError(
                'date-partitioned layout lands with the vehicle_locations '
                'slice (DESIGN §3)'
            )
