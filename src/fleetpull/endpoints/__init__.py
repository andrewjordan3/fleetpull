"""The endpoints-layer face: the ``EndpointDefinition`` binding and the types it composes."""

from fleetpull.endpoints.base import (
    EndpointDefinition,
    FeedMode,
    RecordExtractor,
    ResumeValue,
    SnapshotMode,
    SpecBuilder,
    StorageKind,
    SyncMode,
    TopLevelListExtractor,
    WatermarkMode,
)

__all__: list[str] = [
    'EndpointDefinition',
    'FeedMode',
    'RecordExtractor',
    'ResumeValue',
    'SnapshotMode',
    'SpecBuilder',
    'StorageKind',
    'SyncMode',
    'TopLevelListExtractor',
    'WatermarkMode',
]
