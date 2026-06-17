"""The endpoints-layer face: the ``EndpointDefinition`` binding and the types it composes."""

from fleetpull.endpoints.base import (
    EndpointDefinition,
    FeedMode,
    IncrementalMode,
    RecordExtractor,
    ResumeValue,
    SpecBuilder,
    StorageKind,
    TopLevelListExtractor,
    WatermarkMode,
)

__all__: list[str] = [
    'EndpointDefinition',
    'FeedMode',
    'IncrementalMode',
    'RecordExtractor',
    'ResumeValue',
    'SpecBuilder',
    'StorageKind',
    'TopLevelListExtractor',
    'WatermarkMode',
]
