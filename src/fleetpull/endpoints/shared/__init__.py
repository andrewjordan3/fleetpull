# src/fleetpull/endpoints/shared/__init__.py
"""Shared endpoints-layer surface: the ``EndpointDefinition`` binding, the
``SpecBuilder`` protocol and its provider-agnostic implementations, and the
sync-mode / storage / resume declaration types."""

from fleetpull.endpoints.shared.base import (
    EndpointDefinition,
    FeedMode,
    ResumeValue,
    SnapshotMode,
    SpecBuilder,
    StorageKind,
    SyncMode,
    WatermarkMode,
)
from fleetpull.endpoints.shared.spec_builders import StaticGetSpecBuilder

__all__: list[str] = [
    'EndpointDefinition',
    'FeedMode',
    'ResumeValue',
    'SnapshotMode',
    'SpecBuilder',
    'StaticGetSpecBuilder',
    'StorageKind',
    'SyncMode',
    'WatermarkMode',
]
