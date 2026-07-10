# src/fleetpull/endpoints/shared/__init__.py
"""Shared endpoints-layer surface: the ``EndpointDefinition`` binding, the
``SpecBuilder`` protocol and its provider-agnostic implementations, the
URL-fan-out path renderer, and the sync-mode / storage / resume declaration
types."""

from fleetpull.endpoints.shared.base import (
    CompletenessCheck,
    EndpointDefinition,
    FeedMode,
    ResumeValue,
    SnapshotMode,
    SpecBuilder,
    StorageKind,
    SyncMode,
    WatermarkMode,
)
from fleetpull.endpoints.shared.fan_out import FanOutBinding
from fleetpull.endpoints.shared.spec_builders import StaticGetSpecBuilder
from fleetpull.endpoints.shared.url_paths import (
    UrlPathTemplateError,
    render_url_path_template,
)

__all__: list[str] = [
    'CompletenessCheck',
    'EndpointDefinition',
    'FanOutBinding',
    'FeedMode',
    'ResumeValue',
    'SnapshotMode',
    'SpecBuilder',
    'StaticGetSpecBuilder',
    'StorageKind',
    'SyncMode',
    'UrlPathTemplateError',
    'WatermarkMode',
    'render_url_path_template',
]
