# src/fleetpull/endpoints/shared/__init__.py
"""Shared endpoints-layer surface: the ``EndpointDefinition`` binding, the
``SpecBuilder`` protocol and its provider-agnostic implementations, the
``RequestShape`` union (request cardinality), the URL-path renderer for
fan-out paths, the resume-value type guard, and the sync-mode / storage /
resume declaration types."""

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
from fleetpull.endpoints.shared.request_shape import (
    BatchedRosterFanOut,
    BisectedWindowFetch,
    ParamSweep,
    RequestShape,
    RosterFanOut,
    SingleFetch,
)
from fleetpull.endpoints.shared.resume import require_date_window, require_feed_resume
from fleetpull.endpoints.shared.spec_builders import StaticGetSpecBuilder
from fleetpull.endpoints.shared.url_paths import (
    UrlPathTemplateError,
    render_url_path_template,
)

__all__: list[str] = [
    'BatchedRosterFanOut',
    'BisectedWindowFetch',
    'CompletenessCheck',
    'EndpointDefinition',
    'FeedMode',
    'ParamSweep',
    'RequestShape',
    'ResumeValue',
    'RosterFanOut',
    'SingleFetch',
    'SnapshotMode',
    'SpecBuilder',
    'StaticGetSpecBuilder',
    'StorageKind',
    'SyncMode',
    'UrlPathTemplateError',
    'WatermarkMode',
    'render_url_path_template',
    'require_date_window',
    'require_feed_resume',
]
