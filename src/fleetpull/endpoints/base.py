# src/fleetpull/endpoints/base.py
"""The endpoints-layer binding: the ``EndpointDefinition`` and the types it composes.

An ``EndpointDefinition`` is the single source of truth per endpoint — a frozen,
keyword-only dataclass that composes one implementation per behavioral axis (the
``SpecBuilder``, the ``PaginationStrategy``, the ``RecordExtractor``) plus the
per-endpoint facts the generic machinery reads (DESIGN §11). It is a thin
declarative binding, not a fat base class: the network layer already owns auth,
pagination, classification, and parsing as separate strategies, so the only work
that remains on the endpoint is its spec-builder.

This module ships the binding, the two Protocols it composes (``SpecBuilder`` and
``RecordExtractor``, plain — they are only ever called through the stateless
binding, never verified dynamically), the one generic ``TopLevelListExtractor``,
and the small declaration types beside them: ``StorageKind``, the ``SyncMode``
union (``SnapshotMode`` / ``WatermarkMode`` / ``FeedMode``), and the
``ResumeValue`` alias. The records/state/storage contract (the records overrides
and the event-time column) is deferred — ``base.py`` ships the fetch/binding core.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Protocol

from fleetpull.exceptions import ProviderResponseError
from fleetpull.incremental import DateWindow, FeedToken
from fleetpull.model_contract import ResponseModel
from fleetpull.network.contract import (
    JsonObject,
    JsonValue,
    PaginationStrategy,
    RequestSpec,
)
from fleetpull.vocabulary import Provider, QuotaScope

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


class StorageKind(StrEnum):
    """
    The §3 storage *layout* an endpoint declares: one parquet file vs hive
    partitions. Layout only — *where* the bytes live, not how they merge.

    ``SINGLE`` is one ``data.parquet``; ``DATE_PARTITIONED`` is hive
    ``date=YYYY-MM-DD`` partitions. The caller dispatches on it to pick the storage
    path — read-the-whole-file for ``SINGLE``, touch-only-overlapping-partitions
    for ``DATE_PARTITIONED``. What that read-modify-write *does* to the data —
    full-replace, delete-by-window-then-append, or append-plus-dedup — is the
    ``SyncMode``'s concern, not the layout's; the two are orthogonal axes the
    storage layer combines.

    It lives here on the binding, not in ``vocabulary/``: unlike ``QuotaScope``
    (which config validates against, so it must sit in a leaf config can import),
    ``StorageKind`` has no low-layer consumer — it travels with the
    ``EndpointDefinition`` it configures.
    """

    SINGLE = 'single'
    DATE_PARTITIONED = 'date_partitioned'


@dataclass(frozen=True, slots=True)
class SnapshotMode:
    """
    Snapshot sync declaration (config): a marker carrying no configuration.

    The endpoint re-fetches its full current-state dataset every run and has no
    resume — its spec-builder always receives ``resume=None`` (no window, no
    token). Its write semantic is *full replacement* of the endpoint's current-
    state dataset. A marker member of ``SyncMode``. Snapshot has no event-time
    dimension to partition on, so a snapshot endpoint is laid out ``SINGLE``;
    ``DATE_PARTITIONED`` is not a meaningful pairing (left to discipline, not a
    runtime guard).
    """


@dataclass(frozen=True, slots=True)
class WatermarkMode:
    """
    Watermark sync declaration (config): the late-arrival lookback margin.

    The endpoint's sync *declaration*, distinct from the runtime
    ``IncrementalCursor`` *state* in ``incremental/``: this configures how a fetch
    resumes; the cursor is what it resumes from. Its write semantic is
    *delete-by-window, then append* — the refetched window is cleared and replaced,
    so late arrivals and in-window corrections land cleanly. ``lookback`` is the
    margin ``compute_resume`` subtracts from the stored watermark (§4) so late-
    arriving records inside it are re-fetched. (Whether ``lookback`` is a code
    default or a config override is a later concern; the field shape is the same
    either way.)

    Attributes:
        lookback: How far before the watermark each resume re-fetches.
    """

    lookback: timedelta


@dataclass(frozen=True, slots=True)
class FeedMode:
    """
    Feed sync declaration (config): a marker carrying no configuration.

    The feed arm needs no config — its resume value is the stored ``FeedToken``
    used directly (no lookback, no window). Its write semantic is *append* — feed
    is a forward-only version stream, so new pages are appended; the §6 global
    exact-dedup (on by default) clears the chunk-seam and pagination duplicates
    that append alone would otherwise accumulate. A marker member of ``SyncMode``,
    distinct from the runtime ``FeedToken`` cursor state in ``incremental/``.
    """


# The endpoint's sync-mode declaration (config): the caller matches on it to drive
# both resume and write semantics — SnapshotMode -> no resume + full replace,
# WatermarkMode -> compute_resume + delete-by-window-then-append, FeedMode -> the
# stored token + append. Storage layout (StorageKind) is the orthogonal axis.
type SyncMode = SnapshotMode | WatermarkMode | FeedMode


# The resume value a spec-builder consumes: a DateWindow (watermark, from
# compute_resume), a FeedToken (feed, the stored token), or None — meaning no
# committed cursor, which covers a snapshot every run plus the watermark/feed
# first-fetch bootstrap. Named here with SpecBuilder because it is the
# spec-builder's input contract; compute_resume's own return stays the narrower
# DateWindow | None (watermark arm).
type ResumeValue = DateWindow | FeedToken | None


class RecordExtractor(Protocol):
    """
    Pulls the list of record objects from a response envelope.

    The structural counterpart to how paginators validate their metadata slices:
    it validates the full wire shape and raises ``ProviderResponseError`` on
    anything malformed. Per-record field validation is a separate step the caller
    runs afterward (each returned object into the ``response_model``) — ``extract``
    owns wire shape, the model owns field shape. A plain Protocol (not
    ``@runtime_checkable``): it is only composed into and called through the
    stateless ``EndpointDefinition``, never verified dynamically.
    """

    def extract(self, envelope: JsonValue) -> list[JsonObject]:
        """
        Pull the record objects from a parsed response envelope.

        Args:
            envelope: The parsed response body.

        Returns:
            The list of record objects, each a JSON object.

        Raises:
            ProviderResponseError: The envelope's record-bearing shape is malformed
                (the §8 stance).
        """
        ...


@dataclass(frozen=True, slots=True)
class TopLevelListExtractor:
    """
    The generic extractor: records are a list under one named top-level key.

    Covers Samsara's top-level ``data`` (``TopLevelListExtractor('data')``) and
    Motive's per-endpoint top-level key (``'vehicles'``, ``'users'``, ...).
    Deliberately not a path-walker — one named top-level key only; a nested
    envelope (GeoTab's ``result.data``) gets a named extractor in its provider
    module backed by a slice model, not a generic ``NestedListExtractor(path)``
    (which would be ``items_path`` wearing a Protocol). ``validated_envelope_slice``
    is not used here: it takes a static slice model, and this extractor's ``key``
    is dynamic, so the check is manual.

    Attributes:
        key: The top-level envelope key whose value is the record list.
    """

    key: str

    def extract(self, envelope: JsonValue) -> list[JsonObject]:
        """
        Validate the envelope's wire shape and return the record list.

        Validates, in order: the envelope is a JSON object; ``key`` is present; the
        value at ``key`` is a list; every element is a JSON object. The per-element
        check is what honors the ``list[JsonObject]`` return — a ``cast`` without it
        would be a type lie. It is wire-shape validation of a bounded page, not the
        banned Polars row loop.

        Args:
            envelope: The parsed response body.

        Returns:
            The record list at ``key``, each element a JSON object.

        Raises:
            ProviderResponseError: The envelope is not an object, ``key`` is absent,
                its value is not a list, or an element is not an object — each with
                a message naming the specific failure (the §8 stance).
        """
        if not isinstance(envelope, dict):
            raise ProviderResponseError(
                detail=f'expected a JSON object envelope, got {type(envelope).__name__}'
            )
        if self.key not in envelope:
            raise ProviderResponseError(
                detail=f'envelope is missing the record key {self.key!r}'
            )
        record_list: JsonValue = envelope[self.key]
        if not isinstance(record_list, list):
            raise ProviderResponseError(
                detail=(
                    f'value at {self.key!r} is not a list, got '
                    f'{type(record_list).__name__}'
                )
            )
        records: list[JsonObject] = []
        for index, element in enumerate(record_list):
            if not isinstance(element, dict):
                raise ProviderResponseError(
                    detail=(
                        f'record {index} at {self.key!r} is not a JSON object, got '
                        f'{type(element).__name__}'
                    )
                )
            records.append(element)
        return records


class SpecBuilder(Protocol):
    """
    Builds the first request for an endpoint — the one genuine per-endpoint behavior.

    Builds only the first request (URL, base params, and the resume injection);
    pagination produces every request after it. This is where the canonical
    half-open ``DateWindow`` (§4) is translated to the provider's own request
    convention. A plain Protocol (not ``@runtime_checkable``): it is only composed
    into and called through the stateless ``EndpointDefinition``, never verified
    dynamically.
    """

    def build_spec(
        self, resume: ResumeValue, path_values: Mapping[str, str]
    ) -> RequestSpec:
        """
        Build the endpoint's first request.

        Args:
            resume: The resume value to inject — a ``DateWindow`` (watermark), a
                ``FeedToken`` (feed), or ``None`` (no committed cursor).
            path_values: Substitutions for URL-path placeholders — an empty mapping
                for non-fan-out endpoints, a partition key for URL-path fan-out
                (e.g. a per-vehicle locations endpoint).

        Returns:
            The first ``RequestSpec``; pagination builds every request after it.
        """
        ...


@dataclass(frozen=True, slots=True, kw_only=True)
class EndpointDefinition[ModelT: ResponseModel]:
    """
    The single source of truth per endpoint: a frozen binding, one strategy per axis.

    A frozen, keyword-only dataclass generic over its per-record response model,
    composing one implementation per behavioral axis (``spec_builder``,
    ``pagination``, ``record_extractor`` — Protocols with swappable impls) plus the
    per-endpoint facts. It does no work itself except through its ``spec_builder``.

    Never subclassed per provider. Variation lives in the composed strategies, not
    in subclasses — subclassing would re-braid the per-provider variation the
    network layer already pulled apart into separate strategies, recreating the
    predecessor's tangle. ``if endpoint.name == 'vehicle_locations':`` in the client
    is the failure mode; the fix is always a swapped-in strategy, never a branch or
    a subclass.

    Generic over ``ModelT`` (bound ``ResponseModel``). ``response_model`` is
    ``type[ModelT]``, so an ``EndpointDefinition[Vehicle]`` carries the concrete
    model type forward to a typed ``iter_records`` (§10). ``ModelT`` appears only in
    that output position, so a heterogeneous collection of definitions uses the base
    bound, ``EndpointDefinition[ResponseModel]`` (that collection is a later
    prompt's concern). ``response_model`` is the per-record model (e.g. ``Vehicle``),
    not a full-response wrapper — the ``record_extractor`` and paginator own the
    envelope, so fleet-telemetry-hub's wrapper models are not ported, only the
    per-record ones.

    Keyword-only because nine positional fields is an error waiting to happen. The
    excluded concerns are the records/state/storage contract — the records overrides
    (``schema_overrides`` / ``coercion_overrides``, §9) and the provider-specific
    event-time column the watermark and date-partitioning read (§3/§5) — which
    attach when those layers are built; ``base.py`` ships the fetch/binding core
    only.

    Attributes:
        provider: The provider this endpoint belongs to.
        name: The endpoint's name (e.g. ``'vehicles'``).
        spec_builder: Builds the first request (the one per-endpoint behavior).
        pagination: The per-provider pagination strategy.
        response_model: The per-record response model type.
        record_extractor: Pulls the record list from each response envelope.
        quota_scope: Which token bucket this endpoint spends from.
        storage_kind: The §3 storage layout (single file vs date-partitioned) —
            layout only; merge semantics follow ``sync_mode``.
        sync_mode: The sync-mode declaration (``SnapshotMode`` / ``WatermarkMode``
            / ``FeedMode``) — drives resume and write semantics.
    """

    provider: Provider
    name: str
    spec_builder: SpecBuilder
    pagination: PaginationStrategy
    response_model: type[ModelT]
    record_extractor: RecordExtractor
    quota_scope: QuotaScope
    storage_kind: StorageKind
    sync_mode: SyncMode
