# src/fleetpull/endpoints/shared/base.py
"""The endpoints-layer binding: the ``EndpointDefinition`` and the types it composes.

An ``EndpointDefinition`` is the single source of truth per endpoint — a frozen,
keyword-only dataclass that composes one implementation per behavioral axis (the
``SpecBuilder`` and the ``PageDecoder``) plus the per-endpoint facts the generic
machinery reads (DESIGN §11). It is a thin declarative binding, not a fat base
class: the network layer already owns auth, pagination, classification, and
parsing as separate strategies, so the only work that remains on the endpoint is
its spec-builder.

This module ships the binding, the one Protocol it defines (``SpecBuilder``; the
``PageDecoder`` it composes is imported from the contract), and the small
declaration types beside it: ``StorageKind``, the ``SyncMode`` union
(``SnapshotMode`` / ``WatermarkMode`` / ``FeedMode``), and the ``ResumeValue``
alias. The ``event_time_column`` the watermark and date-partitioning read
(§3/§5) now ships here on the binding, validated at construction; the records
``schema_overrides`` hatch (§9) is the one contract piece still deferred.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol, get_args

from fleetpull.endpoints.shared.bisection import WindowBisection
from fleetpull.endpoints.shared.fan_out import FanOutBinding
from fleetpull.incremental import DateWindow, FeedToken
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import TransportClient
from fleetpull.network.contract import PageDecoder, RequestSpec
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = [
    'CompletenessCheck',
    'EndpointDefinition',
    'FeedMode',
    'ResumeValue',
    'SnapshotMode',
    'SpecBuilder',
    'StorageKind',
    'SyncMode',
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
    dimension to partition on, so a snapshot endpoint must be laid out
    ``SINGLE`` — ``EndpointDefinition`` enforces that pairing at construction,
    since ``DATE_PARTITIONED`` would have no event-time column to split on.
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
    margin the resume resolver subtracts from the stored watermark (§4) so late-
    arriving records inside it are re-fetched; the resolver then floors the
    start to its UTC midnight, so a lookback of N days re-covers N whole days
    before the watermark's day. ``cutoff`` is the complementary
    trailing-edge holdback: the window's end is held back this far from the clock
    so a still-arriving day is never frozen as a complete partition. Both express
    one physical concern -- provider data latency -- from opposite ends, so both
    are sourced from the provider config (``lookback_days`` / ``cutoff_days``),
    not defaulted on the mode.

    Attributes:
        lookback: How far before the watermark each resume re-fetches, to recover
            records that landed after their event-time day.
        cutoff: How far the window's end is held back from the clock, so the most
            recent written partition is always a complete day. Day-granular; zero
            adds no holdback beyond the resolver's own date alignment.
    """

    lookback: timedelta
    cutoff: timedelta


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
# WatermarkMode -> resume resolver + delete-by-window-then-append, FeedMode -> the
# stored token + append. Storage layout (StorageKind) is the orthogonal axis.
type SyncMode = SnapshotMode | WatermarkMode | FeedMode


# The resume value a spec-builder consumes: a DateWindow (watermark, from the
# resume resolver), a FeedToken (feed, the stored token), or None — meaning no
# committed cursor, which covers a snapshot every run plus the watermark/feed
# first-fetch bootstrap. Named here with SpecBuilder because it is the
# spec-builder's input contract; the resolver's own return stays the narrower
# DateWindow | None (watermark arm).
type ResumeValue = DateWindow | FeedToken | None


class SpecBuilder(Protocol):
    """
    Builds the first request for an endpoint — the one genuine per-endpoint behavior.

    Builds only the first request (URL, base params, and the resume injection);
    the page decoder produces every request after it. This is where the canonical
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
            The first ``RequestSpec``; the page decoder builds every request after it.
        """
        ...


class CompletenessCheck(Protocol):
    """
    A provider-reported expected count fired beside a snapshot harvest.

    The truth instrument behind the single-fetch driver's verified
    harvest (an endpoint that silently caps its listing needs an
    independent count to prove the walk lost nothing -- GeoTab's
    ``GetCountOf`` beside the capped ``Get`` is the first case). An
    implementation fires its count request through the SAME open client
    the harvest used, so auth, the limiter (token-per-attempt on the
    given scope), and the classifier all apply. A plain Protocol (not
    ``@runtime_checkable``): it is only declared on the stateless
    ``EndpointDefinition`` and called by the driver, never verified
    dynamically.
    """

    def expected_count(self, client: TransportClient, quota_scope: str) -> int:
        """
        Return the provider-reported count of the harvested entity.

        Args:
            client: The open transport client the harvest ran on.
            quota_scope: The endpoint's rate-limit scope key -- the
                count request spends from the same budget as the data
                pages.

        Returns:
            The provider's expected entity count.
        """
        ...


@dataclass(frozen=True, slots=True, kw_only=True)
class EndpointDefinition[ModelT: ResponseModel]:
    """
    The single source of truth per endpoint: a frozen binding, one strategy per axis.

    A frozen, keyword-only dataclass generic over its per-record response model,
    composing one implementation per behavioral axis (``spec_builder``,
    ``page_decoder`` — swappable implementations) plus the per-endpoint facts. It
    does no work itself except through its ``spec_builder``.

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
    not a full-response wrapper — the ``page_decoder`` owns the envelope, so
    fleet-telemetry-hub's wrapper models are not ported, only the per-record ones.

    Keyword-only because positional fields here are an error waiting to happen. The
    ``event_time_column`` the watermark and date-partitioning read (§3/§5) now
    attaches here, validated against the response model at construction; the
    records ``schema_overrides`` / ``coercion_overrides`` hatch (§9) is the one
    excluded concern still deferred.

    Attributes:
        provider: The provider this endpoint belongs to.
        name: The endpoint's name (e.g. ``'vehicles'``).
        spec_builder: Builds the first request (the one per-endpoint behavior).
        page_decoder: Interprets each response envelope — the page's records and
            its pagination verdict, from one validated view.
        response_model: The per-record response model type.
        quota_scope: Which token bucket this endpoint spends from.
        storage_kind: The §3 storage layout (single file vs date-partitioned) —
            layout only; merge semantics follow ``sync_mode``.
        sync_mode: The sync-mode declaration (``SnapshotMode`` / ``WatermarkMode``
            / ``FeedMode``) — drives resume and write semantics.
        event_time_column: The response model's UTC datetime field the watermark
            and date-partitioning read (§3/§5) — e.g. ``'located_at'``. ``None``
            for endpoints with no event-time dimension (every snapshot). Required
            for ``WatermarkMode`` / ``DATE_PARTITIONED``, forbidden for snapshots;
            validated against ``response_model`` at construction.
        fan_out: The fan-out declaration (``FanOutBinding``) for an endpoint that
            fans a request per member of a roster (e.g. per-vehicle
            ``vehicle_locations``); ``None`` for endpoints that fetch once. Names only
            a ``RosterKey`` -- the source lives in the registry. Read by the
            orchestrator; not validated here.
        completeness_check: The provider-reported-count truth check the
            single-fetch driver fires after the harvest streams (``None``
            for endpoints with no silent cap to guard against).
            Snapshot-mode, ``fan_out=None`` endpoints only -- an
            expected-count comparison is meaningful only against a complete
            listing, which a windowed (partial) or fan-out (per-member) run
            never is; any other declaration is a wiring error rejected at
            construction.
        window_bisection: The adaptive window-bisection declaration
            (``WindowBisection``) for a capped, unsortable Get endpoint;
            ``None`` everywhere else. Watermark-mode, date-partitioned,
            ``fan_out=None`` endpoints only; any other pairing is a wiring
            error rejected at construction. Executed by the orchestrator's
            bisecting driver.
    """

    provider: Provider
    name: str
    spec_builder: SpecBuilder
    page_decoder: PageDecoder
    response_model: type[ModelT]
    quota_scope: QuotaScope
    storage_kind: StorageKind
    sync_mode: SyncMode
    event_time_column: str | None = None
    fan_out: FanOutBinding | None = None
    completeness_check: CompletenessCheck | None = None
    window_bisection: WindowBisection | None = None

    def __post_init__(self) -> None:
        """Validate the binding's storage / sync / event-time / guard coherence.

        Raises:
            ValueError: The storage-kind / sync-mode pairing is invalid, the
                event-time column is required-but-missing or forbidden-but-present
                or names no field on the response model, a completeness check
                is declared outside snapshot-mode single-fetch, or a window
                bisection is declared outside watermark / date-partitioned
                single-fetch.
            TypeError: ``event_time_column`` names a non-date-like field.

        Side Effects:
            None -- reads fields and may raise.
        """
        self._validate_storage_sync_pairing()
        self._validate_event_time_column()
        self._validate_completeness_check()
        self._validate_window_bisection()

    def _validate_window_bisection(self) -> None:
        """Reject a bisection declaration outside its executable shape.

        Bisection recursively narrows a resume window, so it is meaningful
        only for a windowed (``WatermarkMode``), date-partitioned endpoint;
        and it owns the unit's whole request cardinality, so it cannot
        compose with a per-member fan-out.

        Raises:
            ValueError: A bisection is declared on a non-watermark or
                non-date-partitioned endpoint, or beside a fan-out.

        Side Effects:
            None.
        """
        if self.window_bisection is None:
            return
        if not isinstance(self.sync_mode, WatermarkMode) or (
            self.storage_kind is not StorageKind.DATE_PARTITIONED
        ):
            raise ValueError(
                f'{self.provider.value}.{self.name}: window_bisection requires '
                f'WatermarkMode and DATE_PARTITIONED storage, got '
                f'{type(self.sync_mode).__name__} / {self.storage_kind}.'
            )
        if self.fan_out is not None:
            raise ValueError(
                f'{self.provider.value}.{self.name}: window_bisection cannot '
                f'compose with a fan-out declaration.'
            )

    def _validate_storage_sync_pairing(self) -> None:
        """Reject a snapshot endpoint laid out anything but ``SINGLE``.

        A snapshot has no event-time dimension to partition on, so
        ``DATE_PARTITIONED`` is structurally unexecutable for it -- there is no
        column for ``split_by_date`` to split on (DESIGN §3).

        Raises:
            ValueError: The endpoint is a snapshot with a non-``SINGLE`` layout.

        Side Effects:
            None.
        """
        if (
            isinstance(self.sync_mode, SnapshotMode)
            and self.storage_kind is not StorageKind.SINGLE
        ):
            raise ValueError(
                f'{self.provider.value}.{self.name}: SnapshotMode requires '
                f'storage_kind SINGLE, got {self.storage_kind}.'
            )

    def _validate_event_time_column(self) -> None:
        """Validate the event-time column against sync mode, layout, and model.

        Snapshots forbid it (no event-time dimension); ``WatermarkMode`` and
        ``DATE_PARTITIONED`` require it (the watermark and the partition key both
        read it). When present it must name a date-like field on the response
        model, caught here at construction rather than mid-persist, where
        ``split_by_date`` or ``latest_event_time`` would otherwise fail on a
        non-temporal column after the fetch is already spent (DESIGN §3/§5).

        Raises:
            ValueError: The column is required-but-missing, forbidden-but-present,
                or names no field on the response model.
            TypeError: The named field is not date-like.

        Side Effects:
            None.
        """
        is_snapshot = isinstance(self.sync_mode, SnapshotMode)
        requires_event_time = (
            isinstance(self.sync_mode, WatermarkMode)
            or self.storage_kind is StorageKind.DATE_PARTITIONED
        )
        if is_snapshot and self.event_time_column is not None:
            raise ValueError(
                f'{self.provider.value}.{self.name}: snapshot endpoints have no '
                f'event-time dimension, so event_time_column must be None.'
            )
        if requires_event_time and self.event_time_column is None:
            raise ValueError(
                f'{self.provider.value}.{self.name}: WatermarkMode or '
                f'DATE_PARTITIONED requires an event_time_column.'
            )
        if self.event_time_column is not None:
            self._require_date_like_field(self.event_time_column)

    def _validate_completeness_check(self) -> None:
        """Reject a completeness check declared outside snapshot single-fetch.

        An expected-count comparison is meaningful only against a complete
        listing: a snapshot's single chain fetches the entity's full current
        state, so the provider's count and the harvest describe the same
        population. A windowed harvest is deliberately partial (one window of
        an unbounded history) and a fan-out run is per-member -- on either,
        the comparison would be counting different things. Both declarations
        are wiring bugs, rejected here at the declaration seam so the driver
        layer never needs to reason about it.

        Raises:
            ValueError: The check is declared on a non-snapshot or fan-out
                endpoint.

        Side Effects:
            None.
        """
        if self.completeness_check is None:
            return
        if not isinstance(self.sync_mode, SnapshotMode):
            raise ValueError(
                f'{self.provider.value}.{self.name}: a completeness_check '
                f'requires SnapshotMode -- an expected-count comparison is '
                f'meaningful only against a complete listing, and a windowed '
                f'harvest is deliberately partial.'
            )
        if self.fan_out is not None:
            raise ValueError(
                f'{self.provider.value}.{self.name}: a completeness_check '
                f'requires fan_out=None -- a fan-out run is per-member, not '
                f'the complete listing an expected count describes.'
            )

    def _require_date_like_field(self, column: str) -> None:
        """Require ``column`` to name a date-like field on the response model.

        Validated as a top-level Pydantic field name: the records flatten
        preserves top-level field names as column names, so a top-level
        event-time field's column name equals its field name. A nested event-time
        field is not yet supported and would not resolve here (deferred until an
        endpoint needs one).

        Args:
            column: The event-time column name to check against the model.

        Raises:
            ValueError: ``column`` names no field on ``response_model``.
            TypeError: The field is annotated as neither ``date`` nor ``datetime``
                (nullable forms included).

        Side Effects:
            None.
        """
        model_fields = self.response_model.model_fields
        if column not in model_fields:
            valid_names = ', '.join(sorted(model_fields))
            raise ValueError(
                f'{self.provider.value}.{self.name}: event_time_column '
                f'{column!r} is not a field on {self.response_model.__name__}. '
                f'Fields: {valid_names}.'
            )
        annotation = model_fields[column].annotation
        if not _is_date_like_annotation(annotation):
            raise TypeError(
                f'{self.provider.value}.{self.name}: event_time_column '
                f'{column!r} must be date-like, got annotation {annotation!r}.'
            )


# A field annotation is an arbitrary type form (a class, a PEP 604 union, ...):
# typing-justified: Any is the honest input type for annotation inspection
def _is_date_like_annotation(annotation: Any) -> bool:
    """Whether an annotation resolves to ``date`` or ``datetime``.

    Nullable forms count: a provider's ``datetime | None`` timestamp is still a
    valid event-time field. Other unions and non-temporal types do not.

    Args:
        annotation: The field annotation to inspect.

    Returns:
        ``True`` if the annotation is ``date``, ``datetime``, or a two-arm
        optional of either.

    Side Effects:
        None.
    """
    return _unwrap_optional(annotation) in {date, datetime}


# typing-justified: annotation forms (unions, aliases) in, annotation forms out
def _unwrap_optional(annotation: Any) -> Any:
    """Strip ``None`` from a two-arm optional annotation, else return it as is.

    Args:
        annotation: The annotation to inspect.

    Returns:
        The sole non-``None`` arm of a two-arm ``X | None`` union; otherwise
        ``annotation`` unchanged. A union of more than two arms passes through, so
        a genuinely ambiguous union is not treated as date-like.

    Side Effects:
        None.
    """
    union_args = get_args(annotation)
    if not union_args:
        return annotation
    non_none_args = [arg for arg in union_args if arg is not type(None)]
    if len(non_none_args) == 1 and len(non_none_args) != len(union_args):
        return non_none_args[0]
    return annotation
