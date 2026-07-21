# src/fleetpull/endpoints/shared/sync_mode.py
"""The sync-mode and storage-layout declaration family.

The two declared axes every ``EndpointDefinition`` carries (the
``request_shape.py`` family precedent -- one declaration family per file):
``StorageKind`` is the §3 storage *layout* (where the bytes live), and the
``SyncMode`` union (``SnapshotMode`` / ``WatermarkMode`` / ``FeedMode``) is
the resume-and-write *semantic* (how a run resumes and what its write does to
the data). The two are orthogonal; the valid pairings are validated on the
binding (``base.py``) at construction.
"""

from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum

__all__: list[str] = [
    'FeedMode',
    'SnapshotMode',
    'StorageKind',
    'SyncMode',
    'WatermarkMode',
]


class StorageKind(StrEnum):
    """
    The §3 storage *layout* an endpoint declares: one parquet file vs hive
    partitions. Layout only — *where* the bytes live, not how they merge.

    ``SINGLE`` is one ``data.parquet``; ``DATE_PARTITIONED`` is hive
    ``date=YYYY-MM-DD`` partitions; ``APPEND_LOG`` is hive ``date=`` partitions
    holding numbered ``part-NNNNN.parquet`` files that only ever accumulate —
    the feed arm's append-only layout, where nothing is ever deleted or
    replaced. The caller dispatches on it to pick the storage path —
    read-the-whole-file for ``SINGLE``, touch-only-overlapping-partitions for
    ``DATE_PARTITIONED``, append-new-parts-only for ``APPEND_LOG``. What that
    write *does* to the data — full-replace, delete-by-window-then-append, or
    append — is the ``SyncMode``'s concern, not the layout's; the two are
    orthogonal axes the storage layer combines (``APPEND_LOG`` pairs only with
    ``FeedMode``, validated at construction: append-only is the feed stream's
    write semantic, and every windowed or snapshot semantic would corrupt an
    accumulate-only layout).

    It lives here beside the binding, not in ``vocabulary/``: unlike
    ``QuotaScope``
    (which config validates against, so it must sit in a leaf config can import),
    ``StorageKind`` has no low-layer consumer — it travels with the
    ``EndpointDefinition`` it configures.
    """

    SINGLE = 'single'
    DATE_PARTITIONED = 'date_partitioned'
    APPEND_LOG = 'append_log'


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
        fixed_unit_days: The endpoint's fixed work-unit width in whole days, or
            ``None`` (the default) to tile at ``sync.backfill_chunk_days``. Set
            only by endpoints whose rows are per-request-window rollups: the
            provider aggregates over exactly the requested window, so the unit
            width is part of the ROW'S MEANING and must never float with user
            configuration — the Samsara fuel-energy probe proved day rollups are
            NOT a lossless decomposition of wider windows (summing two adjacent
            day windows reproduced the two-day rollup on only 178 of 267
            vehicles; DESIGN §8, 2026-07-21). When set, the window planner tiles
            this endpoint's resume window into units of exactly this many days,
            ignoring ``sync.backfill_chunk_days``; the config knob remains the
            default for every endpoint that leaves this ``None``. Validated
            >= 1 at construction.
    """

    lookback: timedelta
    cutoff: timedelta
    fixed_unit_days: int | None = None

    def __post_init__(self) -> None:
        """Validate the fixed unit width when one is declared.

        Raises:
            ValueError: ``fixed_unit_days`` is set but not >= 1 — a
                zero-or-negative unit width can tile nothing.

        Side Effects:
            None -- reads fields and may raise.
        """
        if self.fixed_unit_days is not None and self.fixed_unit_days < 1:
            raise ValueError(
                f'fixed_unit_days must be >= 1 when set, got {self.fixed_unit_days}.'
            )


@dataclass(frozen=True, slots=True)
class FeedMode:
    """
    Feed sync declaration (config): a marker carrying no configuration.

    The feed arm needs no config — its resume value is the stored ``FeedToken``
    used directly (no lookback, no window), or a ``FeedSeed`` from the sync-wide
    cold-start anchor on the tokenless first run. Its write semantic is
    *append-only*: the feed is a forward-only version stream stored as emitted
    — every run appends new numbered part files into the event-date partitions
    its records belong to, and nothing is ever deleted or replaced (DESIGN §4).
    Re-emitted versions and crash-window duplicates land as new rows; the
    consumer reconciles calculated feeds by ``(id, max version)`` and active
    feeds by ``id``. A ``FeedMode`` endpoint therefore requires the
    ``APPEND_LOG`` layout and an ``event_time_column`` (the records' own event
    dates route the partitions), both validated at construction. A marker
    member of ``SyncMode``, distinct from the runtime ``FeedToken`` cursor
    state in ``incremental/``.
    """


# The endpoint's sync-mode declaration (config): the caller matches on it to drive
# both resume and write semantics — SnapshotMode -> no resume + full replace,
# WatermarkMode -> resume resolver + delete-by-window-then-append, FeedMode -> the
# stored token (or cold-start seed) + append-only. Storage layout (StorageKind) is
# the orthogonal axis.
type SyncMode = SnapshotMode | WatermarkMode | FeedMode
