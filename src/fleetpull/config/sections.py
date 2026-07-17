# src/fleetpull/config/sections.py
"""The run-scoped standalone sections: ``sync``, ``storage``, ``state``.

One model family per file (house rule): these three sections describe
run-wide facts -- when history starts, where the dataset lands, where
operational state lives -- and change together as the run vocabulary
grows. Path fields normalize through ``paths.resolve_path`` at
validation, so a validated section is fully resolved by construction and
downstream code never normalizes.
"""

from datetime import UTC, date, datetime, time
from pathlib import Path

from pydantic import Field, field_validator

from fleetpull.config.base import ConfigModel
from fleetpull.paths import resolve_path

__all__: list[str] = ['StateConfig', 'StorageConfig', 'SyncConfig']


class SyncConfig(ConfigModel):
    """
    User-facing sync-wide settings, one instance per run.

    ``dataset_root`` deliberately does not live here. It did once; the
    config rebuild moved it to ``StorageConfig`` for real, so the YAML
    section and this model are the same shape and no loader machinery
    bridges them. Consumers that need both the anchor and the dataset
    root (the runner) take the root ``FleetpullConfig`` and read each
    from its own section.

    Attributes:
        default_start_date: The cold-start backfill anchor -- the UTC calendar date
            a watermark endpoint's history begins from on its very first run, before
            any committed watermark or completed coverage exists (DESIGN section 4/5
            resume precedence arm 3). Required: there is no safe default for "where
            our history begins", so it must be declared. It goes inert the moment
            observed data or completed coverage exists; thereafter the start is
            derived from those, never from this.
        lookback_days: Package-wide late-arrival re-fetch margin in whole days.
            Optional; ``None`` means no package-wide value is declared. Root-level
            resolution fans a declared value into every provider section that does
            not set its own key (provider key > this > provider default); the
            field itself is never read as a runtime knob.
        cutoff_days: Package-wide trailing-edge holdback in whole days, the
            complement of ``lookback_days``; same precedence and ``None``
            semantics.
        backfill_chunk_days: The width, in whole days, of the work units a
            windowed run's plan tiles its window into (DESIGN sections 4/5).
            Every windowed pull is planned as units and each unit commits
            independently, so smaller chunks mean finer crash-resume
            granularity at the cost of more per-unit commits; a window
            smaller than one chunk degenerates to a single unit (the daily
            run). Sync-level only, deliberately without a per-provider
            override: chunk size is fleetpull's transactional knob, not a
            provider-latency fact. Applies to newly planned units only --
            already-persisted unit boundaries are honored on resume.
    """

    default_start_date: date
    lookback_days: int | None = Field(default=None, ge=0)
    cutoff_days: int | None = Field(default=None, ge=0)
    backfill_chunk_days: int = Field(default=7, ge=1)

    @property
    def default_start_datetime(self) -> datetime:
        """The cold-start anchor as a timezone-aware UTC midnight instant.

        ``default_start_date`` is the human-authored calendar date history
        begins from; the resume resolver (``resolve_resume_start``) composes it
        with watermark and frontier datetimes, so it is lifted to the start of
        that UTC day here. Deriving it keeps the stored field a ``date`` (a
        time-of-day on a backfill anchor is meaningless) while handing the
        orchestrator the ``datetime`` it needs, the conversion defined in one
        place.

        Returns:
            ``default_start_date`` at 00:00:00 with ``tzinfo`` ``datetime.UTC``.
        """
        return datetime.combine(self.default_start_date, time.min, tzinfo=UTC)


class StorageConfig(ConfigModel):
    """
    User-facing storage settings, one instance per run.

    Attributes:
        dataset_root: The root directory the dataset is written under
            (``{root}/{provider}/{endpoint}/``, DESIGN section 3).
            Required: there is no safe default for where output lands.
            Use a real local path -- never a cloud-synced folder
            (OneDrive and kin), whose sync clients fight the writer's
            atomic renames. Normalized through ``resolve_path`` at
            validation.
        drop_exact_duplicates: Whether write-time compaction drops
            exact-duplicate rows (DESIGN section 6 -- the default-on
            dedup with a config flag off; semantic dedup stays out of
            scope). ``False`` preserves duplicates byte-for-byte.
    """

    dataset_root: Path
    drop_exact_duplicates: bool = True

    @field_validator('dataset_root')
    @classmethod
    def _resolve(cls, value: Path) -> Path:
        """Normalize the path lexically; see ``paths.resolve_path``."""
        return resolve_path(value)


class StateConfig(ConfigModel):
    """
    User-facing operational-state settings, one instance per run.

    Attributes:
        database_path: Where the SQLite state database lives -- separable
            from ``dataset_root`` so SQLite stays on a real local disk
            even when the parquet dataset sits on a network filesystem
            (DESIGN section 5). Optional in YAML: root-level resolution
            defaults it to ``<dataset_root>/.fleetpull/state.sqlite3``,
            so any ``FleetpullConfig`` validated from a raw document
            carries a real, normalized path. ``None`` survives only
            direct section construction.
    """

    database_path: Path | None = None

    @field_validator('database_path')
    @classmethod
    def _resolve(cls, value: Path | None) -> Path | None:
        """Normalize the path lexically when present; ``None`` passes through."""
        return None if value is None else resolve_path(value)
