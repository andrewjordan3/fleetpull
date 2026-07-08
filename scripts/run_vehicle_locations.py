# scripts/run_vehicle_locations.py
"""Hand-run driver: prove the vehicle_locations incremental loop through ``Sync``.

The after-picture of the Sync build (the consumer-cost evidence, closing
the loop the fetch build's snapshot script opened): the hand composition
this script used to carry -- provider config, endpoint and roster
registries, state database and stores, provider profile, client runtime,
client registry, refresh coordinator, run executor, and the
orchestration-entry closure -- is now one written config file and
``Sync(config_path).run()``. The diagnostic reporting stays as reads over
the results: partition layout, persisted watermark, roster membership,
net-new analysis, and the dedup check.

Run 1 against an empty directory exercises the cold-backfill arm (the
coordinator harvests the vehicles feeder to populate the fan-out roster);
Run 2 immediately after exercises resume from the persisted watermark.
State is deliberately retained across invocations.

Set MOTIVE_API_KEY in the environment (never in a file -- ``Sync``'s
credential fallback reads it) and DATASET_ROOT below, then run from the
repo root:

    uv run python scripts/run_vehicle_locations.py

Errors propagate with a traceback by design -- this is a debugging driver.
"""

import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

from fleetpull import Sync
from fleetpull.incremental import DateWatermark, IncrementalCursor
from fleetpull.paths import endpoint_directory, parse_date_partition_segment
from fleetpull.roster import RosterKey
from fleetpull.state import CursorStore, RosterStore, StateDatabase
from fleetpull.timing import SystemClock
from fleetpull.vocabulary import Provider

# --- knobs (the config file below is generated from these) ------------------

# Set MOTIVE_API_KEY in the environment; Sync's env fallback reads it and it
# never touches a file, temp or otherwise.
MOTIVE_API_KEY: str = os.environ.get('MOTIVE_API_KEY', '')

# Where the parquet and the SQLite operational-state DB land. Set this for
# your environment before running (e.g. a OneDrive-synced path on the
# laptop). State is deliberately retained across invocations: an empty
# directory exercises the cold-start arm, and every later run resumes from
# the persisted watermark -- the resume path is part of what this proves.
DATASET_ROOT: str = ''

# True behind the Zscaler-intercepting laptop; False on Colab / a GCP VM.
USE_TRUSTSTORE: bool = True

# Motive's vehicle_locations start_date/end_date are day-granular (see the
# spec-builder), so the bound below is a day count, not an hour count. An
# unbounded cold backfill would pull the endpoint's full history across every
# vehicle onto OneDrive -- this keeps Run 1's window to a single day.
BACKFILL_LOOKBACK_DAYS: int = 1

# The watermark endpoint's late-arrival re-fetch margin and trailing-edge
# holdback (per-provider lookback_days / cutoff_days keys). The resume window
# is [floor(watermark - lookback), trailing_edge), so its width is the
# watermark's age plus the lookback: a small lookback keeps the re-fetch
# *margin* small, not the window -- a run days after the last watermark
# still re-pulls every day since it, regardless of this value.
LOOKBACK_DAYS: int = 1
CUTOFF_DAYS: int = 0

# The identity a breadcrumb is unique on: Motive's own per-record UUID
# (VehicleLocation.location_id, wire key `id`) -- a stronger natural key than
# a composite of vehicle + timestamp, and the one this endpoint's model
# actually carries.
_DEDUP_IDENTITY_COLUMN: str = 'location_id'

_VEHICLE_IDS_KEY = RosterKey(Provider.MOTIVE, 'vehicle_ids')

_INCREMENTAL_SEMANTICS = """\
Incremental semantics this proof relies on (read from the source, not assumed):
  - The composition is the public verb: Sync loads the generated config,
    validates the endpoint selection against the catalog, and composes the
    state DB, registries, clients, and run executor itself -- this script
    builds none of that.
  - The fan-out is declared, not hand-built: the endpoint's FanOutBinding names
    the `vehicle_ids` roster (discovered from its feeder's module, never
    hand-listed), and the orchestration entry resolves it through the roster
    registry, refresh coordinator, and store.
  - The resume window is half-open [start, end): start inclusive, end exclusive
    (fleetpull.incremental.DateWindow); for this day-granular endpoint both
    bounds are UTC midnights, so every covered date is refetched whole (the
    floored-window invariant wholesale partition replacement relies on).
  - The persisted watermark is the maximum observed `located_at` across a run's
    kept rows; it advances only when that observed maximum is strictly past
    the prior watermark (orchestrator/resume.py:should_advance_watermark).
  - Resume precedence: persisted watermark (less the lookback margin) takes
    precedence over the coverage frontier, which takes precedence over the
    default_start_date cold-start anchor; the chosen start is floored to the
    UTC midnight of its date, so a lookback of N days re-covers N whole days
    before the watermark's day. The in-window filter's residual job is
    dropping provider overshoot outside the requested dates.
  - Dedup is structural, not row-level: a watermark + date-partitioned run
    REPLACES each touched date=YYYY-MM-DD partition wholesale (the prior
    contents are never read or appended to), so a date refetched by a later
    run overwrites rather than duplicates. Within one run, exact-duplicate
    rows landing on the same date (e.g. pagination or fan-out seams) are
    additionally dropped at compaction (storage.drop_exact_duplicates,
    default on).
"""


def _write_config_file(dataset_root: Path) -> Path:
    """Generate the run's YAML config beside the dataset root.

    The credential is deliberately absent: Sync's environment fallback
    resolves MOTIVE_API_KEY, so the secret never lands in a file.

    The logging section carries the designed console/file split: INFO on
    the console, full DEBUG flow in a log file beside the dataset.

    Args:
        dataset_root: Where the dataset (and the generated config) live.

    Returns:
        The written config path.
    """
    default_start_date = (
        datetime.now(tz=UTC) - timedelta(days=BACKFILL_LOOKBACK_DAYS)
    ).date()
    config_path = dataset_root / 'fleetpull-diagnostic.yaml'
    config_path.write_text(
        f'sync:\n'
        f'  default_start_date: {default_start_date.isoformat()}\n'
        f'storage:\n'
        f'  dataset_root: {dataset_root}\n'
        f'logging:\n'
        f'  console_level: INFO\n'
        f'  file_path: {dataset_root / "fleetpull-diagnostic.log"}\n'
        f'  file_level: DEBUG\n'
        f'http:\n'
        f'  use_truststore: {str(USE_TRUSTSTORE).lower()}\n'
        f'providers:\n'
        f'  motive:\n'
        f'    endpoints: [vehicle_locations]\n'
        f'    lookback_days: {LOOKBACK_DAYS}\n'
        f'    cutoff_days: {CUTOFF_DAYS}\n',
        encoding='utf-8',
    )
    return config_path


def _read_partition_frames(endpoint_dir: Path) -> dict[date, pl.DataFrame]:
    """Every written date=YYYY-MM-DD partition, read into a frame keyed by date."""
    frames: dict[date, pl.DataFrame] = {}
    for part_file in sorted(endpoint_dir.glob('date=*/part.parquet')):
        partition_date = parse_date_partition_segment(part_file.parent.name)
        frames[partition_date] = pl.read_parquet(part_file)
    return frames


def _print_partition_layout(
    endpoint_dir: Path, frames: dict[date, pl.DataFrame]
) -> None:
    """Print the on-disk partition structure -- paths and counts, never values."""
    if not frames:
        print(f'  No partitions under {endpoint_dir} yet.')
        return
    for partition_date, frame in sorted(frames.items()):
        print(f'  date={partition_date.isoformat()}/part.parquet: {frame.height} rows')
    total_rows = sum(frame.height for frame in frames.values())
    print(f'  Total: {total_rows} rows across {len(frames)} partitions.')


def _print_watermark(label: str, cursor: IncrementalCursor | None) -> None:
    """Report a persisted watermark's value, or that none is stored yet."""
    match cursor:
        case None:
            print(f'{label}: no watermark persisted yet.')
        case DateWatermark(watermark=watermark):
            print(f'{label}: {watermark.isoformat()}')
        case _:
            raise TypeError(
                f'expected a DateWatermark cursor for a watermark endpoint, '
                f'got {type(cursor).__name__}'
            )


def _check_duplicates(combined: pl.DataFrame) -> None:
    """Report duplicate-row pass/fail on the combined on-disk output.

    Args:
        combined: Every written partition's rows, concatenated.

    Raises:
        SystemExit: The combined output holds duplicate identity-column values.
    """
    if combined.is_empty():
        print('Dedup check: no rows written; nothing to check (PASS).')
        return
    unique_count = combined.get_column(_DEDUP_IDENTITY_COLUMN).n_unique()
    duplicate_count = combined.height - unique_count
    print(
        f'Dedup check on `{_DEDUP_IDENTITY_COLUMN}` across {combined.height} '
        f'combined rows: {duplicate_count} duplicates.'
    )
    if duplicate_count > 0:
        raise SystemExit(
            f'DEDUP CHECK FAILED: {duplicate_count} duplicate '
            f'{_DEDUP_IDENTITY_COLUMN} value(s) across the combined output.'
        )
    print('Dedup check: PASS.')


class _RunReporter:
    """Runs the sync and reports the post-run reads, once per label.

    The run outcome is no longer visible piecewise -- ``Sync.run()``
    returns ``None`` by design -- so the report is entirely reads over
    the results: the persisted watermark and the on-disk partitions.
    """

    def __init__(self, sync: Sync, cursor_store: CursorStore, endpoint_dir: Path):
        self._sync = sync
        self._cursor_store = cursor_store
        self._endpoint_dir = endpoint_dir

    def run(self, label: str) -> dict[date, pl.DataFrame]:
        """Run once, then report the watermark and on-disk layout."""
        print(f'\n--- {label} ---')
        self._sync.run()
        watermark = self._cursor_store.get_cursor(Provider.MOTIVE, 'vehicle_locations')
        _print_watermark(f'Watermark after {label}', watermark)
        frames = _read_partition_frames(self._endpoint_dir)
        print(f'On-disk partitions after {label}:')
        _print_partition_layout(self._endpoint_dir, frames)
        return frames


def main() -> None:
    """Drive vehicle_locations through backfill -> resume -> dedup and report."""
    if not MOTIVE_API_KEY:
        raise SystemExit('Set MOTIVE_API_KEY in the environment before running.')
    if not DATASET_ROOT:
        raise SystemExit('Set DATASET_ROOT to a destination path before running.')

    print(_INCREMENTAL_SEMANTICS)

    dataset_root = Path(DATASET_ROOT)
    dataset_root.mkdir(parents=True, exist_ok=True)
    config_path = _write_config_file(dataset_root)
    print(f'Generated config: {config_path}')

    sync = Sync(config_path)
    endpoint_dir = endpoint_directory(
        dataset_root, Provider.MOTIVE.value, 'vehicle_locations'
    )

    # Post-run diagnostic READ handles over the same state DB Sync writes.
    database = StateDatabase(dataset_root / '.fleetpull' / 'state.sqlite3')
    cursor_store = CursorStore(database, SystemClock())
    roster_store = RosterStore(database)

    reporter = _RunReporter(sync, cursor_store, endpoint_dir)

    print(
        '\nOn a fresh state store, Run 1 first harvests the vehicles feeder to '
        'populate the fan-out roster (the cold-start path) -- expected, not '
        'an error.'
    )
    frames_after_1 = reporter.run('Run 1 (cold backfill)')
    roster_member_count = len(roster_store.read_members(_VEHICLE_IDS_KEY))
    print(
        f'Fan-out roster {_VEHICLE_IDS_KEY.name!r} (read back from the '
        f'store): {roster_member_count} members.'
    )
    if frames_after_1:
        sample = next(iter(frames_after_1.values()))
        print(f'Resulting frame shape (one partition sample): {sample.shape}')
        print(f'Columns: {sample.columns}')

    frames_after_2 = reporter.run('Run 2 (resume)')

    rows_after_1 = sum(frame.height for frame in frames_after_1.values())
    rows_after_2 = sum(frame.height for frame in frames_after_2.values())
    net_new = rows_after_2 - rows_after_1
    if net_new > 0:
        print(
            f'\nRun 2 grew the on-disk row count by {net_new} rows -- genuinely new '
            f'data (a new date and/or a lookback-driven late-arrival correction).'
        )
    else:
        print(
            '\nRun 2 added no net rows -- the runs happened back-to-back, so the '
            'resume window resolved to the same date(s) Run 1 already covered '
            '(each replaced wholesale, not appended). This is expected: the dedup '
            'proof below rests on that boundary overlap, not on new data.'
        )

    print('\n--- Dedup check across the combined output ---')
    combined = pl.concat(frames_after_2.values()) if frames_after_2 else pl.DataFrame()
    _check_duplicates(combined)


if __name__ == '__main__':
    main()
