# scripts/run_vehicle_locations.py
"""Throwaway driver: prove the vehicle_locations incremental loop end to end.

Unlike ``run_vehicles_snapshot.py`` (one snapshot call), vehicle_locations is a
watermarked, per-vehicle fan-out endpoint, so the point of this script is the
incremental *loop*: a cold backfill run from an empty ``DATASET_ROOT``, then a
second run that resumes from the watermark the first run persisted to SQLite,
then a check that the combined on-disk output holds no duplicate rows.

The fan-out is composed, not hand-built: the script hands the resolved
definition to the orchestration entry (``run_endpoint``) and the declared
``FanOutBinding`` resolves through the roster machinery -- see the printed
semantics block. The first run populates the roster via the cold-start
listing path (expected, not an error).

Also validates two pieces of real infrastructure unit tests cannot: truststore
through the corporate (Zscaler) proxy on a live API call, and Parquet + SQLite
writes over a OneDrive-synced filesystem.

Run from the repo root once MOTIVE_API_KEY and DATASET_ROOT are set in this file:

    uv run python scripts/run_vehicle_locations.py

Errors propagate with a traceback by design -- this is a debugging driver.
"""

import logging
import random
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path

import polars as pl
from pydantic import SecretStr

from fleetpull.config import HttpConfig, MotiveConfig, RetryConfig, SyncConfig
from fleetpull.endpoints import build_endpoint_registry
from fleetpull.endpoints.motive.vehicles import VEHICLE_IDS_ROSTER
from fleetpull.endpoints.shared import EndpointDefinition
from fleetpull.incremental import DateWatermark, IncrementalCursor
from fleetpull.model_contract import ResponseModel
from fleetpull.network.auth import StaticHeaderAuth
from fleetpull.network.classifiers import MotiveResponseClassifier
from fleetpull.network.client import (
    ClientRuntime,
    ProviderClientRegistry,
    ProviderProfile,
)
from fleetpull.network.limits import RateLimitConfig, RateLimiterRegistry
from fleetpull.orchestrator import (
    CaughtUp,
    EndpointRunner,
    Executed,
    RosterRefreshCoordinator,
    RunOutcome,
    run_endpoint,
)
from fleetpull.paths import endpoint_directory, parse_date_partition_segment
from fleetpull.roster import RosterRegistry
from fleetpull.state import (
    CursorStore,
    RosterStore,
    RunLedger,
    StateDatabase,
    migrate_to_head,
)
from fleetpull.timing import SystemClock, SystemSleeper
from fleetpull.vocabulary import Provider, QuotaScope

# --- hardcoded config (stands in for the YAML loader) -----------------------

# Paste your Motive API key here for a quick run. Left empty in committed
# versions of this file -- never commit a live key.
MOTIVE_API_KEY: str = ''

# Where the parquet and the SQLite operational-state DB land. Set this for
# your environment before running (e.g. a OneDrive-synced path on the
# laptop). Must be a fresh/empty directory -- this script proves the *cold*
# backfill arm, which assumes no prior state.
DATASET_ROOT: str = ''

# True behind the Zscaler-intercepting laptop; False on Colab / a GCP VM.
USE_TRUSTSTORE: bool = True

MOTIVE_BASE_URL: str = 'https://api.gomotive.com'

# Motive's vehicle_locations start_date/end_date are day-granular (see the
# spec-builder), so the bound below is a day count, not an hour count. An
# unbounded cold backfill would pull the endpoint's full history across every
# vehicle onto OneDrive -- this keeps Run 1's window to a single day.
BACKFILL_LOOKBACK_DAYS: int = 1

# The watermark endpoint's late-arrival re-fetch margin and trailing-edge
# holdback (MotiveConfig.lookback_days / cutoff_days). A small lookback keeps
# Run 2's resume window similarly modest -- MotiveConfig's production default
# (7 days) would silently widen the second pull well past "a day."
LOOKBACK_DAYS: int = 1
CUTOFF_DAYS: int = 0

# Placeholder Motive rate limits (real values TBD per DESIGN). Unlike the
# snapshot script, vehicle_locations fans out one request per vehicle, so
# this genuinely binds -- part of what the run exercises.
MOTIVE_RATE_LIMIT: RateLimitConfig = RateLimitConfig(
    requests_per_period=60, period_seconds=60.0, burst=10, max_concurrency=2
)

# The identity a breadcrumb is unique on: Motive's own per-record UUID
# (VehicleLocation.location_id, wire key `id`) -- a stronger natural key than
# a composite of vehicle + timestamp, and the one this endpoint's model
# actually carries.
_DEDUP_IDENTITY_COLUMN: str = 'location_id'

_INCREMENTAL_SEMANTICS = """\
Incremental semantics this proof relies on (read from the source, not assumed):
  - The fan-out is declared, not hand-built: the endpoint's FanOutBinding names
    the `vehicle_ids` roster, and the orchestration entry (run_endpoint)
    resolves it through the roster registry, refresh coordinator, and store --
    this script never constructs a driver or harvests members itself.
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
    additionally dropped at compaction (storage/frames.py:drop_exact_duplicates).
"""

# Surfaces the library's own internal logging (auth, retries, "caught up")
# alongside this script's print()-based report.
logging.basicConfig(level=logging.INFO)


def _build_motive_config() -> MotiveConfig:
    """The Motive provider config, with a small lookback/cutoff for this run."""
    return MotiveConfig(
        base_url=MOTIVE_BASE_URL, lookback_days=LOOKBACK_DAYS, cutoff_days=CUTOFF_DAYS
    )


def _build_client_runtime() -> ClientRuntime:
    """The shared transport runtime, rate-limited on the one Motive quota scope."""
    limits = {QuotaScope.MOTIVE.value: MOTIVE_RATE_LIMIT}
    return ClientRuntime(
        http_config=HttpConfig(use_truststore=USE_TRUSTSTORE),
        retry_config=RetryConfig(),
        limiter_registry=RateLimiterRegistry(limits),
        random_source=random.Random(),
        sleeper=SystemSleeper(),
    )


def _build_state(
    dataset_root: Path,
) -> tuple[StateDatabase, CursorStore, RunLedger, RosterStore]:
    """Initialize the SQLite operational-state DB at its DESIGN-convention path."""
    database_path = dataset_root / '.fleetpull' / 'state.sqlite3'
    database = StateDatabase(database_path)
    database.initialize()
    migrate_to_head(database)
    clock = SystemClock()
    return (
        database,
        CursorStore(database, clock),
        RunLedger(database, clock),
        RosterStore(database),
    )


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


def _print_run_outcome(label: str, outcome: RunOutcome) -> None:
    """Report one run's outcome uniformly, dispatching on the RunOutcome union."""
    match outcome:
        case Executed(records_fetched=records_fetched, write=write):
            print(
                f'{label}: fetched {records_fetched} records; wrote '
                f'{write.rows_written} rows across {write.files_written} partition '
                f'file(s) ({write.duplicates_dropped} exact duplicates dropped at '
                f'compaction, {len(write.deleted_partitions)} empty partitions pruned).'
            )
        case CaughtUp():
            print(f'{label}: CaughtUp -- the resume window resolved to empty.')


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
    """Runs the composed vehicle_locations run and reports it, once per label.

    Bundles the collaborators that stay constant across Run 1 and Run 2 --
    only the label changes between calls -- so the call site reads as the
    two-run loop it is. The run itself is a zero-argument callable closing
    over the orchestration-entry collaborators; the reporter never sees the
    driver or the roster machinery.
    """

    def __init__(
        self,
        run_once: Callable[[], RunOutcome],
        definition: EndpointDefinition[ResponseModel],
        cursor_store: CursorStore,
        endpoint_dir: Path,
    ) -> None:
        self._run_once = run_once
        self._definition = definition
        self._cursor_store = cursor_store
        self._endpoint_dir = endpoint_dir

    def run(self, label: str) -> dict[date, pl.DataFrame]:
        """Run once, report its outcome, watermark, and on-disk layout."""
        print(f'\n--- {label} ---')
        outcome = self._run_once()
        _print_run_outcome(label, outcome)
        watermark = self._cursor_store.get_cursor(
            self._definition.provider, self._definition.name
        )
        _print_watermark(f'Watermark after {label}', watermark)
        frames = _read_partition_frames(self._endpoint_dir)
        print(f'On-disk partitions after {label}:')
        _print_partition_layout(self._endpoint_dir, frames)
        return frames


def main() -> None:
    """Drive vehicle_locations through backfill -> resume -> dedup and report."""
    if not MOTIVE_API_KEY:
        raise SystemExit('Set MOTIVE_API_KEY (in this file) before running.')
    if not DATASET_ROOT:
        raise SystemExit('Set DATASET_ROOT to a destination path before running.')

    print(_INCREMENTAL_SEMANTICS)

    dataset_root = Path(DATASET_ROOT)
    motive_config = _build_motive_config()
    endpoint_registry = build_endpoint_registry([motive_config])
    locations_definition = endpoint_registry.get(Provider.MOTIVE, 'vehicle_locations')
    roster_registry = RosterRegistry([VEHICLE_IDS_ROSTER])
    endpoint_dir = endpoint_directory(
        dataset_root, locations_definition.provider.value, locations_definition.name
    )

    database, cursor_store, run_ledger, roster_store = _build_state(dataset_root)
    print(f'State database: {database.database_path}')

    profile = ProviderProfile(
        auth=StaticHeaderAuth('X-API-Key', SecretStr(MOTIVE_API_KEY)),
        classifier=MotiveResponseClassifier(),
    )
    runtime = _build_client_runtime()
    clock = SystemClock()
    default_start_date = (
        clock.now_utc() - timedelta(days=BACKFILL_LOOKBACK_DAYS)
    ).date()
    sync_config = SyncConfig(
        default_start_date=default_start_date, dataset_root=dataset_root
    )

    with ProviderClientRegistry({Provider.MOTIVE: profile}, runtime) as clients:
        coordinator = RosterRefreshCoordinator(
            endpoint_registry, roster_store, run_ledger, clients, clock
        )
        runner = EndpointRunner(
            client_source=clients,
            run_recorder=run_ledger,
            clock=clock,
            cursor_access=cursor_store,
            sync_config=sync_config,
        )

        def run_locations_once() -> RunOutcome:
            """One composed run: the entry resolves the declared fan-out itself."""
            return run_endpoint(
                locations_definition, runner, roster_registry, coordinator, roster_store
            )

        reporter = _RunReporter(
            run_locations_once, locations_definition, cursor_store, endpoint_dir
        )

        print(
            '\nOn a fresh state store, Run 1 first lists the vehicles feeder to '
            'populate the fan-out roster (the cold-start path) -- expected, not '
            'an error.'
        )
        frames_after_1 = reporter.run('Run 1 (cold backfill)')
        roster_member_count = len(roster_store.read_members(VEHICLE_IDS_ROSTER.key))
        print(
            f'Fan-out roster {VEHICLE_IDS_ROSTER.key.name!r} (read back from the '
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
