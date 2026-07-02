"""Tests for fleetpull.storage.writers."""

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    FeedMode,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
    SyncMode,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.model_contract import ResponseModel
from fleetpull.models.motive import Vehicle
from fleetpull.network.decoders import MotiveWrappedListPageDecoder
from fleetpull.storage.writers import (
    SnapshotWriter,
    WatermarkPartitionedWriter,
    select_writer,
)
from fleetpull.vocabulary import Provider, QuotaScope


def _frame() -> pl.DataFrame:
    return pl.DataFrame({'a': [1, 2], 'b': ['x', 'y']})


def _located_frame(rows: list[tuple[datetime, int]]) -> pl.DataFrame:
    """Build a (located_at, id) frame from (datetime, id) rows."""
    return pl.DataFrame(
        {
            'located_at': [moment for moment, _ in rows],
            'id': [identifier for _, identifier in rows],
        }
    )


class _LocationStub(ResponseModel):
    located_at: datetime


def _vehicles_definition(sync_mode: SyncMode) -> EndpointDefinition[Vehicle]:
    """The real Motive vehicles single-file binding with the given sync mode.

    select_writer reads only provider / name / storage_kind / sync_mode; the
    other fields are the real vehicles binding (snapshot + SINGLE). A FeedMode
    variant exercises the unbuilt-cell routing (feed needs no event_time_column,
    so it constructs).
    """
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='vehicles',
        spec_builder=StaticGetSpecBuilder(
            base_url='https://api.gomotive.com', path='/v1/vehicles'
        ),
        page_decoder=MotiveWrappedListPageDecoder(
            list_key='vehicles', item_key='vehicle', per_page=100
        ),
        response_model=Vehicle,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.SINGLE,
        sync_mode=sync_mode,
    )


def _partitioned_watermark_definition() -> EndpointDefinition[_LocationStub]:
    """A date-partitioned watermark binding (the vehicle_locations shape)."""
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='vehicle_locations',
        spec_builder=StaticGetSpecBuilder(
            base_url='https://api.gomotive.com', path='/v3/vehicle_locations'
        ),
        page_decoder=MotiveWrappedListPageDecoder(
            list_key='vehicle_locations',
            item_key='vehicle_location',
            per_page=100,
        ),
        response_model=_LocationStub,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=2)),
        event_time_column='located_at',
    )


class TestSnapshotWriter:
    def test_first_run_writes_the_file(self, tmp_path: Path) -> None:
        writer = SnapshotWriter(tmp_path)
        writer.write(_frame())
        result = writer.finalize()
        assert (tmp_path / 'data.parquet').exists()
        assert result.rows_written == 2
        assert result.files_written == 1

    def test_second_run_overwrites_the_first(self, tmp_path: Path) -> None:
        first = SnapshotWriter(tmp_path)
        first.write(_frame())
        first.finalize()
        replacement = pl.DataFrame({'a': [9], 'b': ['z']})
        second = SnapshotWriter(tmp_path)
        second.write(replacement)
        second.finalize()
        assert pl.read_parquet(tmp_path / 'data.parquet').equals(replacement)

    def test_reports_dropped_exact_duplicates(self, tmp_path: Path) -> None:
        duped = pl.DataFrame({'a': [1, 1, 2], 'b': ['x', 'x', 'y']})
        writer = SnapshotWriter(tmp_path)
        writer.write(duped)
        result = writer.finalize()
        assert result.duplicates_dropped == 1
        assert result.rows_written == 2

    def test_does_not_read_the_prior_file(self, tmp_path: Path) -> None:
        # Garbage where data.parquet lives: a writer that read it before
        # overwriting would raise on the non-parquet bytes. The snapshot path
        # overwrites without reading, so the write succeeds.
        (tmp_path / 'data.parquet').write_bytes(b'not a parquet file')
        writer = SnapshotWriter(tmp_path)
        writer.write(_frame())
        writer.finalize()
        assert pl.read_parquet(tmp_path / 'data.parquet').equals(_frame())


class TestWatermarkPartitionedWriter:
    def test_single_run_writes_one_partition_per_date(self, tmp_path: Path) -> None:
        window = DateWindow(
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 3, tzinfo=UTC),
        )
        writer = WatermarkPartitionedWriter(tmp_path, 'located_at', window)
        writer.write(
            _located_frame(
                [
                    (datetime(2026, 6, 1, 8, tzinfo=UTC), 1),
                    (datetime(2026, 6, 2, 8, tzinfo=UTC), 2),
                ]
            )
        )
        result = writer.finalize()
        assert (tmp_path / 'date=2026-06-01' / 'part.parquet').exists()
        assert (tmp_path / 'date=2026-06-02' / 'part.parquet').exists()
        assert result.rows_written == 2
        assert result.files_written == 2

    def test_fan_out_folds_into_one_partition(self, tmp_path: Path) -> None:
        window = DateWindow(
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
        )
        writer = WatermarkPartitionedWriter(tmp_path, 'located_at', window)
        writer.write(_located_frame([(datetime(2026, 6, 1, 8, tzinfo=UTC), 1)]))
        writer.write(_located_frame([(datetime(2026, 6, 1, 9, tzinfo=UTC), 2)]))
        writer.finalize()
        part = pl.read_parquet(tmp_path / 'date=2026-06-01' / 'part.parquet')
        assert sorted(part.get_column('id').to_list()) == [1, 2]

    def test_replaces_the_existing_partition(self, tmp_path: Path) -> None:
        window = DateWindow(
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
        )
        stale_dir = tmp_path / 'date=2026-06-01'
        stale_dir.mkdir(parents=True)
        pl.DataFrame(
            {'located_at': [datetime(2026, 6, 1, 1, tzinfo=UTC)], 'id': [99]}
        ).write_parquet(stale_dir / 'part.parquet')
        writer = WatermarkPartitionedWriter(tmp_path, 'located_at', window)
        writer.write(_located_frame([(datetime(2026, 6, 1, 8, tzinfo=UTC), 1)]))
        writer.finalize()
        part = pl.read_parquet(stale_dir / 'part.parquet')
        assert part.get_column('id').to_list() == [1]

    def test_prunes_the_empty_refetch_date(self, tmp_path: Path) -> None:
        window = DateWindow(
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 3, tzinfo=UTC),
        )
        stale_dir = tmp_path / 'date=2026-06-01'
        stale_dir.mkdir(parents=True)
        pl.DataFrame(
            {'located_at': [datetime(2026, 6, 1, 1, tzinfo=UTC)], 'id': [99]}
        ).write_parquet(stale_dir / 'part.parquet')
        writer = WatermarkPartitionedWriter(tmp_path, 'located_at', window)
        writer.write(_located_frame([(datetime(2026, 6, 2, 8, tzinfo=UTC), 2)]))
        result = writer.finalize()
        assert not stale_dir.exists()
        assert result.deleted_partitions == [date(2026, 6, 1)]
        assert (tmp_path / 'date=2026-06-02' / 'part.parquet').exists()

    def test_clears_stale_staging_shards_at_construction(self, tmp_path: Path) -> None:
        window = DateWindow(
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
        )
        # A prior run crashed after staging but before compaction: a stale shard
        # (id=99) sits under the covered date's staging directory.
        staging = tmp_path / 'date=2026-06-01' / 'staging'
        staging.mkdir(parents=True)
        pl.DataFrame(
            {'located_at': [datetime(2026, 6, 1, 1, tzinfo=UTC)], 'id': [99]}
        ).write_parquet(staging / 'shard-stale.shard')
        writer = WatermarkPartitionedWriter(tmp_path, 'located_at', window)
        writer.write(_located_frame([(datetime(2026, 6, 1, 8, tzinfo=UTC), 1)]))
        writer.finalize()
        part = pl.read_parquet(tmp_path / 'date=2026-06-01' / 'part.parquet')
        assert part.get_column('id').to_list() == [1]

    def test_write_rejects_a_staged_date_outside_the_window(
        self, tmp_path: Path
    ) -> None:
        # The interior tripwire: a staged partition date outside
        # window_dates(window) means an upstream window filter missed rows;
        # wholesale replacement and the prune must not proceed on a date the
        # run had no right to touch.
        window = DateWindow(
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
        )
        writer = WatermarkPartitionedWriter(tmp_path, 'located_at', window)
        with pytest.raises(ValueError, match='outside the resume window'):
            writer.write(_located_frame([(datetime(2026, 6, 5, 8, tzinfo=UTC), 1)]))

    def test_leaves_staging_outside_the_window_untouched(self, tmp_path: Path) -> None:
        window = DateWindow(
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
        )
        # Staging for a date the window does not cover survives construction.
        outside = tmp_path / 'date=2026-06-05' / 'staging'
        outside.mkdir(parents=True)
        stale_shard = outside / 'shard-stale.shard'
        stale_shard.write_bytes(b'stale')
        WatermarkPartitionedWriter(tmp_path, 'located_at', window)
        assert stale_shard.exists()


class TestSelectWriter:
    def test_returns_snapshot_writer_for_snapshot_single(self, tmp_path: Path) -> None:
        writer = select_writer(_vehicles_definition(SnapshotMode()), tmp_path)
        assert isinstance(writer, SnapshotWriter)

    def test_writes_dataset_at_provider_endpoint_path(self, tmp_path: Path) -> None:
        definition = _vehicles_definition(SnapshotMode())
        writer = select_writer(definition, tmp_path)
        writer.write(pl.DataFrame({'a': [1, 2]}))
        result = writer.finalize()
        written = (
            tmp_path / definition.provider.value / definition.name / 'data.parquet'
        )
        assert written.exists()
        assert result.rows_written == 2
        assert result.files_written == 1

    def test_rejects_a_window_for_snapshot(self, tmp_path: Path) -> None:
        window = DateWindow(
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
        )
        with pytest.raises(ValueError, match='resume window'):
            select_writer(_vehicles_definition(SnapshotMode()), tmp_path, window=window)

    def test_raises_for_an_unbuilt_cell(self, tmp_path: Path) -> None:
        with pytest.raises(NotImplementedError):
            select_writer(_vehicles_definition(FeedMode()), tmp_path)

    def test_returns_watermark_partitioned_writer(self, tmp_path: Path) -> None:
        window = DateWindow(
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
        )
        writer = select_writer(
            _partitioned_watermark_definition(), tmp_path, window=window
        )
        assert isinstance(writer, WatermarkPartitionedWriter)

    def test_rejects_missing_window_for_partitioned_watermark(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(ValueError, match='resume window'):
            select_writer(_partitioned_watermark_definition(), tmp_path)
