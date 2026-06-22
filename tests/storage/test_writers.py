"""Tests for fleetpull.storage.writers."""

from datetime import UTC, datetime
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
)
from fleetpull.incremental import DateWindow
from fleetpull.models.motive import Vehicle
from fleetpull.network.decoders import MotiveWrappedListPageDecoder
from fleetpull.storage.writers import SnapshotWriter, select_writer
from fleetpull.vocabulary import Provider, QuotaScope


def _frame() -> pl.DataFrame:
    return pl.DataFrame({'a': [1, 2], 'b': ['x', 'y']})


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
