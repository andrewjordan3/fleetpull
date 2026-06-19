"""Tests for fleetpull.storage.persist."""

from datetime import timedelta
from pathlib import Path

import polars as pl
import pytest

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    FeedMode,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.models.motive import Vehicle
from fleetpull.network.decoders import MotiveWrappedListPageDecoder
from fleetpull.storage.layout import SingleFileLayout
from fleetpull.storage.merge import merge_snapshot
from fleetpull.storage.persist import _select_layout, _select_merge, persist
from fleetpull.vocabulary import Provider, QuotaScope


def _snapshot_single_definition() -> EndpointDefinition[Vehicle]:
    """A real snapshot + single EndpointDefinition for the happy-path test.

    persist reads only provider / name / storage_kind / sync_mode; the other
    fields exist only to satisfy the binding. The page decoder is the real
    MotiveWrappedListPageDecoder the vehicles binding uses.
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
        sync_mode=SnapshotMode(),
    )


def test_writes_dataset_at_provider_endpoint_path(tmp_path: Path) -> None:
    definition = _snapshot_single_definition()
    result = persist(definition, pl.DataFrame({'a': [1, 2]}), tmp_path)
    written = tmp_path / definition.provider.value / definition.name / 'data.parquet'
    assert written.exists()
    assert result.rows_written == 2
    assert result.files_written == 1


def test_select_merge_returns_snapshot_arm() -> None:
    assert _select_merge(SnapshotMode()) is merge_snapshot


def test_select_merge_raises_for_watermark() -> None:
    with pytest.raises(NotImplementedError):
        _select_merge(WatermarkMode(lookback=timedelta(hours=1)))


def test_select_merge_raises_for_feed() -> None:
    with pytest.raises(NotImplementedError):
        _select_merge(FeedMode())


def test_select_layout_returns_single_arm() -> None:
    assert isinstance(_select_layout(StorageKind.SINGLE), SingleFileLayout)


def test_select_layout_raises_for_partitioned() -> None:
    with pytest.raises(NotImplementedError):
        _select_layout(StorageKind.DATE_PARTITIONED)
