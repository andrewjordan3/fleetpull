"""Tests for fleetpull.storage.metadata."""

import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from fleetpull.storage.metadata import (
    MetadataSnapshot,
    render_metadata_json,
    write_metadata_json,
)

# The null-window, cursorless base every render test varies from.
_BASE_SNAPSHOT = MetadataSnapshot(
    provider='motive',
    endpoint='vehicles',
    sync_mode='snapshot',
    generated_at=datetime(2026, 6, 16, 12, 30, tzinfo=UTC),
    records_fetched=5,
    rows_written=5,
    duplicates_dropped=0,
    files_written=1,
    deleted_partitions=(),
    window_start=None,
    window_end=None,
    cursor_kind=None,
    cursor_value=None,
)


class TestRender:
    def test_null_window_snapshot_shape(self) -> None:
        document = json.loads(render_metadata_json(_BASE_SNAPSHOT))
        assert document == {
            'schema_version': 1,
            'provider': 'motive',
            'endpoint': 'vehicles',
            'sync_mode': 'snapshot',
            'generated_at': '2026-06-16T12:30:00Z',
            'last_run': {
                'records_fetched': 5,
                'rows_written': 5,
                'duplicates_dropped': 0,
                'files_written': 1,
                'deleted_partitions': [],
                'window_start': None,
                'window_end': None,
            },
            'cursor': None,
        }

    def test_date_watermark_cursor_arm_with_window(self) -> None:
        snapshot = replace(
            _BASE_SNAPSHOT,
            endpoint='locations',
            sync_mode='watermark',
            deleted_partitions=(date(2026, 6, 12), date(2026, 6, 13)),
            window_start=datetime(2026, 6, 12, tzinfo=UTC),
            window_end=datetime(2026, 6, 15, tzinfo=UTC),
            cursor_kind='date_watermark',
            cursor_value='2026-06-14T10:00:00Z',
        )
        document = json.loads(render_metadata_json(snapshot))
        assert document['sync_mode'] == 'watermark'
        assert document['last_run']['deleted_partitions'] == [
            '2026-06-12',
            '2026-06-13',
        ]
        assert document['last_run']['window_start'] == '2026-06-12T00:00:00Z'
        assert document['last_run']['window_end'] == '2026-06-15T00:00:00Z'
        assert document['cursor'] == {
            'kind': 'date_watermark',
            'value': '2026-06-14T10:00:00Z',
        }

    def test_feed_token_cursor_arm(self) -> None:
        snapshot = replace(
            _BASE_SNAPSHOT,
            sync_mode='feed',
            cursor_kind='feed_token',
            cursor_value='v42',
        )
        document = json.loads(render_metadata_json(snapshot))
        assert document['cursor'] == {'kind': 'feed_token', 'value': 'v42'}

    def test_render_ends_with_a_trailing_newline(self) -> None:
        assert render_metadata_json(_BASE_SNAPSHOT).endswith('}\n')


class TestWrite:
    def test_writes_the_text_to_metadata_json(self, tmp_path: Path) -> None:
        write_metadata_json(tmp_path, '{"schema_version": 1}\n')
        target = tmp_path / 'metadata.json'
        assert target.read_text(encoding='utf-8') == '{"schema_version": 1}\n'

    def test_replaces_an_existing_file(self, tmp_path: Path) -> None:
        write_metadata_json(tmp_path, 'first\n')
        write_metadata_json(tmp_path, 'second\n')
        assert (tmp_path / 'metadata.json').read_text(encoding='utf-8') == 'second\n'

    def test_leaves_no_temp_residue(self, tmp_path: Path) -> None:
        write_metadata_json(tmp_path, 'text\n')
        assert [entry.name for entry in tmp_path.iterdir()] == ['metadata.json']

    def test_missing_endpoint_directory_raises_oserror(self, tmp_path: Path) -> None:
        # No mkdir by design: an absent endpoint directory means no data ever
        # landed -- an upstream bug that must surface, not be papered over.
        with pytest.raises(OSError, match='No such file'):
            write_metadata_json(tmp_path / 'absent', 'text\n')
