"""Tests for fleetpull.storage.append.

The append-log cell's contract, invariant by invariant: per-write
durability (each ``write`` lands its part files before returning — what
the feed drive's parquet-before-token order stands on), max-plus-one part
numbering, and the I3 tripwire — no append ever touches an existing file
(DESIGN sections 4/14).
"""

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

import fleetpull.storage.atomic as atomic_module
from fleetpull.exceptions import ProviderResponseError
from fleetpull.storage import append as append_module
from fleetpull.storage.append import FeedAppendWriter, _next_part_number

_DAY_ONE = datetime(2026, 7, 1, 12, tzinfo=UTC)
_DAY_TWO = datetime(2026, 7, 2, 3, tzinfo=UTC)


def _frame(*moments: datetime) -> pl.DataFrame:
    return pl.DataFrame(
        {
            'occurred_at': list(moments),
            'value': list(range(len(moments))),
        },
        schema={'occurred_at': pl.Datetime('us', 'UTC'), 'value': pl.Int64},
    )


def _partition(tmp_path: Path, day: str) -> Path:
    return tmp_path / f'date={day}'


class TestNextPartNumber:
    def test_missing_partition_starts_at_one(self, tmp_path: Path) -> None:
        assert _next_part_number(tmp_path / 'date=2026-07-01') == 1

    def test_returns_max_plus_one(self, tmp_path: Path) -> None:
        (tmp_path / 'part-00001.parquet').touch()
        (tmp_path / 'part-00003.parquet').touch()
        assert _next_part_number(tmp_path) == 4

    def test_gaps_never_reuse_a_number(self, tmp_path: Path) -> None:
        # Only part-00002 present: the next part is 3, never the "free" 1 --
        # reusing a gap could shadow a number a crashed run already spent.
        (tmp_path / 'part-00002.parquet').touch()
        assert _next_part_number(tmp_path) == 3

    def test_foreign_part_file_raises(self, tmp_path: Path) -> None:
        (tmp_path / 'part-abc.parquet').touch()
        with pytest.raises(ValueError, match='foreign part file'):
            _next_part_number(tmp_path)

    def test_atomic_write_temp_does_not_skew_the_scan(self, tmp_path: Path) -> None:
        # A crashed atomic write's temp sibling (.part-....tmp) must be
        # invisible to the scan -- it is neither a part nor foreign.
        (tmp_path / 'part-00001.parquet').touch()
        (tmp_path / '.part-00002.parquet.deadbeef.tmp').touch()
        assert _next_part_number(tmp_path) == 2


class TestFeedAppendWriter:
    def test_write_lands_parts_before_returning(self, tmp_path: Path) -> None:
        # Per-write durability: the feed drive commits a page's token right
        # after write() returns, so the bytes must already be on disk.
        writer = FeedAppendWriter(tmp_path, 'occurred_at')
        writer.write(_frame(_DAY_ONE, _DAY_ONE))
        part = _partition(tmp_path, '2026-07-01') / 'part-00001.parquet'
        assert part.exists()
        assert pl.read_parquet(part).height == 2

    def test_rows_route_to_their_event_dates(self, tmp_path: Path) -> None:
        writer = FeedAppendWriter(tmp_path, 'occurred_at')
        writer.write(_frame(_DAY_ONE, _DAY_TWO, _DAY_TWO))
        day_one = _partition(tmp_path, '2026-07-01') / 'part-00001.parquet'
        day_two = _partition(tmp_path, '2026-07-02') / 'part-00001.parquet'
        assert pl.read_parquet(day_one).height == 1
        assert pl.read_parquet(day_two).height == 2

    def test_successive_writes_number_upward(self, tmp_path: Path) -> None:
        writer = FeedAppendWriter(tmp_path, 'occurred_at')
        writer.write(_frame(_DAY_ONE))
        writer.write(_frame(_DAY_ONE))
        partition = _partition(tmp_path, '2026-07-01')
        assert sorted(path.name for path in partition.glob('*.parquet')) == [
            'part-00001.parquet',
            'part-00002.parquet',
        ]

    def test_a_second_run_continues_the_numbering(self, tmp_path: Path) -> None:
        # A fresh writer (the next run) scans the partition and continues --
        # numbering is a property of the partition, not the writer instance.
        FeedAppendWriter(tmp_path, 'occurred_at').write(_frame(_DAY_ONE))
        FeedAppendWriter(tmp_path, 'occurred_at').write(_frame(_DAY_ONE))
        partition = _partition(tmp_path, '2026-07-01')
        assert (partition / 'part-00002.parquet').exists()

    def test_empty_frame_writes_nothing(self, tmp_path: Path) -> None:
        # The at-head empty page: no partitions, no files, no directories.
        writer = FeedAppendWriter(tmp_path, 'occurred_at')
        writer.write(_frame())
        assert list(tmp_path.iterdir()) == []
        result = writer.finalize()
        assert result.rows_written == 0
        assert result.files_written == 0

    def test_finalize_reports_the_accumulated_appends(self, tmp_path: Path) -> None:
        writer = FeedAppendWriter(tmp_path, 'occurred_at')
        writer.write(_frame(_DAY_ONE, _DAY_TWO))
        writer.write(_frame(_DAY_ONE))
        result = writer.finalize()
        assert result.rows_written == 3
        assert result.files_written == 3
        assert result.duplicates_dropped == 0
        assert result.deleted_partitions == ()

    def test_duplicate_rows_are_stored_as_emitted(self, tmp_path: Path) -> None:
        # No write-time dedup: a crash-window refetch or a re-emitted version
        # lands as new rows; reconciliation is the consumer's (DESIGN §4).
        writer = FeedAppendWriter(tmp_path, 'occurred_at')
        writer.write(_frame(_DAY_ONE))
        writer.write(_frame(_DAY_ONE))
        partition = _partition(tmp_path, '2026-07-01')
        combined = pl.read_parquet(partition / 'part-*.parquet')
        assert combined.height == 2

    def test_a_null_event_date_rejects_the_page_whole(self, tmp_path: Path) -> None:
        # A feed record without its partition key is a provider-contract
        # violation surfaced loudly BEFORE any part lands -- never a
        # mid-write stall, never a partial append.
        writer = FeedAppendWriter(tmp_path, 'occurred_at')
        frame = _frame(_DAY_ONE).vstack(
            _frame(_DAY_ONE).with_columns(
                pl.lit(None, dtype=pl.Datetime('us', 'UTC')).alias('occurred_at')
            )
        )
        with pytest.raises(ProviderResponseError, match='null'):
            writer.write(frame)
        assert not any(tmp_path.rglob('part-*.parquet'))

    def test_the_append_write_is_durable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The durable-rename recipe rides every part: the token commit is
        # fsynced (SQLite), so a power loss must never persist a token
        # whose page -- or whose newly created partition directory -- the
        # page cache still held. This first-ever write creates the date=
        # partition directory, so the file, the new directory, AND the
        # dataset root holding its entry all fsync (the durable chain).
        synced: list[Path] = []
        real_fsync_path = atomic_module._fsync_path

        def recording_fsync_path(path: Path) -> None:
            synced.append(path)
            real_fsync_path(path)

        monkeypatch.setattr(atomic_module, '_fsync_path', recording_fsync_path)
        FeedAppendWriter(tmp_path, 'occurred_at').write(_frame(_DAY_ONE))
        assert synced[0].name.endswith('.tmp')
        assert synced[1:] == [tmp_path / 'date=2026-07-01', tmp_path]

    def test_append_never_touches_existing_files(self, tmp_path: Path) -> None:
        # THE I3 TRIPWIRE (DESIGN section 14): a feed run appends new part
        # files and never rewrites or deletes a landed one. Snapshot every
        # byte on disk, append again, and require the prior inventory
        # byte-identical -- any "fix" that makes the append path touch an
        # existing file must consciously break this test.
        writer = FeedAppendWriter(tmp_path, 'occurred_at')
        writer.write(_frame(_DAY_ONE, _DAY_TWO))
        before = {
            path: path.read_bytes() for path in sorted(tmp_path.rglob('*.parquet'))
        }
        writer.write(_frame(_DAY_ONE, _DAY_TWO, _DAY_TWO))
        for path, prior_bytes in before.items():
            assert path.exists(), f'{path} was deleted by an append'
            assert path.read_bytes() == prior_bytes, f'{path} was rewritten'
        after = set(tmp_path.rglob('*.parquet'))
        assert after > set(before), 'the second write appended no new parts'

    def test_part_collision_fails_loudly_instead_of_clobbering(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The production half of I3: if the single-writer assumption is ever
        # violated -- another writer lands the scanned number between the
        # scan and the rename -- the writer refuses rather than overwrite a
        # landed part. The race is recreated by pinning the scan to a number
        # a concurrent writer then takes first.
        partition = _partition(tmp_path, '2026-07-01')
        partition.mkdir(parents=True)
        taken = partition / 'part-00001.parquet'
        taken.write_bytes(b'landed by the other writer')
        monkeypatch.setattr(append_module, '_next_part_number', lambda _dir: 1)
        writer = FeedAppendWriter(tmp_path, 'occurred_at')
        with pytest.raises(RuntimeError, match='part collision'):
            writer.write(_frame(_DAY_ONE))
        assert taken.read_bytes() == b'landed by the other writer'
