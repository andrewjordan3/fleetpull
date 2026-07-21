"""Tests for fleetpull.endpoints.shared.base."""

import dataclasses
from collections.abc import Mapping
from datetime import datetime, timedelta

import pytest

from fleetpull.endpoints.shared import (
    BatchedRosterFanOut,
    BisectedWindowFetch,
    CompletenessCheck,
    EndpointDefinition,
    FeedMode,
    ParamSweep,
    RequestShape,
    ResumeValue,
    RosterFanOut,
    SingleFetch,
    SnapshotMode,
    StorageKind,
    SyncMode,
    WatermarkMode,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import TransportClient
from fleetpull.network.contract import DecodedPage, HttpMethod, PageAdvance, RequestSpec
from fleetpull.roster import RosterKey
from fleetpull.vocabulary import JsonValue, Provider, QuotaScope


class _StubSpecBuilder:
    """A SpecBuilder double returning a fixed first request."""

    def build_spec(
        self, resume: ResumeValue, member_values: Mapping[str, str]
    ) -> RequestSpec:
        return RequestSpec(method=HttpMethod.GET, url='https://example.test/v1/items')


class _StubPageDecoder:
    """A PageDecoder double that returns one empty page."""

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        return spec

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        return DecodedPage(
            records=[], advance=PageAdvance(next_spec=None, durable_progress=None)
        )


class _StubModel(ResponseModel):
    name: str
    occurred_at: datetime
    maybe_at: datetime | None = None


# A shared frozen marker for the helper's default (B008: no call in defaults).
_SINGLE_FETCH = SingleFetch()


class _StubCompletenessCheck:
    """A CompletenessCheck double returning a fixed count."""

    def expected_count(self, client: TransportClient, quota_scope: str) -> int:
        return 0


def _make_endpoint(
    sync_mode: SyncMode,
    *,
    storage_kind: StorageKind = StorageKind.SINGLE,
    event_time_column: str | None = None,
    request_shape: RequestShape = _SINGLE_FETCH,
    completeness_check: CompletenessCheck | None = None,
) -> EndpointDefinition[_StubModel]:
    """Build an EndpointDefinition from the stubs and the given axes."""
    return EndpointDefinition(
        provider=Provider.SAMSARA,
        name='trips',
        spec_builder=_StubSpecBuilder(),
        page_decoder=_StubPageDecoder(),
        response_model=_StubModel,
        quota_scope=QuotaScope.SAMSARA,
        storage_kind=storage_kind,
        sync_mode=sync_mode,
        event_time_column=event_time_column,
        request_shape=request_shape,
        completeness_check=completeness_check,
    )


def _make_feed_endpoint(
    *,
    request_shape: RequestShape = _SINGLE_FETCH,
    completeness_check: CompletenessCheck | None = None,
) -> EndpointDefinition[_StubModel]:
    """A validly-paired feed endpoint: FeedMode + APPEND_LOG + event time."""
    return _make_endpoint(
        FeedMode(),
        storage_kind=StorageKind.APPEND_LOG,
        event_time_column='occurred_at',
        request_shape=request_shape,
        completeness_check=completeness_check,
    )


class TestEndpointDefinition:
    def test_constructs_and_reads_back_fields(self) -> None:
        endpoint = _make_endpoint(
            WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=2)),
            event_time_column='occurred_at',
        )
        assert endpoint.provider == Provider.SAMSARA
        assert endpoint.name == 'trips'
        assert endpoint.response_model is _StubModel
        assert endpoint.quota_scope == QuotaScope.SAMSARA
        assert endpoint.storage_kind == StorageKind.SINGLE
        assert endpoint.sync_mode == WatermarkMode(
            lookback=timedelta(days=1), cutoff=timedelta(days=2)
        )
        assert endpoint.event_time_column == 'occurred_at'

    def test_accepts_a_feed_mode(self) -> None:
        endpoint = _make_feed_endpoint()
        assert endpoint.sync_mode == FeedMode()
        assert endpoint.storage_kind == StorageKind.APPEND_LOG

    def test_accepts_a_snapshot_mode(self) -> None:
        endpoint = _make_endpoint(SnapshotMode())
        assert endpoint.sync_mode == SnapshotMode()

    def test_is_frozen(self) -> None:
        endpoint = _make_feed_endpoint()
        with pytest.raises(dataclasses.FrozenInstanceError):
            endpoint.name = 'other'  # type: ignore[misc]

    def test_has_slots_no_dict(self) -> None:
        assert not hasattr(_make_feed_endpoint(), '__dict__')

    def test_constructs_with_a_roster_fan_out(self) -> None:
        shape = RosterFanOut(
            roster=RosterKey(Provider.SAMSARA, 'vehicle_ids'),
            member_key='vehicle_id',
        )
        endpoint = _make_endpoint(SnapshotMode(), request_shape=shape)
        assert endpoint.request_shape == shape

    def test_request_shape_defaults_to_single_fetch(self) -> None:
        # Constructed WITHOUT the field: single-chain leaves stay undeclared.
        endpoint = EndpointDefinition(
            provider=Provider.SAMSARA,
            name='trips',
            spec_builder=_StubSpecBuilder(),
            page_decoder=_StubPageDecoder(),
            response_model=_StubModel,
            quota_scope=QuotaScope.SAMSARA,
            storage_kind=StorageKind.SINGLE,
            sync_mode=SnapshotMode(),
        )
        assert endpoint.request_shape == SingleFetch()


class TestSyncMode:
    def test_watermark_mode_holds_lookback(self) -> None:
        assert WatermarkMode(
            lookback=timedelta(hours=6), cutoff=timedelta(days=2)
        ).lookback == timedelta(hours=6)

    def test_watermark_mode_is_frozen_and_slotted(self) -> None:
        mode = WatermarkMode(lookback=timedelta(hours=6), cutoff=timedelta(days=2))
        assert not hasattr(mode, '__dict__')
        with pytest.raises(dataclasses.FrozenInstanceError):
            mode.lookback = timedelta(0)  # type: ignore[misc]

    def test_watermark_mode_fixed_unit_days_defaults_to_none(self) -> None:
        # None is the ordinary endpoint: the planner tiles at
        # sync.backfill_chunk_days.
        mode = WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=0))
        assert mode.fixed_unit_days is None

    def test_watermark_mode_holds_a_declared_fixed_unit_width(self) -> None:
        # The window-grain rollup declaration: the unit width is part
        # of the row's meaning, so it rides the mode, not config.
        mode = WatermarkMode(
            lookback=timedelta(days=1),
            cutoff=timedelta(days=0),
            fixed_unit_days=1,
        )
        assert mode.fixed_unit_days == 1

    @pytest.mark.parametrize('invalid_width', [0, -1, -7])
    def test_watermark_mode_rejects_a_non_positive_fixed_unit_width(
        self, invalid_width: int
    ) -> None:
        # A zero-or-negative unit width can tile nothing -- rejected at
        # declaration, not discovered mid-plan.
        with pytest.raises(ValueError, match='fixed_unit_days'):
            WatermarkMode(
                lookback=timedelta(days=1),
                cutoff=timedelta(days=0),
                fixed_unit_days=invalid_width,
            )

    def test_feed_mode_is_slotted_and_equal(self) -> None:
        mode = FeedMode()
        assert not hasattr(mode, '__dict__')
        assert mode == FeedMode()

    def test_snapshot_mode_is_slotted_and_equal(self) -> None:
        mode = SnapshotMode()
        assert not hasattr(mode, '__dict__')
        assert mode == SnapshotMode()


class TestStorageKind:
    def test_is_str_enum(self) -> None:
        assert issubclass(StorageKind, str)

    def test_member_values(self) -> None:
        assert StorageKind.SINGLE.value == 'single'
        assert StorageKind.DATE_PARTITIONED.value == 'date_partitioned'
        assert StorageKind.APPEND_LOG.value == 'append_log'


class TestEndpointDefinitionValidation:
    def test_snapshot_with_date_partitioned_raises(self) -> None:
        with pytest.raises(ValueError, match='SINGLE'):
            _make_endpoint(SnapshotMode(), storage_kind=StorageKind.DATE_PARTITIONED)

    def test_snapshot_with_event_time_column_raises(self) -> None:
        with pytest.raises(ValueError, match='event_time_column must be None'):
            _make_endpoint(SnapshotMode(), event_time_column='occurred_at')

    def test_watermark_without_event_time_column_raises(self) -> None:
        with pytest.raises(ValueError, match='requires an event_time_column'):
            _make_endpoint(
                WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=2))
            )

    def test_date_partitioned_without_event_time_column_raises(self) -> None:
        with pytest.raises(ValueError, match='requires an event_time_column'):
            _make_endpoint(
                WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=2)),
                storage_kind=StorageKind.DATE_PARTITIONED,
            )

    def test_append_log_without_event_time_column_raises(self) -> None:
        # The feed cell routes each record into its event date's partition,
        # so a feed endpoint with no event-time column cannot write at all.
        with pytest.raises(ValueError, match='requires an event_time_column'):
            _make_endpoint(FeedMode(), storage_kind=StorageKind.APPEND_LOG)

    def test_feed_with_single_storage_raises(self) -> None:
        # FeedMode requires APPEND_LOG: any other layout would delete or
        # replace what the stored-as-emitted contract must keep.
        with pytest.raises(ValueError, match='FeedMode requires storage_kind'):
            _make_endpoint(FeedMode(), event_time_column='occurred_at')

    def test_feed_with_date_partitioned_storage_raises(self) -> None:
        with pytest.raises(ValueError, match='FeedMode requires storage_kind'):
            _make_endpoint(
                FeedMode(),
                storage_kind=StorageKind.DATE_PARTITIONED,
                event_time_column='occurred_at',
            )

    def test_append_log_with_watermark_mode_raises(self) -> None:
        # The reverse direction of the exclusive pairing: delete-by-window
        # against an accumulate-only layout would never clear its window.
        with pytest.raises(ValueError, match='APPEND_LOG requires FeedMode'):
            _make_endpoint(
                WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=2)),
                storage_kind=StorageKind.APPEND_LOG,
                event_time_column='occurred_at',
            )

    def test_append_log_with_snapshot_mode_raises(self) -> None:
        # Snapshot's SINGLE requirement fires first -- either way, loud.
        with pytest.raises(ValueError, match='SnapshotMode requires'):
            _make_endpoint(SnapshotMode(), storage_kind=StorageKind.APPEND_LOG)

    def test_event_time_column_not_a_field_raises(self) -> None:
        with pytest.raises(ValueError, match='not a field'):
            _make_endpoint(
                WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=2)),
                event_time_column='not_a_field',
            )

    def test_non_date_like_event_time_column_raises(self) -> None:
        with pytest.raises(TypeError, match='date-like'):
            _make_endpoint(
                WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=2)),
                event_time_column='name',
            )

    def test_watermark_single_with_event_time_constructs(self) -> None:
        endpoint = _make_endpoint(
            WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=2)),
            event_time_column='occurred_at',
        )
        assert endpoint.event_time_column == 'occurred_at'

    def test_watermark_date_partitioned_constructs(self) -> None:
        endpoint = _make_endpoint(
            WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=2)),
            storage_kind=StorageKind.DATE_PARTITIONED,
            event_time_column='occurred_at',
        )
        assert endpoint.storage_kind == StorageKind.DATE_PARTITIONED

    def test_nullable_date_like_event_time_constructs(self) -> None:
        endpoint = _make_endpoint(
            WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=2)),
            event_time_column='maybe_at',
        )
        assert endpoint.event_time_column == 'maybe_at'

    def test_snapshot_single_without_event_time_constructs(self) -> None:
        endpoint = _make_endpoint(SnapshotMode())
        assert endpoint.event_time_column is None

    def test_feed_append_log_with_event_time_constructs(self) -> None:
        endpoint = _make_feed_endpoint()
        assert endpoint.event_time_column == 'occurred_at'

    def test_snapshot_single_fetch_with_completeness_check_constructs(self) -> None:
        check = _StubCompletenessCheck()
        endpoint = _make_endpoint(SnapshotMode(), completeness_check=check)
        assert endpoint.completeness_check is check

    def test_completeness_check_defaults_to_none(self) -> None:
        assert _make_endpoint(SnapshotMode()).completeness_check is None

    def test_non_snapshot_with_completeness_check_raises(self) -> None:
        with pytest.raises(ValueError, match='requires SnapshotMode'):
            _make_feed_endpoint(completeness_check=_StubCompletenessCheck())

    def test_roster_fan_out_with_completeness_check_raises(self) -> None:
        # The wiring-error rejection: a fan-out run is per-member, never
        # the one complete listing an expected count describes.
        with pytest.raises(ValueError, match='requires the SingleFetch'):
            _make_endpoint(
                SnapshotMode(),
                request_shape=RosterFanOut(
                    roster=RosterKey(Provider.SAMSARA, 'trip_ids'),
                    member_key='trip_id',
                ),
                completeness_check=_StubCompletenessCheck(),
            )

    def test_param_sweep_with_completeness_check_raises(self) -> None:
        # A sweep run is per-value, so the same per-member rejection applies.
        with pytest.raises(ValueError, match='requires the SingleFetch'):
            _make_endpoint(
                SnapshotMode(),
                request_shape=ParamSweep(param='status', values=('active',)),
                completeness_check=_StubCompletenessCheck(),
            )

    def test_snapshot_param_sweep_constructs(self) -> None:
        sweep = ParamSweep(param='status', values=('active', 'inactive'))
        endpoint = _make_endpoint(SnapshotMode(), request_shape=sweep)
        assert endpoint.request_shape == sweep

    def test_watermark_param_sweep_raises(self) -> None:
        # Windowed sweep composition is unprobed against any provider --
        # rejected loudly until a real consumer proves it (the reopen note).
        with pytest.raises(ValueError, match='ParamSweep requires SnapshotMode'):
            _make_endpoint(
                WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=2)),
                event_time_column='occurred_at',
                request_shape=ParamSweep(param='status', values=('active',)),
            )

    def test_feed_param_sweep_raises(self) -> None:
        with pytest.raises(ValueError, match='ParamSweep requires SnapshotMode'):
            _make_feed_endpoint(
                request_shape=ParamSweep(param='status', values=('active',))
            )

    def test_cross_provider_roster_fan_out_raises(self) -> None:
        # Provider-parallel Sync's queue independence rests on rosters
        # never crossing providers (§7); the pairing is rejected at
        # construction so the invariant cannot erode silently.
        with pytest.raises(ValueError, match='crosses the provider boundary'):
            _make_endpoint(
                WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=2)),
                storage_kind=StorageKind.DATE_PARTITIONED,
                event_time_column='occurred_at',
                request_shape=RosterFanOut(
                    roster=RosterKey(Provider.GEOTAB, 'vehicle_ids'),
                    member_key='vehicle_id',
                ),
            )

    def test_cross_provider_batched_roster_fan_out_raises(self) -> None:
        # The batched shape carries the same provider-boundary invariant
        # as its per-member sibling; both arms of the guard stay pinned.
        with pytest.raises(ValueError, match='crosses the provider boundary'):
            _make_endpoint(
                WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=2)),
                storage_kind=StorageKind.DATE_PARTITIONED,
                event_time_column='occurred_at',
                request_shape=BatchedRosterFanOut(
                    roster=RosterKey(Provider.GEOTAB, 'vehicle_ids'),
                    member_key='ids',
                    batch_size=50,
                ),
            )

    def test_watermark_partitioned_bisected_constructs(self) -> None:
        shape = BisectedWindowFetch(
            results_limit=100, floor=timedelta(minutes=1), event_time_wire_key='at'
        )
        endpoint = _make_endpoint(
            WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=2)),
            storage_kind=StorageKind.DATE_PARTITIONED,
            event_time_column='occurred_at',
            request_shape=shape,
        )
        assert endpoint.request_shape == shape

    def test_snapshot_bisected_window_fetch_raises(self) -> None:
        # Bisection recursively narrows a resume window; a snapshot has none.
        with pytest.raises(ValueError, match='BisectedWindowFetch requires'):
            _make_endpoint(
                SnapshotMode(),
                request_shape=BisectedWindowFetch(
                    results_limit=100,
                    floor=timedelta(minutes=1),
                    event_time_wire_key='at',
                ),
            )

    def test_feed_bisected_window_fetch_raises(self) -> None:
        # The overflow signal halves a resume window; a feed resumes from a
        # token and carries no window to halve.
        with pytest.raises(ValueError, match='BisectedWindowFetch requires'):
            _make_feed_endpoint(
                request_shape=BisectedWindowFetch(
                    results_limit=100,
                    floor=timedelta(minutes=1),
                    event_time_wire_key='at',
                )
            )

    def test_single_storage_bisected_window_fetch_raises(self) -> None:
        # The leaf ownership filter routes records to date partitions; a
        # SINGLE layout has none to route to.
        with pytest.raises(ValueError, match='BisectedWindowFetch requires'):
            _make_endpoint(
                WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=2)),
                event_time_column='occurred_at',
                request_shape=BisectedWindowFetch(
                    results_limit=100,
                    floor=timedelta(minutes=1),
                    event_time_wire_key='at',
                ),
            )
