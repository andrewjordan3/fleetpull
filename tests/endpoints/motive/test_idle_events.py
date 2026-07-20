"""Tests for fleetpull.endpoints.motive.idle_events.

The pad tests are the load-bearing ones: idle_events windows are
interpreted on company-local day boundaries and matched by overlap
(DESIGN section 8, captured 2026-07-15), so the wire window widens one
day each side while the true UTC window -- the post-fetch filter and the
writer's partition tripwire -- keeps ownership of every record.
"""

from datetime import UTC, datetime, timedelta

from fleetpull.config import MotiveConfig
from fleetpull.endpoints.motive._spec_builders import MotiveFleetDateRangeSpecBuilder
from fleetpull.endpoints.motive.idle_events import build_endpoint
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SingleFetch,
    StorageKind,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.models.motive import IdleEvent
from fleetpull.network.decoders import MotiveWrappedListPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope


def _window() -> DateWindow:
    return DateWindow(
        start=datetime(2026, 6, 1, tzinfo=UTC),
        end=datetime(2026, 6, 4, tzinfo=UTC),
    )


def _build_endpoint() -> EndpointDefinition[IdleEvent]:
    return build_endpoint(MotiveConfig(base_url='https://api.example.test'))


class TestBuildIdleEventsEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.MOTIVE
        assert endpoint.name == 'idle_events'
        assert endpoint.quota_scope is QuotaScope.MOTIVE
        assert endpoint.storage_kind is StorageKind.DATE_PARTITIONED
        assert endpoint.response_model is IdleEvent
        assert endpoint.event_time_column == 'start_time'
        assert endpoint.request_shape == SingleFetch()
        assert endpoint.completeness_check is None

    def test_watermark_knobs_come_from_config(self) -> None:
        endpoint = build_endpoint(
            MotiveConfig(
                base_url='https://api.example.test',
                lookback_days=2,
                cutoff_days=0,
            )
        )
        assert isinstance(endpoint.sync_mode, WatermarkMode)
        assert endpoint.sync_mode.lookback == timedelta(days=2)
        assert endpoint.sync_mode.cutoff == timedelta(days=0)

    def test_decoder_speaks_the_wrapped_list_wire_shape(self) -> None:
        endpoint = _build_endpoint()
        decoder = endpoint.page_decoder
        assert isinstance(decoder, MotiveWrappedListPageDecoder)
        assert decoder.list_key == 'idle_events'
        assert decoder.item_key == 'idle_event'
        assert decoder.per_page == 100


class TestIdleEventsWindowPad:
    def test_the_wire_window_pads_one_day_each_side(self) -> None:
        # Resume window covers 2026-06-01 through 2026-06-03; the wire
        # window fetches 2026-05-31 through 2026-06-04 so any account
        # timezone's local-day interpretation still covers every record
        # whose UTC start falls in the resume window.
        endpoint = _build_endpoint()
        spec = endpoint.spec_builder.build_spec(resume=_window(), member_values={})
        assert spec.params == {
            'start_date': '2026-05-31',
            'end_date': '2026-06-04',
        }

    def test_the_pad_is_declared_not_computed(self) -> None:
        endpoint = _build_endpoint()
        builder = endpoint.spec_builder
        assert isinstance(builder, MotiveFleetDateRangeSpecBuilder)
        assert builder.window_pad_days == 1
