"""Tests for fleetpull.orchestrator.bisection.

The fake client simulates the captured provider behavior the driver
exists for: OVERLAP-matched retrieval (a window returns every record
whose interval intersects it) under a silent record cap (at most
``limit`` records, no continuation signal). The driver must recover the
complete, exactly-once record set from that surface or fail loudly.
"""

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from fleetpull.config import GeotabConfig
from fleetpull.endpoints.geotab.exception_events import build_endpoint
from fleetpull.endpoints.shared import (
    BisectedWindowFetch,
    EndpointDefinition,
    ResumeValue,
    StorageKind,
    WatermarkMode,
)
from fleetpull.exceptions import ProviderResponseError
from fleetpull.incremental import DateWindow
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.network.contract import HttpMethod, PageDecoder, RequestSpec
from fleetpull.network.decoders import SinglePageDecoder
from fleetpull.orchestrator.bisection import BisectingWindowDriver
from fleetpull.orchestrator.drivers import SingleRequestDriver
from fleetpull.orchestrator.entry import RosterMachinery, _resolve_driver
from fleetpull.orchestrator.shape_resolution import FetchPoolSource
from fleetpull.vocabulary import JsonObject, Provider, QuotaScope

_LIMIT = 3
_FLOOR = timedelta(minutes=1)


class _Event(ResponseModel):
    """The minimal date-like model the binding validation needs."""

    active_from: datetime | None = None


@dataclass(frozen=True, slots=True)
class _WindowEchoSpecBuilder:
    """Write the resume window into the spec params for the fake to read."""

    def build_spec(
        self, resume: ResumeValue, member_values: Mapping[str, str]
    ) -> RequestSpec:
        assert isinstance(resume, DateWindow)
        return RequestSpec(
            method=HttpMethod.GET,
            url='https://provider.example.test/events',
            params={
                'from': resume.start.isoformat(),
                'to': resume.end.isoformat(),
            },
        )


def _event(start: datetime, end: datetime) -> JsonObject:
    return {
        'activeFrom': start.isoformat().replace('+00:00', 'Z'),
        'activeTo': end.isoformat().replace('+00:00', 'Z'),
    }


class _OverlapCappedClient(TransportClient):
    """Serve overlap-matched records under a silent cap, per request."""

    def __init__(self, dataset: list[JsonObject], limit: int) -> None:
        self._dataset = dataset
        self._limit = limit
        self.requested_windows: list[tuple[datetime, datetime]] = []

    def fetch_pages(
        self, spec: RequestSpec, page_decoder: PageDecoder, quota_scope: str
    ) -> Iterator[FetchedPage]:
        assert spec.params is not None
        window_start = datetime.fromisoformat(spec.params['from'])
        window_end = datetime.fromisoformat(spec.params['to'])
        self.requested_windows.append((window_start, window_end))
        overlapping = [
            record
            for record in self._dataset
            if self._overlaps(record, window_start, window_end)
        ]
        yield FetchedPage(records=overlapping[: self._limit], durable_progress=None)

    @staticmethod
    def _overlaps(
        record: JsonObject, window_start: datetime, window_end: datetime
    ) -> bool:
        record_start = datetime.fromisoformat(str(record['activeFrom']))
        record_end = datetime.fromisoformat(str(record['activeTo']))
        return record_start < window_end and record_end > window_start


def _definition() -> EndpointDefinition[_Event]:
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='synthetic_bisected',
        spec_builder=_WindowEchoSpecBuilder(),
        page_decoder=SinglePageDecoder(records_key='result'),
        response_model=_Event,
        quota_scope=QuotaScope.GEOTAB_GET,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(0)),
        event_time_column='active_from',
        request_shape=BisectedWindowFetch(
            results_limit=_LIMIT,
            floor=_FLOOR,
            event_time_wire_key='activeFrom',
        ),
    )


def _driver() -> BisectingWindowDriver:
    definition = _definition()
    assert isinstance(definition.request_shape, BisectedWindowFetch)
    return BisectingWindowDriver(shape=definition.request_shape)


def _at(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 7, 6, hour, minute, second, tzinfo=UTC)


def _window(start: datetime, end: datetime) -> DateWindow:
    return DateWindow(start=start, end=end)


class TestBisectingWindowDriver:
    def test_a_sparse_window_fetches_once(self) -> None:
        dataset = [
            _event(_at(13, 10), _at(13, 20)),
            _event(_at(13, 30), _at(13, 40)),
        ]
        client = _OverlapCappedClient(dataset, _LIMIT)
        batches = list(
            _driver().record_batches(_definition(), client, _window(_at(13), _at(14)))
        )
        assert len(batches) == 1
        assert batches[0].records == dataset
        assert len(client.requested_windows) == 1

    def test_a_sparse_window_narrates_no_bisection(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        client = _OverlapCappedClient([_event(_at(13, 10), _at(13, 20))], _LIMIT)
        with caplog.at_level('INFO', logger='fleetpull.orchestrator.bisection'):
            list(
                _driver().record_batches(
                    _definition(), client, _window(_at(13), _at(14))
                )
            )
        assert not any(
            'bisection complete' in record.message for record in caplog.records
        )

    def test_overflow_narrates_the_unit_summary(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        dataset = [
            _event(_at(13, 5), _at(13, 10)),
            _event(_at(13, 20), _at(13, 25)),
            _event(_at(14, 5), _at(14, 10)),
            _event(_at(14, 20), _at(14, 25)),
        ]
        client = _OverlapCappedClient(dataset, _LIMIT)
        with caplog.at_level('INFO', logger='fleetpull.orchestrator.bisection'):
            list(
                _driver().record_batches(
                    _definition(), client, _window(_at(13), _at(15))
                )
            )
        summaries = [
            record
            for record in caplog.records
            if 'bisection complete' in record.getMessage()
        ]
        assert len(summaries) == 1
        assert summaries[0].levelname == 'INFO'
        assert 'leaves=2' in summaries[0].getMessage()
        assert 'overflows=1' in summaries[0].getMessage()

    def test_overflow_splits_and_recovers_every_record(self) -> None:
        # Four events across two hours: the whole window overflows (the
        # cap serves only three), each half serves two -- the driver must
        # recover all four, left half first.
        dataset = [
            _event(_at(13, 5), _at(13, 10)),
            _event(_at(13, 20), _at(13, 25)),
            _event(_at(14, 5), _at(14, 10)),
            _event(_at(14, 20), _at(14, 25)),
        ]
        client = _OverlapCappedClient(dataset, _LIMIT)
        batches = list(
            _driver().record_batches(_definition(), client, _window(_at(13), _at(15)))
        )
        assert len(batches) == 2
        assert batches[0].records == dataset[:2]
        assert batches[1].records == dataset[2:]
        # One overflowed probe plus two leaves.
        assert len(client.requested_windows) == 3

    def test_a_split_straddler_lands_in_exactly_one_leaf(self) -> None:
        # The third event's interval crosses the 14:00 split boundary, so
        # overlap retrieval returns it from BOTH halves — and its presence
        # makes the left half serve exactly the cap, forcing a second
        # split there. Its anchor (activeFrom 13:50) owns it to the
        # left-right leaf alone; the right half fetches it and drops it.
        dataset = [
            _event(_at(13, 5), _at(13, 10)),
            _event(_at(13, 20), _at(13, 25)),
            _event(_at(13, 50), _at(14, 10)),
            _event(_at(14, 20), _at(14, 25)),
        ]
        client = _OverlapCappedClient(dataset, _LIMIT)
        batches = list(
            _driver().record_batches(_definition(), client, _window(_at(13), _at(15)))
        )
        assert [batch.records for batch in batches] == [
            [dataset[0], dataset[1]],
            [dataset[2]],
            [dataset[3]],
        ]
        yielded = [record for batch in batches for record in batch.records]
        assert yielded.count(dataset[2]) == 1

    def test_overlap_edge_returns_outside_the_unit_window_drop(self) -> None:
        # An event anchored before the unit window but overlapping into it
        # is returned by overlap retrieval; anchoring drops it -- the
        # neighboring unit owns it.
        dataset = [
            _event(_at(12, 50), _at(13, 10)),
            _event(_at(13, 20), _at(13, 25)),
        ]
        client = _OverlapCappedClient(dataset, _LIMIT)
        batches = list(
            _driver().record_batches(_definition(), client, _window(_at(13), _at(14)))
        )
        assert batches[0].records == [dataset[1]]

    def test_a_full_floor_window_fails_loudly(self) -> None:
        # Three events overlapping one instant: no window width resolves
        # them under a cap of three, so recursion reaches the floor and
        # raises rather than degrading silently.
        dataset = [
            _event(_at(13, 0), _at(13, 30)),
            _event(_at(13, 0), _at(13, 30)),
            _event(_at(13, 1), _at(13, 29)),
        ]
        client = _OverlapCappedClient(dataset, _LIMIT)
        with pytest.raises(ProviderResponseError) as raised:
            list(
                _driver().record_batches(
                    _definition(), client, _window(_at(13), _at(14))
                )
            )
        assert 'cannot be narrowed' in str(raised.value)

    def test_midpoints_stay_whole_second(self) -> None:
        # An odd-width window must never produce fractional-second
        # sub-window bounds (unprobed on the wire).
        dataset = [
            _event(_at(13, 0, 2), _at(13, 0, 10)),
            _event(_at(13, 1, 0), _at(13, 1, 10)),
            _event(_at(13, 2, 0), _at(13, 2, 10)),
            _event(_at(13, 3, 0), _at(13, 3, 10)),
        ]
        client = _OverlapCappedClient(dataset, _LIMIT)
        list(
            _driver().record_batches(
                _definition(),
                client,
                _window(_at(13, 0, 1), _at(13, 3, 12)),
            )
        )
        for window_start, window_end in client.requested_windows:
            assert window_start.microsecond == 0
            assert window_end.microsecond == 0

    def test_requires_a_date_window(self) -> None:
        client = _OverlapCappedClient([], _LIMIT)
        with pytest.raises(TypeError):
            list(_driver().record_batches(_definition(), client, None))

    def test_a_record_without_the_anchor_key_fails_loudly(self) -> None:
        dataset: list[JsonObject] = [{'activeTo': '2026-07-06T13:10:00Z'}]
        client = _AnyWindowClient(dataset)
        with pytest.raises(ProviderResponseError) as raised:
            list(
                _driver().record_batches(
                    _definition(), client, _window(_at(13), _at(14))
                )
            )
        assert 'missing the anchor timestamp' in str(raised.value)

    def test_an_unparseable_anchor_fails_loudly(self) -> None:
        dataset: list[JsonObject] = [{'activeFrom': 'not-a-timestamp'}]
        client = _AnyWindowClient(dataset)
        with pytest.raises(ProviderResponseError) as raised:
            list(
                _driver().record_batches(
                    _definition(), client, _window(_at(13), _at(14))
                )
            )
        assert 'unparseable anchor timestamp' in str(raised.value)


class _AnyWindowClient(TransportClient):
    """Serve a fixed record list for any request (anchor-failure tests)."""

    def __init__(self, records: list[JsonObject]) -> None:
        self._records = records

    def fetch_pages(
        self, spec: RequestSpec, page_decoder: PageDecoder, quota_scope: str
    ) -> Iterator[FetchedPage]:
        yield FetchedPage(records=self._records, durable_progress=None)


class TestDriverRouting:
    def test_a_bisected_shape_routes_to_the_bisecting_driver(self) -> None:
        # The regression this guards is SILENT: mis-routed to the
        # single-fetch driver, a capped endpoint truncates every unit at
        # one page. The roster and pool machinery are never consulted on
        # the bisection path, so inert stand-ins suffice.
        definition = build_endpoint(GeotabConfig())
        driver = _resolve_driver(
            definition,
            cast(RosterMachinery, object()),
            cast(FetchPoolSource, object()),
        )
        assert isinstance(driver, BisectingWindowDriver)
        assert driver.shape is definition.request_shape

    def test_an_undeclared_definition_still_routes_single_fetch(self) -> None:
        undeclared = EndpointDefinition(
            provider=Provider.GEOTAB,
            name='synthetic_plain',
            spec_builder=_WindowEchoSpecBuilder(),
            page_decoder=SinglePageDecoder(records_key='result'),
            response_model=_Event,
            quota_scope=QuotaScope.GEOTAB_GET,
            storage_kind=StorageKind.DATE_PARTITIONED,
            sync_mode=WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(0)),
            event_time_column='active_from',
        )
        driver = _resolve_driver(
            undeclared,
            cast(RosterMachinery, object()),
            cast(FetchPoolSource, object()),
        )
        assert isinstance(driver, SingleRequestDriver)
