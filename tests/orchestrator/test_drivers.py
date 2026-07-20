"""Tests for fleetpull.orchestrator.drivers."""

import logging
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime

import pytest

from fleetpull.endpoints.shared import (
    CompletenessCheck,
    EndpointDefinition,
    ResumeValue,
    SnapshotMode,
    SpecBuilder,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.exceptions import ProviderResponseError
from fleetpull.incremental import DateWindow
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.network.contract import (
    HttpMethod,
    PageDecoder,
    RequestSpec,
)
from fleetpull.orchestrator.drivers import (
    _MEMBER_PROGRESS_INTERVAL,
    FanOutRequestDriver,
    SingleRequestDriver,
)
from fleetpull.orchestrator.fanout import FetchPool
from fleetpull.vocabulary import JsonObject, Provider, QuotaScope
from tests.orchestrator.doubles import StubPageDecoder
from tests.orchestrator.serial_executor import SerialExecutor

_PLACEHOLDER = 'vehicle_id'


def _fan_out(members: list[str]) -> FanOutRequestDriver:
    """A fan-out driver on the deterministic seam: the synchronous executor."""
    return FanOutRequestDriver(
        members=members,
        member_key=_PLACEHOLDER,
        fetch_pool=FetchPool(executor=SerialExecutor(), submission_window=2),
    )


class _StubModel(ResponseModel):
    id: int
    name: str


class _StubClient(TransportClient):
    """Yields canned pages; opens no real pool (no ``super().__init__``)."""

    def __init__(self, pages: list[FetchedPage]) -> None:
        self._pages = pages

    def fetch_pages(
        self, spec: RequestSpec, page_decoder: PageDecoder, quota_scope: str
    ) -> Iterator[FetchedPage]:
        yield from self._pages


class _SequencedClient(TransportClient):
    """Serves a distinct page list per ``fetch_pages`` call, in order.

    Counts calls at call time (not first iteration), so a test can assert
    exactly how many chains were requested.
    """

    def __init__(self, pages_per_call: list[list[FetchedPage]]) -> None:
        self._calls = iter(pages_per_call)
        self.fetch_pages_calls = 0

    def fetch_pages(
        self, spec: RequestSpec, page_decoder: PageDecoder, quota_scope: str
    ) -> Iterator[FetchedPage]:
        self.fetch_pages_calls += 1
        return iter(next(self._calls))


class _RecordingSpecBuilder:
    """Captures the ``build_spec`` arguments and returns a fixed request.

    Keeps ``resume`` / ``member_values`` as the most recent call (for the
    single-chain tests) and appends every call to ``calls`` (for the fan-out
    tests, which build one spec per member).
    """

    def __init__(self) -> None:
        self.resume: ResumeValue = None
        self.member_values: Mapping[str, str] | None = None
        self.calls: list[tuple[ResumeValue, Mapping[str, str]]] = []

    def build_spec(
        self, resume: ResumeValue, member_values: Mapping[str, str]
    ) -> RequestSpec:
        self.resume = resume
        self.member_values = member_values
        self.calls.append((resume, member_values))
        return RequestSpec(method=HttpMethod.GET, url='https://example.test/v1/items')


def _page(records: list[JsonObject]) -> FetchedPage:
    return FetchedPage(records=records, durable_progress=None)


def _definition(
    spec_builder: SpecBuilder,
    completeness_check: CompletenessCheck | None = None,
) -> EndpointDefinition[_StubModel]:
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='items',
        spec_builder=spec_builder,
        page_decoder=StubPageDecoder(),
        response_model=_StubModel,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
        completeness_check=completeness_check,
    )


def _static_builder() -> StaticGetSpecBuilder:
    return StaticGetSpecBuilder(base_url='https://x.test', path='/v1/items')


def test_yields_one_batch_per_page_each_holding_that_pages_records() -> None:
    page_a: list[JsonObject] = [{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}]
    page_b: list[JsonObject] = [{'id': 3, 'name': 'c'}]
    definition = _definition(_static_builder())
    client = _StubClient([_page(page_a), _page(page_b)])
    batches = list(SingleRequestDriver().record_batches(definition, client, None))
    assert [page.records for page in batches] == [page_a, page_b]


def test_forwards_resume_and_uses_empty_member_values() -> None:
    spec_builder = _RecordingSpecBuilder()
    definition = _definition(spec_builder)
    client = _StubClient([])
    list(SingleRequestDriver().record_batches(definition, client, None))
    assert spec_builder.resume is None
    assert spec_builder.member_values == {}


def test_fan_out_yields_pages_member_by_member() -> None:
    definition = _definition(_RecordingSpecBuilder())
    member_one = [_page([{'id': 1, 'name': 'a'}]), _page([{'id': 2, 'name': 'b'}])]
    member_two = [_page([{'id': 3, 'name': 'c'}]), _page([{'id': 4, 'name': 'd'}])]
    client = _SequencedClient([member_one, member_two])
    driver = _fan_out(['v1', 'v2'])
    pages = list(driver.record_batches(definition, client, None))
    assert [page.records for page in pages] == [
        [{'id': 1, 'name': 'a'}],
        [{'id': 2, 'name': 'b'}],
        [{'id': 3, 'name': 'c'}],
        [{'id': 4, 'name': 'd'}],
    ]


def test_fan_out_builds_member_values_per_member() -> None:
    spec_builder = _RecordingSpecBuilder()
    definition = _definition(spec_builder)
    client = _SequencedClient([[_page([])], [_page([])]])
    driver = _fan_out(['v1', 'v2'])
    list(driver.record_batches(definition, client, None))
    assert [member_values for _resume, member_values in spec_builder.calls] == [
        {_PLACEHOLDER: 'v1'},
        {_PLACEHOLDER: 'v2'},
    ]


def test_fan_out_forwards_resume_to_every_member() -> None:
    spec_builder = _RecordingSpecBuilder()
    definition = _definition(spec_builder)
    client = _SequencedClient([[_page([])], [_page([])]])
    window = DateWindow(
        start=datetime(2026, 6, 1, tzinfo=UTC), end=datetime(2026, 6, 2, tzinfo=UTC)
    )
    driver = _fan_out(['v1', 'v2'])
    list(driver.record_batches(definition, client, window))
    assert [resume for resume, _member_values in spec_builder.calls] == [window, window]


def test_fan_out_single_member_issues_one_chain() -> None:
    spec_builder = _RecordingSpecBuilder()
    definition = _definition(spec_builder)
    client = _SequencedClient([[_page([{'id': 1, 'name': 'a'}])]])
    driver = _fan_out(['v1'])
    pages = list(driver.record_batches(definition, client, None))
    assert [page.records for page in pages] == [[{'id': 1, 'name': 'a'}]]
    assert [member_values for _resume, member_values in spec_builder.calls] == [
        {_PLACEHOLDER: 'v1'}
    ]


def test_fan_out_preserves_durable_progress() -> None:
    definition = _definition(_RecordingSpecBuilder())
    record: JsonObject = {'id': 1, 'name': 'a'}
    progressing = FetchedPage(records=[record], durable_progress='v42')
    client = _SequencedClient([[progressing]])
    driver = _fan_out(['v1'])
    pages = list(driver.record_batches(definition, client, None))
    assert pages[0].durable_progress == 'v42'


def test_fan_out_empty_member_still_yields_a_page() -> None:
    definition = _definition(_RecordingSpecBuilder())
    client = _SequencedClient([[_page([])], [_page([])]])
    driver = _fan_out(['v1', 'v2'])
    pages = list(driver.record_batches(definition, client, None))
    assert [page.records for page in pages] == [[], []]


def test_fan_out_preserves_member_order() -> None:
    definition = _definition(_RecordingSpecBuilder())
    client = _SequencedClient(
        [
            [_page([{'id': 1, 'name': 'm1'}])],
            [_page([{'id': 2, 'name': 'm2'}])],
            [_page([{'id': 3, 'name': 'm3'}])],
        ]
    )
    driver = _fan_out(['m1', 'm2', 'm3'])
    pages = list(driver.record_batches(definition, client, None))
    assert [page.records for page in pages] == [
        [{'id': 1, 'name': 'm1'}],
        [{'id': 2, 'name': 'm2'}],
        [{'id': 3, 'name': 'm3'}],
    ]


class TestFanOutNarration:
    """The fan-out's progress narration on the consuming side of the channel.

    Substring assertions on ``getMessage()`` only -- never whole formatted
    lines -- so format tweaks do not break the pins.
    """

    @staticmethod
    def _messages_at(caplog: pytest.LogCaptureFixture, level: int) -> list[str]:
        return [
            log_record.getMessage()
            for log_record in caplog.records
            if log_record.levelno == level
        ]

    def test_member_progress_interval_is_pinned(self) -> None:
        # The heartbeat cadence is settled narration policy (~15 progress
        # lines for a ~1,500-member fleet; DESIGN section 13) -- a silent
        # change should be loud.
        assert _MEMBER_PROGRESS_INTERVAL == 100

    def test_small_fan_out_narrates_members_at_debug_and_completion_at_info(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        definition = _definition(_RecordingSpecBuilder())
        client = _SequencedClient([[_page([])], [_page([])], [_page([])]])
        driver = _fan_out(['v1', 'v2', 'v3'])
        with caplog.at_level(logging.DEBUG, logger='fleetpull.orchestrator.drivers'):
            list(driver.record_batches(definition, client, None))
        member_lines = [
            message
            for message in self._messages_at(caplog, logging.DEBUG)
            if 'fetched member:' in message
        ]
        assert len(member_lines) == 3
        assert 'v1 (1/3)' in member_lines[0]
        assert 'v2 (2/3)' in member_lines[1]
        assert 'v3 (3/3)' in member_lines[2]
        info_messages = self._messages_at(caplog, logging.INFO)
        assert any('fan-out complete:' in m and 'members=3' in m for m in info_messages)
        # Below one interval, no heartbeat fires -- a small fleet narrates
        # at most one progress line, here none.
        assert not any('fan-out progress' in m for m in info_messages)

    def test_heartbeat_fires_every_interval_and_once_on_completion(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        member_total = 2 * _MEMBER_PROGRESS_INTERVAL + 5
        definition = _definition(_RecordingSpecBuilder())
        # The stub serves the same single empty page for every member's chain.
        client = _StubClient([_page([])])
        driver = _fan_out([f'veh-{index}' for index in range(member_total)])
        with caplog.at_level(logging.INFO, logger='fleetpull.orchestrator.drivers'):
            list(driver.record_batches(definition, client, None))
        info_messages = self._messages_at(caplog, logging.INFO)
        progress_lines = [m for m in info_messages if 'fan-out progress' in m]
        assert len(progress_lines) == 2
        assert (
            f'members={_MEMBER_PROGRESS_INTERVAL}/{member_total}' in progress_lines[0]
        )
        assert (
            f'members={2 * _MEMBER_PROGRESS_INTERVAL}/{member_total}'
            in progress_lines[1]
        )
        assert any(
            'fan-out complete:' in m and f'members={member_total}' in m
            for m in info_messages
        )


class _ScriptedCheck:
    """A CompletenessCheck double serving one scripted count per call."""

    def __init__(self, counts: list[int]) -> None:
        self._counts = iter(counts)
        self.scopes_seen: list[str] = []

    def expected_count(self, client: TransportClient, quota_scope: str) -> int:
        self.scopes_seen.append(quota_scope)
        return next(self._counts)


class TestVerifiedHarvest:
    """The single-fetch driver's completeness guard (plant-and-fire):
    stream-then-verify, one harvest, no refetch."""

    def test_match_passes_records_through_untouched(self) -> None:
        page_a: list[JsonObject] = [{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}]
        page_b: list[JsonObject] = [{'id': 3, 'name': 'c'}]
        check = _ScriptedCheck([3])
        definition = _definition(_static_builder(), completeness_check=check)
        client = _SequencedClient([[_page(page_a), _page(page_b)]])
        batches = list(SingleRequestDriver().record_batches(definition, client, None))
        # One check call, on the endpoint's own scope; order preserved.
        assert check.scopes_seen == [QuotaScope.MOTIVE.value]
        assert [page.records for page in batches] == [page_a, page_b]

    def test_mismatch_raises_after_streaming_naming_both_counts(self) -> None:
        harvest = [_page([{'id': 1, 'name': 'a'}]), _page([{'id': 2, 'name': 'b'}])]
        never_requested = [_page([{'id': 9, 'name': 'z'}])]
        check = _ScriptedCheck([5])
        definition = _definition(_static_builder(), completeness_check=check)
        client = _SequencedClient([harvest, never_requested])
        streamed: list[FetchedPage] = []
        batches = SingleRequestDriver().record_batches(definition, client, None)
        # extend() keeps the pages appended before the raise, proving the
        # stream flowed; the raise lands only after the terminal page.
        with pytest.raises(ProviderResponseError) as raised:
            streamed.extend(batches)
        message = str(raised.value)
        assert '5' in message  # the expected count
        assert '2' in message  # the harvested count
        # Every page streamed before the raise -- nothing was buffered back.
        assert [page.records for page in streamed] == [
            [{'id': 1, 'name': 'a'}],
            [{'id': 2, 'name': 'b'}],
        ]
        # Exactly one harvest and one check: a second request is the failure.
        assert client.fetch_pages_calls == 1
        assert check.scopes_seen == [QuotaScope.MOTIVE.value]

    def test_no_check_declared_means_no_buffering_and_no_check_calls(self) -> None:
        # The undeclared path is the pre-guard streaming behavior, untouched.
        page_a: list[JsonObject] = [{'id': 1, 'name': 'a'}]
        definition = _definition(_static_builder())
        client = _StubClient([_page(page_a)])
        batches = list(SingleRequestDriver().record_batches(definition, client, None))
        assert [page.records for page in batches] == [[{'id': 1, 'name': 'a'}]]
