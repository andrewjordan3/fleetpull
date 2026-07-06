"""Tests for fleetpull.orchestrator.drivers."""

from collections.abc import Iterator, Mapping
from datetime import UTC, datetime

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    ResumeValue,
    SnapshotMode,
    SpecBuilder,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.incremental import DateWindow
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.network.contract import (
    DecodedPage,
    HttpMethod,
    PageAdvance,
    PageDecoder,
    RequestSpec,
)
from fleetpull.orchestrator.drivers import FanOutRequestDriver, SingleRequestDriver
from fleetpull.vocabulary import JsonObject, JsonValue, Provider, QuotaScope

_PLACEHOLDER = 'vehicle_id'


class _StubModel(ResponseModel):
    id: int
    name: str


class _StubPageDecoder:
    """A PageDecoder double; the stub client bypasses it, so it is never called."""

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        return spec

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        return DecodedPage(
            records=[], advance=PageAdvance(next_spec=None, durable_progress=None)
        )


class _StubClient(TransportClient):
    """Yields canned pages; opens no real pool (no ``super().__init__``)."""

    def __init__(self, pages: list[FetchedPage]) -> None:
        self._pages = pages

    def fetch_pages(
        self, spec: RequestSpec, page_decoder: PageDecoder, quota_scope: str
    ) -> Iterator[FetchedPage]:
        yield from self._pages


class _SequencedClient(TransportClient):
    """Serves a distinct page list per ``fetch_pages`` call, in order."""

    def __init__(self, pages_per_call: list[list[FetchedPage]]) -> None:
        self._calls = iter(pages_per_call)

    def fetch_pages(
        self, spec: RequestSpec, page_decoder: PageDecoder, quota_scope: str
    ) -> Iterator[FetchedPage]:
        yield from next(self._calls)


class _RecordingSpecBuilder:
    """Captures the ``build_spec`` arguments and returns a fixed request.

    Keeps ``resume`` / ``path_values`` as the most recent call (for the single-chain
    tests) and appends every call to ``calls`` (for the fan-out tests, which build
    one spec per member).
    """

    def __init__(self) -> None:
        self.resume: ResumeValue = None
        self.path_values: Mapping[str, str] | None = None
        self.calls: list[tuple[ResumeValue, Mapping[str, str]]] = []

    def build_spec(
        self, resume: ResumeValue, path_values: Mapping[str, str]
    ) -> RequestSpec:
        self.resume = resume
        self.path_values = path_values
        self.calls.append((resume, path_values))
        return RequestSpec(method=HttpMethod.GET, url='https://example.test/v1/items')


def _page(records: list[JsonObject]) -> FetchedPage:
    return FetchedPage(records=records, durable_progress=None)


def _definition(spec_builder: SpecBuilder) -> EndpointDefinition[_StubModel]:
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='items',
        spec_builder=spec_builder,
        page_decoder=_StubPageDecoder(),
        response_model=_StubModel,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
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


def test_forwards_resume_and_uses_empty_path_values() -> None:
    spec_builder = _RecordingSpecBuilder()
    definition = _definition(spec_builder)
    client = _StubClient([])
    list(SingleRequestDriver().record_batches(definition, client, None))
    assert spec_builder.resume is None
    assert spec_builder.path_values == {}


def test_fan_out_yields_pages_member_by_member() -> None:
    definition = _definition(_RecordingSpecBuilder())
    member_one = [_page([{'id': 1, 'name': 'a'}]), _page([{'id': 2, 'name': 'b'}])]
    member_two = [_page([{'id': 3, 'name': 'c'}]), _page([{'id': 4, 'name': 'd'}])]
    client = _SequencedClient([member_one, member_two])
    driver = FanOutRequestDriver(members=['v1', 'v2'], path_placeholder=_PLACEHOLDER)
    pages = list(driver.record_batches(definition, client, None))
    assert [page.records for page in pages] == [
        [{'id': 1, 'name': 'a'}],
        [{'id': 2, 'name': 'b'}],
        [{'id': 3, 'name': 'c'}],
        [{'id': 4, 'name': 'd'}],
    ]


def test_fan_out_builds_path_values_per_member() -> None:
    spec_builder = _RecordingSpecBuilder()
    definition = _definition(spec_builder)
    client = _SequencedClient([[_page([])], [_page([])]])
    driver = FanOutRequestDriver(members=['v1', 'v2'], path_placeholder=_PLACEHOLDER)
    list(driver.record_batches(definition, client, None))
    assert [path_values for _resume, path_values in spec_builder.calls] == [
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
    driver = FanOutRequestDriver(members=['v1', 'v2'], path_placeholder=_PLACEHOLDER)
    list(driver.record_batches(definition, client, window))
    assert [resume for resume, _path_values in spec_builder.calls] == [window, window]


def test_fan_out_single_member_issues_one_chain() -> None:
    spec_builder = _RecordingSpecBuilder()
    definition = _definition(spec_builder)
    client = _SequencedClient([[_page([{'id': 1, 'name': 'a'}])]])
    driver = FanOutRequestDriver(members=['v1'], path_placeholder=_PLACEHOLDER)
    pages = list(driver.record_batches(definition, client, None))
    assert [page.records for page in pages] == [[{'id': 1, 'name': 'a'}]]
    assert [path_values for _resume, path_values in spec_builder.calls] == [
        {_PLACEHOLDER: 'v1'}
    ]


def test_fan_out_preserves_durable_progress() -> None:
    definition = _definition(_RecordingSpecBuilder())
    record: JsonObject = {'id': 1, 'name': 'a'}
    progressing = FetchedPage(records=[record], durable_progress='v42')
    client = _SequencedClient([[progressing]])
    driver = FanOutRequestDriver(members=['v1'], path_placeholder=_PLACEHOLDER)
    pages = list(driver.record_batches(definition, client, None))
    assert pages[0].durable_progress == 'v42'


def test_fan_out_empty_member_still_yields_a_page() -> None:
    definition = _definition(_RecordingSpecBuilder())
    client = _SequencedClient([[_page([])], [_page([])]])
    driver = FanOutRequestDriver(members=['v1', 'v2'], path_placeholder=_PLACEHOLDER)
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
    driver = FanOutRequestDriver(
        members=['m1', 'm2', 'm3'], path_placeholder=_PLACEHOLDER
    )
    pages = list(driver.record_batches(definition, client, None))
    assert [page.records for page in pages] == [
        [{'id': 1, 'name': 'm1'}],
        [{'id': 2, 'name': 'm2'}],
        [{'id': 3, 'name': 'm3'}],
    ]
