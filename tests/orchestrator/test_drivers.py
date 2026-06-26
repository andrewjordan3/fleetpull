"""Tests for fleetpull.orchestrator.drivers."""

from collections.abc import Iterator, Mapping

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    ResumeValue,
    SnapshotMode,
    SpecBuilder,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.network.contract import (
    DecodedPage,
    HttpMethod,
    JsonObject,
    JsonValue,
    PageAdvance,
    PageDecoder,
    RequestSpec,
)
from fleetpull.orchestrator.drivers import SingleRequestDriver
from fleetpull.vocabulary import Provider, QuotaScope


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


class _RecordingSpecBuilder:
    """Captures the ``build_spec`` arguments and returns a fixed request."""

    def __init__(self) -> None:
        self.resume: ResumeValue = None
        self.path_values: Mapping[str, str] | None = None

    def build_spec(
        self, resume: ResumeValue, path_values: Mapping[str, str]
    ) -> RequestSpec:
        self.resume = resume
        self.path_values = path_values
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
    assert batches == [page_a, page_b]


def test_forwards_resume_and_uses_empty_path_values() -> None:
    spec_builder = _RecordingSpecBuilder()
    definition = _definition(spec_builder)
    client = _StubClient([])
    list(SingleRequestDriver().record_batches(definition, client, None))
    assert spec_builder.resume is None
    assert spec_builder.path_values == {}
