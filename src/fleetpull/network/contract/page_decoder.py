# src/fleetpull/network/contract/page_decoder.py
"""The page-decoder contract: interpret a response envelope once,
yielding its records and its pagination verdict together.

Supersedes the split ``PaginationStrategy`` + ``RecordExtractor``: a
provider's envelope is parsed in exactly one place, so the records and
the continuation cursor are read from a single validated view rather
than re-interpreted downstream. Implementations live in the sibling
``network/decoders/`` package and reach this surface through the
contract face; like the paginators they deliberately share no concrete
behavior (blast-radius over DRY).
"""

from dataclasses import dataclass
from typing import Protocol

from fleetpull.network.contract.request import JsonObject, JsonValue, RequestSpec

__all__: list[str] = ['DecodedPage', 'PageAdvance', 'PageDecoder']


@dataclass(frozen=True, slots=True)
class PageAdvance:
    """
    A decoder's verdict on one completed page.

    Attributes:
        next_spec: The request for the next page, or None when
            pagination is complete.
        durable_progress: Cursor progress that must outlive the fetch
            (GeoTab's ``toVersion``; the state layer's FeedToken commit
            value). None for providers whose cursors are fetch-private
            (the established inert pattern). Carried on EVERY page
            including the terminal one — the terminal page's value is
            the resume point.
    """

    next_spec: RequestSpec | None
    durable_progress: str | None


@dataclass(frozen=True, slots=True)
class DecodedPage:
    """One decoded page: its records and its pagination verdict.

    Composes ``PageAdvance`` rather than re-declaring ``next_spec`` /
    ``durable_progress`` — the meaning of "next transient request plus
    durable cursor progress" stays defined in exactly one place.

    Attributes:
        records: The page's records, each a JSON object.
        advance: The pagination verdict (continue/complete plus durable
            progress) for the page just decoded.
    """

    records: list[JsonObject]
    advance: PageAdvance


class PageDecoder(Protocol):
    """Per-provider envelope interpreter: records and pagination in one pass.

    A plain Protocol (no ``@runtime_checkable``) — conformance is a
    static obligation checked by mypy at the binding site, not an
    isinstance gate.
    """

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Decorate the base spec for page one.

        Absorbs the former ``PaginationStrategy.first_request`` so that
        pagination configuration (page size, the first-page cursor
        rule) stays inside the decoder and the spec builder stays
        pagination-blind.

        Args:
            spec: The endpoint's base request spec.

        Returns:
            The spec to send for the first page.
        """
        ...

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        """Interpret one response envelope into records and a verdict.

        Args:
            sent: The spec that produced this page; supplies whatever
                the continuation request is derived from.
            envelope: The parsed response body.

        Returns:
            The page's records and its pagination verdict.

        Raises:
            ProviderResponseError: When the envelope's record-bearing or
                pagination shape is structurally violating (the §8
                stance).
        """
        ...
