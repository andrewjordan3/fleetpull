# src/fleetpull/network/decoders/single_page.py
"""Single-page decoder: top-level-list records, no pagination.

For unpaginated endpoints -- replaces any is-paginated flag. The first
request is the base spec unchanged and every page is terminal. Decoder
logic deliberately resembles its siblings without sharing code.
"""

from dataclasses import dataclass

from fleetpull.network.contract import (
    DecodedPage,
    PageAdvance,
    RequestSpec,
    require_record_list,
)
from fleetpull.vocabulary import JsonObject, JsonValue

__all__: list[str] = ['SinglePageDecoder']


@dataclass(frozen=True, slots=True)
class SinglePageDecoder:
    """Decode a single unpaginated page of top-level-list records.

    Attributes:
        records_key: The top-level key holding the record list.
    """

    records_key: str

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Return the base spec unchanged."""
        return spec

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        """Extract the records; the page is always terminal.

        Args:
            sent: The spec that produced this page (unused; there is no
                continuation).
            envelope: The parsed response body.

        Returns:
            The records and a terminal verdict.

        Raises:
            ProviderResponseError: When the record-bearing shape is
                structurally violating.
        """
        records: list[JsonObject] = require_record_list(envelope, self.records_key)
        return DecodedPage(
            records=records,
            advance=PageAdvance(next_spec=None, durable_progress=None),
        )
