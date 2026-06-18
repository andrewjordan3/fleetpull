# src/fleetpull/network/decoders/samsara.py
"""Samsara page decoder: top-level-list records, cursor pagination
(sources: scrubbed provider-behavior verification, June 2026; cursor
contract from provider documentation).

Records arrive as a top-level list under a per-endpoint key; the
``pagination`` block carries ``endCursor``/``hasNextPage``. The first
page sends no ``after``; subsequent pages send ``after=<endCursor>``.
Decoder logic deliberately resembles its siblings without sharing code.
"""

from dataclasses import dataclass
from typing import Final

from pydantic import BaseModel, ConfigDict, Field

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract import (
    DecodedPage,
    JsonValue,
    PageAdvance,
    RequestSpec,
    require_record_list,
    validated_envelope_slice,
)

__all__: list[str] = ['SamsaraCursorPageDecoder']

# Wire-protocol token: Final constant, not an enum. Deliberately unshared.
_AFTER_PARAM: Final[str] = 'after'


class _SamsaraPageEcho(BaseModel):
    """The pagination block Samsara returns on every page."""

    # strict=True / extra='ignore' rationale: see motive.py's _MotivePageEcho.
    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    has_next_page: bool = Field(alias='hasNextPage')
    end_cursor: str | None = Field(default=None, alias='endCursor')


class _SamsaraEnvelope(BaseModel):
    """Envelope slice: locates the echo; the record key is ignored."""

    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    pagination: _SamsaraPageEcho


@dataclass(frozen=True, slots=True)
class SamsaraCursorPageDecoder:
    """Decode Samsara's top-level-list pages and cursor.

    Attributes:
        records_key: The top-level key holding the record list.
    """

    records_key: str

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Return the spec unchanged; page one must NOT carry ``after``."""
        return spec

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        """Extract the records and compute the cursor verdict.

        Args:
            sent: The spec that produced this page.
            envelope: The parsed response body.

        Returns:
            The records and the pagination verdict; ``durable_progress``
            is always None -- Samsara cursors are fetch-private.

        Raises:
            ProviderResponseError: When the record-bearing shape or the
                cursor block is structurally violating, including
                continuation promised without a cursor.
        """
        records = require_record_list(envelope, self.records_key)
        echo = validated_envelope_slice(_SamsaraEnvelope, envelope).pagination
        if not echo.has_next_page:
            return DecodedPage(
                records=records,
                advance=PageAdvance(next_spec=None, durable_progress=None),
            )
        if echo.end_cursor is None or echo.end_cursor == '':
            # Continuation promised without a cursor: silently finishing
            # here would truncate data -- the one failure mode a fetch
            # library must never have.
            raise ProviderResponseError(
                detail='hasNextPage is true but endCursor is missing or empty'
            )
        next_spec = sent.with_merged_params({_AFTER_PARAM: echo.end_cursor})
        return DecodedPage(
            records=records,
            advance=PageAdvance(next_spec=next_spec, durable_progress=None),
        )
