# src/fleetpull/network/decoders/samsara.py
"""Samsara page decoders: the cursor walk and its series-unnesting
composition for the vehicle-stats surfaces
(sources: scrubbed provider-behavior verification, June 2026; cursor
contract from provider documentation, proven live 2026-07-17 -- the
advance continued across a real page boundary with no overlap or loss,
and the terminal page carried ``hasNextPage: false`` beside an
EMPTY-STRING ``endCursor``, the shape the continuation guard below is
calibrated against).

Records arrive as a top-level list under a per-endpoint key; the
``pagination`` block carries ``endCursor``/``hasNextPage``. The first
page sends ``limit`` and no ``after``; subsequent pages send
``after=<endCursor>`` (merged onto the sent spec, so ``limit``
persists). Decoder logic deliberately resembles its siblings without
sharing code; the cursor verdict is written once here
(``cursor_page_advance``, exported so ``samsara_reports.py`` shares it)
and consumed by every decoder that walks the cursor.
The one deliberate cross-module share is the window stamp
(``_window_stamp.py``): the synthesized keys are our own
provider-uniform vocabulary, not envelope logic.

``SamsaraVehicleSeriesPageDecoder`` composes the cursor decoder by
delegation for ``/fleet/vehicles/stats/history``, whose cursor walks
the VEHICLE axis while each vehicle record nests a per-type reading
series (probe-settled 2026-07-20, DESIGN section 8).

The window-stamping report family for the fuel-energy report surfaces
(``SamsaraWindowReportPageDecoder``) lives in ``samsara_reports.py``,
sharing this module's cursor verdict and first-page shape.
"""

from dataclasses import dataclass
from typing import Final

from pydantic import Field

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract import (
    DecodedPage,
    PageAdvance,
    RequestSpec,
    StrictEnvelopeSlice,
    require_record_list,
    validated_envelope_slice,
)
from fleetpull.vocabulary import JsonObject, JsonValue

__all__: list[str] = [
    'SamsaraCursorPageDecoder',
    'SamsaraVehicleSeriesPageDecoder',
    'cursor_page_advance',
    'first_page_spec',
]

# Wire-protocol tokens: Final constants, not an enum. Deliberately unshared.
_AFTER_PARAM: Final[str] = 'after'
_LIMIT_PARAM: Final[str] = 'limit'


class _SamsaraPageEcho(StrictEnvelopeSlice):
    """The pagination block Samsara returns on every page."""

    has_next_page: bool = Field(alias='hasNextPage')
    end_cursor: str | None = Field(default=None, alias='endCursor')


class _SamsaraEnvelope(StrictEnvelopeSlice):
    """Envelope slice: locates the echo; the record key is ignored."""

    pagination: _SamsaraPageEcho


def cursor_page_advance(sent: RequestSpec, envelope: JsonValue) -> PageAdvance:
    """Compute one page's cursor verdict from its ``pagination`` echo.

    The one cursor contract every Samsara walk shares, written once for
    the decoders in this module and the report family beside it
    (``samsara_reports.py`` -- a same-provider extraction, not a
    cross-provider abstraction): terminal on ``hasNextPage: false``;
    otherwise ``after=<endCursor>`` merges onto the SENT spec, so every
    first-request parameter (``limit``, a window, a fixed selector)
    persists across the whole walk. ``durable_progress`` is always
    ``None`` -- Samsara cursors are fetch-private.

    Args:
        sent: The spec that produced this page.
        envelope: The parsed response body.

    Returns:
        The page's pagination verdict.

    Raises:
        ProviderResponseError: The ``pagination`` block is structurally
            violating, including continuation promised without a cursor.
    """
    echo = validated_envelope_slice(_SamsaraEnvelope, envelope).pagination
    if not echo.has_next_page:
        return PageAdvance(next_spec=None, durable_progress=None)
    if echo.end_cursor is None or echo.end_cursor == '':
        # Continuation promised without a cursor: silently finishing
        # here would truncate data -- the one failure mode a fetch
        # library must never have.
        raise ProviderResponseError(
            detail='hasNextPage is true but endCursor is missing or empty'
        )
    next_spec = sent.with_merged_params({_AFTER_PARAM: echo.end_cursor})
    return PageAdvance(next_spec=next_spec, durable_progress=None)


def first_page_spec(spec: RequestSpec, results_limit: int) -> RequestSpec:
    """Page one's spec: ``limit`` merged, and deliberately NO ``after``.

    The first-request shape every Samsara cursor walk sends, written once
    for this module's decoders and the report family beside it: the shared
    cursor verdict's ``after`` merge layers onto this spec, so the limit
    persists across every subsequent page.

    Args:
        spec: The endpoint's base first request.
        results_limit: The per-page record count to request via ``limit``.

    Returns:
        The first request with ``limit`` merged.
    """
    return spec.with_merged_params({_LIMIT_PARAM: str(results_limit)})


@dataclass(frozen=True, slots=True)
class SamsaraCursorPageDecoder:
    """Decode Samsara's top-level-list pages and cursor.

    Attributes:
        records_key: The top-level key holding the record list.
        results_limit: The per-page record count requested via the
            ``limit`` query parameter (pagination parameters are the
            decoder's, per the ``StaticGetSpecBuilder`` seam).
    """

    records_key: str
    results_limit: int

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Send page one via the shared first-page shape (``first_page_spec``)."""
        return first_page_spec(spec, self.results_limit)

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
        return DecodedPage(records=records, advance=cursor_page_advance(sent, envelope))


# The vehicle-stats wire keys the unnesting reads (2026-07-20 capture):
# per-vehicle identity is `id`/`name` plus the `externalIds` object's
# literal DOTTED keys `samsara.serial`/`samsara.vin`.
_VEHICLE_ID_SOURCE_KEY: Final[str] = 'id'
_VEHICLE_NAME_SOURCE_KEY: Final[str] = 'name'
_EXTERNAL_IDS_KEY: Final[str] = 'externalIds'
_SERIAL_SOURCE_KEY: Final[str] = 'samsara.serial'
_VIN_SOURCE_KEY: Final[str] = 'samsara.vin'

# The synthesized identity keys merged onto each emitted reading. Chosen
# to be collision-free against every series key observed in the
# 2026-07-20 census (`time`, `value`, `latitude`, `longitude`,
# `headingDegrees`, `speedMilesPerHour`, `isEcuSpeed`, `reverseGeo`,
# `address`), so the reading-keys-win merge order below never actually
# discards an identity key.
_VEHICLE_ID_KEY: Final[str] = 'vehicleId'
_VEHICLE_NAME_KEY: Final[str] = 'vehicleName'
_VEHICLE_SERIAL_KEY: Final[str] = 'vehicleSerial'
_VEHICLE_VIN_KEY: Final[str] = 'vehicleVin'


def _synthesized_identity(vehicle: JsonObject) -> JsonObject:
    """The identity keys one vehicle record contributes to its readings.

    Every key is synthesized ONLY when its source is present -- the
    omit-absent-keys posture: a vehicle without ``externalIds`` (or
    without a dotted key inside it) contributes readings without the
    corresponding synthesized key, never a null.

    Args:
        vehicle: One vehicle record from the page's record list.

    Returns:
        The synthesized identity keys, wire values verbatim.

    Raises:
        ProviderResponseError: ``externalIds`` is present but not a JSON
            object -- silently dropping serial/vin there would hide the
            malformation.
    """
    identity: JsonObject = {}
    if _VEHICLE_ID_SOURCE_KEY in vehicle:
        identity[_VEHICLE_ID_KEY] = vehicle[_VEHICLE_ID_SOURCE_KEY]
    if _VEHICLE_NAME_SOURCE_KEY in vehicle:
        identity[_VEHICLE_NAME_KEY] = vehicle[_VEHICLE_NAME_SOURCE_KEY]
    if _EXTERNAL_IDS_KEY not in vehicle:
        return identity
    external_ids = vehicle[_EXTERNAL_IDS_KEY]
    if not isinstance(external_ids, dict):
        raise ProviderResponseError(
            detail=f'vehicle {_EXTERNAL_IDS_KEY!r} is not a JSON object'
        )
    if _SERIAL_SOURCE_KEY in external_ids:
        identity[_VEHICLE_SERIAL_KEY] = external_ids[_SERIAL_SOURCE_KEY]
    if _VIN_SOURCE_KEY in external_ids:
        identity[_VEHICLE_VIN_KEY] = external_ids[_VIN_SOURCE_KEY]
    return identity


@dataclass(frozen=True, slots=True)
class SamsaraVehicleSeriesPageDecoder:
    """Decode vehicle-stats pages into flat per-reading records.

    The series-unnesting composition over ``SamsaraCursorPageDecoder``
    for ``GET /fleet/vehicles/stats/history`` (probe-settled
    2026-07-20): the cursor walks the VEHICLE axis within the fixed
    window (three consecutive live pages showed zero vehicle-id
    overlap), and each vehicle record nests one reading series under
    the requested stat type's key. Composition is by DELEGATION: an
    inner cursor decoder handles ``first_request`` and the whole
    pagination verdict verbatim -- this class only unnests the inner
    page's vehicle records.

    The unnesting contract: for each vehicle record, for each element
    of ``vehicle[series_key]``, emit one flat record carrying the
    reading's keys verbatim plus the SYNTHESIZED identity keys
    ``vehicleId``/``vehicleName``/``vehicleSerial``/``vehicleVin``
    (sourced from ``id``/``name`` and the ``externalIds`` object's
    literal dotted ``samsara.serial``/``samsara.vin`` keys). Identity
    keys are synthesized only when their source is present; reading
    keys win any collision -- impossible by census, since the
    synthesized names were chosen collision-free against every observed
    series key. A vehicle whose series array is missing or empty
    contributes zero records.

    This decoder is Samsara-stats-specific by evidence, not a generic
    flattener: the identity-key sourcing, the dotted ``externalIds``
    keys, and the one-series-per-record shape are this surface's
    captured facts, and generalizing beyond them would encode structure
    no probe has shown.

    Attributes:
        records_key: The top-level key holding the vehicle-record list
            (forwarded to the inner cursor decoder).
        results_limit: The per-page vehicle count requested via
            ``limit`` (forwarded to the inner cursor decoder).
        series_key: The per-vehicle key holding this endpoint's reading
            series -- the requested stat type's wire name.
    """

    records_key: str
    results_limit: int
    series_key: str

    def _cursor_decoder(self) -> SamsaraCursorPageDecoder:
        """The inner cursor decoder pagination delegates to."""
        return SamsaraCursorPageDecoder(
            records_key=self.records_key, results_limit=self.results_limit
        )

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Delegate page one verbatim to the inner cursor decoder."""
        return self._cursor_decoder().first_request(spec)

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        """Unnest the inner page's vehicles into flat reading records.

        Args:
            sent: The spec that produced this page.
            envelope: The parsed response body.

        Returns:
            One record per reading (the unnesting contract, class
            docstring); the pagination advance passes through the inner
            decoder untouched.

        Raises:
            ProviderResponseError: When the inner decode raises (the
                cursor contract's guards, including continuation
                promised without a cursor), or when a present series
                value or element is structurally violating.
        """
        inner_page = self._cursor_decoder().decode_page(sent, envelope)
        readings = [
            reading
            for vehicle in inner_page.records
            for reading in self._unnest_vehicle(vehicle)
        ]
        return DecodedPage(records=readings, advance=inner_page.advance)

    def _unnest_vehicle(self, vehicle: JsonObject) -> list[JsonObject]:
        """One vehicle record's flat reading records (possibly none).

        Args:
            vehicle: One vehicle record from the inner page.

        Returns:
            One flat record per series element; empty when the series
            is missing or empty (only carriers are returned per
            requested type in capture, but absence stays a zero-record
            vehicle, never an error).

        Raises:
            ProviderResponseError: A present series value is not a
                list, or a series element is not a JSON object.
        """
        if self.series_key not in vehicle:
            return []
        series = vehicle[self.series_key]
        if not isinstance(series, list):
            raise ProviderResponseError(
                detail=f'vehicle series {self.series_key!r} is not a list'
            )
        identity = _synthesized_identity(vehicle)
        readings: list[JsonObject] = []
        for element in series:
            if not isinstance(element, dict):
                raise ProviderResponseError(
                    detail=(
                        f'vehicle series {self.series_key!r} element is not a '
                        'JSON object'
                    )
                )
            # Reading keys win the merge (collision-free by census; the
            # class docstring records the naming choice).
            readings.append({**identity, **element})
        return readings
