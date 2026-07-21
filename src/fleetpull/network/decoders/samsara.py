# src/fleetpull/network/decoders/samsara.py
"""Samsara page decoders: the cursor walk, its series-unnesting
composition for the vehicle-stats surfaces, and the window-stamping
report decoder for the fuel-energy report surfaces
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
sharing code; WITHIN this module the cursor verdict is written once
(``_cursor_page_advance``) and shared by every decoder that walks it.

``SamsaraVehicleSeriesPageDecoder`` composes the cursor decoder by
delegation for ``/fleet/vehicles/stats/history``, whose cursor walks
the VEHICLE axis while each vehicle record nests a per-type reading
series (probe-settled 2026-07-20, DESIGN section 8).

``SamsaraWindowReportPageDecoder`` decodes the fuel-energy report
surfaces (``/fleet/reports/{vehicles,drivers}/fuel-energy``), whose
record list nests one level deeper (``data`` is an OBJECT holding the
list under ``vehicleReports``/``driverReports``) and whose rows carry
NO event-time key of any kind -- each row is the provider's rollup over
exactly the requested window, so the decoder stamps every report with
the window the SENT spec asked for (probe-settled 2026-07-21, DESIGN
section 8).
"""

from dataclasses import dataclass
from typing import Final

from pydantic import BaseModel, ConfigDict, Field

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract import (
    DecodedPage,
    PageAdvance,
    RequestSpec,
    require_record_list,
    validated_envelope_slice,
)
from fleetpull.vocabulary import JsonObject, JsonValue

__all__: list[str] = [
    'SamsaraCursorPageDecoder',
    'SamsaraVehicleSeriesPageDecoder',
    'SamsaraWindowReportPageDecoder',
]

# Wire-protocol tokens: Final constants, not an enum. Deliberately unshared.
_AFTER_PARAM: Final[str] = 'after'
_LIMIT_PARAM: Final[str] = 'limit'


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


def _cursor_page_advance(sent: RequestSpec, envelope: JsonValue) -> PageAdvance:
    """Compute one page's cursor verdict from its ``pagination`` echo.

    The one cursor contract every Samsara walk shares, written once for
    the decoders in this module (a same-file extraction, not a
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
        """Send page one with ``limit`` and NO ``after``.

        The shared cursor verdict's ``after`` merge layers onto this
        spec, so the limit persists across every subsequent page.
        """
        return spec.with_merged_params({_LIMIT_PARAM: str(self.results_limit)})

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
        return DecodedPage(
            records=records, advance=_cursor_page_advance(sent, envelope)
        )


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


# The fuel-energy report surfaces' window wire params (2026-07-21
# capture): these surfaces take startDate/endDate NAMES -- unlike every
# other probed Samsara vertical's startTime/endTime -- while accepting
# full RFC3339 datetimes despite the names. The decoder reads them back
# off the SENT spec to stamp each report row.
_WINDOW_START_PARAM: Final[str] = 'startDate'
_WINDOW_END_PARAM: Final[str] = 'endDate'

# The synthesized window-identity keys merged onto each report. Chosen
# collision-free against the 2026-07-21 report-key censuses (71/71
# vehicle, 47/47 driver -- no time-shaped key of any kind on the wire),
# and derived from the wire param names so the stamp reads as what was
# asked of the provider.
_WINDOW_START_KEY: Final[str] = 'windowStartDate'
_WINDOW_END_KEY: Final[str] = 'windowEndDate'


def _report_window_stamp(sent: RequestSpec) -> JsonObject:
    """The window-identity keys the sent spec contributes to its reports.

    Copied VERBATIM from the sent spec's own ``startDate``/``endDate``
    params -- wire-truthful: the stamp is exactly what was asked of the
    provider, whose answer each report row is. A sent spec lacking
    either param is a wiring bug (the builder always renders both), and
    silently unstamped rows would strip the rows' time identity -- so it
    fails loudly instead.

    Args:
        sent: The spec that produced the page being decoded.

    Returns:
        ``{'windowStartDate': ..., 'windowEndDate': ...}``, values
        verbatim from the sent params.

    Raises:
        ProviderResponseError: The sent spec lacks either window param.
    """
    params = sent.params or {}
    if _WINDOW_START_PARAM not in params or _WINDOW_END_PARAM not in params:
        raise ProviderResponseError(
            detail=(
                f'sent spec lacks the {_WINDOW_START_PARAM!r}/'
                f'{_WINDOW_END_PARAM!r} window params to stamp report '
                'rows with'
            )
        )
    return {
        _WINDOW_START_KEY: params[_WINDOW_START_PARAM],
        _WINDOW_END_KEY: params[_WINDOW_END_PARAM],
    }


def _require_report_list(
    envelope: JsonValue, records_key: str, report_key: str
) -> list[JsonObject]:
    """The report list nested one level under the envelope's record key.

    The nested twin of ``require_record_list``, with the same
    structural-violation loudness: the envelope must be a JSON object,
    ``records_key`` present and itself an object, and the list under
    ``report_key`` inside it a list of JSON objects (that last leg IS
    ``require_record_list``, applied to the inner object).

    Args:
        envelope: The parsed response body.
        records_key: The top-level key holding the report container
            object.
        report_key: The key inside the container holding the report
            list.

    Returns:
        The report list, each element a JSON object.

    Raises:
        ProviderResponseError: The envelope is not an object,
            ``records_key`` is absent or not an object, ``report_key``
            is absent or not a list, or an element is not a JSON object.
    """
    if not isinstance(envelope, dict):
        raise ProviderResponseError(detail='response envelope is not a JSON object')
    if records_key not in envelope:
        raise ProviderResponseError(
            detail=f'response envelope is missing the record key {records_key!r}'
        )
    container = envelope[records_key]
    if not isinstance(container, dict):
        raise ProviderResponseError(
            detail=f'record key {records_key!r} is not a JSON object'
        )
    return require_record_list(container, report_key)


@dataclass(frozen=True, slots=True)
class SamsaraWindowReportPageDecoder:
    """Decode fuel-energy report pages into window-stamped records.

    The decoder for ``GET /fleet/reports/{vehicles,drivers}/fuel-energy``
    (probe-settled 2026-07-21, DESIGN section 8), whose envelope differs
    from the flat cursor surfaces twice over:

    - **The record list is NESTED.** ``data`` is an OBJECT whose only
      key is the per-surface report key (``vehicleReports`` /
      ``driverReports``), each a list of report objects -- extracted
      with the same structural-violation loudness ``require_record_list``
      gives flat lists.
    - **The rollup grain is the request window.** Report rows carry NO
      event-time key of any kind; each row is the provider's aggregate
      over exactly the requested window (widening the window GREW
      per-entity metrics, and day rollups are NOT additive into wider
      windows -- 89/267 mismatched). So the decoder stamps every report
      with the synthesized keys ``windowStartDate``/``windowEndDate``,
      copied verbatim from the SENT spec's own ``startDate``/``endDate``
      params -- the stats triple's synthesized-identity-keys precedent,
      sourced from the sent spec rather than the record. The stamp wins
      any (census-impossible) key collision: it is the row's REQUIRED
      time identity, and a colliding future wire key must never silently
      supplant what was actually asked of the provider -- the inverse of
      the series decoder's reading-keys-win order, where the synthesized
      keys are auxiliary attribution.

    Pagination is the standard cursor contract, shared via the
    module-level ``_cursor_page_advance`` (real at scale: a 2-day
    vehicle window walked 3 pages/267 reports); ``first_request``
    injects ``limit`` exactly as the cursor decoder does.

    Attributes:
        records_key: The top-level key holding the report container
            object (``'data'``).
        report_key: The container key holding this surface's report
            list (``'vehicleReports'`` / ``'driverReports'``).
        results_limit: The per-page record count requested via the
            ``limit`` query parameter (pagination parameters are the
            decoder's, per the ``StaticGetSpecBuilder`` seam).
    """

    records_key: str
    report_key: str
    results_limit: int

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Send page one with ``limit`` and NO ``after``.

        The ``after`` merge in the shared cursor verdict layers onto
        this spec, so the limit persists across every subsequent page.
        """
        return spec.with_merged_params({_LIMIT_PARAM: str(self.results_limit)})

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        """Extract the nested reports, stamp each with the sent window.

        Args:
            sent: The spec that produced this page.
            envelope: The parsed response body.

        Returns:
            One record per report, each carrying the synthesized
            ``windowStartDate``/``windowEndDate`` keys (class
            docstring); the pagination verdict is the shared cursor
            contract's.

        Raises:
            ProviderResponseError: The sent spec lacks a window param
                (a wiring bug -- never silently unstamped rows), the
                nested record-bearing shape is structurally violating,
                or the cursor block is (including continuation promised
                without a cursor).
        """
        window_stamp = _report_window_stamp(sent)
        reports = _require_report_list(envelope, self.records_key, self.report_key)
        stamped = [{**report, **window_stamp} for report in reports]
        return DecodedPage(
            records=stamped, advance=_cursor_page_advance(sent, envelope)
        )
