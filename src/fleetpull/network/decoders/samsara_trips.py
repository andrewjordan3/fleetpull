# src/fleetpull/network/decoders/samsara_trips.py
"""The Samsara trips page decoder: unpaginated records, vehicle-stamped.

The trips surface (``GET /v1/fleet/trips``) is a per-vehicle fan-out
whose response does NOT echo the requested ``vehicleId`` -- the inner
per-trip object carries no vehicle field of any kind (2026-07-20
census; re-proven at scale 2026-07-22, where every stored trip lacked a
vehicle identity). The vehicle a trip belongs to is therefore the
fan-out member the request asked for, not anything on the wire, so this
decoder STAMPS every trip with the ``vehicleId`` copied VERBATIM from
the SENT spec's own query param. It is the report family's
sent-spec-sourced stamp (``SamsaraWindowReportPageDecoder``) applied to
a fan-out member instead of a window: without it the stored row cannot
be attributed to a vehicle.

The envelope is ``{"trips": [...]}`` with no pagination of any kind --
one response per (vehicle, window) -- so the page is always terminal
(the ``SinglePageDecoder`` shape it supersedes for this surface, now
that the terminal page must also carry the member stamp). Decoder logic
deliberately resembles its siblings without sharing code.
"""

from dataclasses import dataclass

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract import (
    DecodedPage,
    PageAdvance,
    RequestSpec,
    require_record_list,
)
from fleetpull.vocabulary import JsonValue

__all__: list[str] = ['SamsaraTripsPageDecoder']


@dataclass(frozen=True, slots=True)
class SamsaraTripsPageDecoder:
    """Decode Samsara's unpaginated trips page, stamping the fan-out vehicle.

    The trips surface returns a top-level list under ``records_key`` with
    no pagination, so every page is terminal. The per-trip wire object
    carries no vehicle identity; the vehicle is the fan-out member the
    request asked for, so each record is stamped with ``member_key``
    copied verbatim from the SENT spec's own query param -- the
    ``SamsaraWindowReportPageDecoder`` sent-spec stamp, sourcing a
    fan-out member rather than a window.

    The stamp wins any (census-impossible) key collision: it is the
    row's REQUIRED vehicle identity, exactly the vehicle asked of the
    provider, and a colliding future wire key must never silently
    supplant it -- the report family's stamp-wins order, the inverse of
    the series decoder's reading-keys-win.

    Attributes:
        records_key: The top-level key holding the trip record list.
        member_key: The fan-out member's query-param name (the roster
            ``member_key``); read off the SENT spec and stamped onto
            every record under the SAME key, which is also the response
            model's alias for the stamped ``vehicle_id`` field. One
            token binds the request param, the stamp, and the model
            alias, so no translation seam can drift.
    """

    records_key: str
    member_key: str

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Return the base spec unchanged; the trips surface is unpaginated."""
        return spec

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        """Extract the records, stamp each with the sent vehicle; terminal.

        Args:
            sent: The spec that produced this page; supplies the fan-out
                member (``member_key``) stamped onto every record.
            envelope: The parsed response body.

        Returns:
            One record per trip, each carrying the ``member_key`` stamp,
            and a terminal verdict -- there is no continuation.

        Raises:
            ProviderResponseError: The sent spec lacks the ``member_key``
                param (a wiring bug -- a fan-out request always carries
                its member -- never silently unstamped, vehicle-less
                rows), or the record-bearing shape is structurally
                violating.
        """
        params = sent.params or {}
        if self.member_key not in params:
            raise ProviderResponseError(
                detail=(
                    f'sent spec lacks the {self.member_key!r} fan-out member '
                    'to stamp trip rows with'
                )
            )
        vehicle_stamp = {self.member_key: params[self.member_key]}
        records = require_record_list(envelope, self.records_key)
        stamped = [{**record, **vehicle_stamp} for record in records]
        return DecodedPage(
            records=stamped,
            advance=PageAdvance(next_spec=None, durable_progress=None),
        )
