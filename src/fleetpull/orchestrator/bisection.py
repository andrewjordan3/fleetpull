# src/fleetpull/orchestrator/bisection.py
"""The bisecting request driver: complete fetches from capped, unsortable Gets.

The third ``RequestDriver`` (the request-cardinality seam,
``orchestrator/drivers.py``): for endpoints declaring a
``WindowBisection``, the unit's resume window is fetched whole; a
response of exactly the declared ``results_limit`` is the overflow
signal — the page is discarded, the window halves at a whole-second
midpoint, and both halves recurse left-to-right; a floor-width window
still returning a full page raises loudly (the data is denser than
windowed fetching can enumerate — the provider's feed transport is the
escape for such streams). Overflow is a return-type condition (page
length), never an exception.

Fetch grain thereby decouples from write grain: work units and the
delete-by-window merge stay whole-window while only the wire requests
narrow. Every sub-request rides ``client.fetch_pages`` — one limiter
token per attempt, exactly like any page walk — and a mid-recursion
crash re-claims the whole unit, idempotent under delete-by-window
(no bisection state is ever persisted).

Under overlap-matched retrieval a record straddling an internal split
boundary is returned by both neighboring leaves, so each emitted page
is filtered to the records ANCHORED in its own leaf window (the
binding's ``event_time_wire_key``): leaves partition the unit, every
record has exactly one owning leaf, and write-time dedup stays hygiene
rather than a correctness mechanism. Midpoints are computed at whole
seconds because fractional-second search bounds are unprobed on the
wire.
"""

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    ResumeValue,
    WindowBisection,
)
from fleetpull.exceptions import ProviderResponseError
from fleetpull.incremental import DateWindow
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.vocabulary import JsonObject

__all__: list[str] = ['BisectingWindowDriver']


@dataclass(frozen=True, slots=True)
class BisectingWindowDriver:
    """Fetch a windowed endpoint completely by adaptive window bisection.

    Attributes:
        bisection: The endpoint's declared bisection facts — the overflow
            threshold, the floor width, and the wire key that anchors
            each record to its one owning leaf window.
    """

    bisection: WindowBisection

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[FetchedPage]:
        """Yield one ownership-filtered batch per non-overflowing leaf window.

        Args:
            definition: The endpoint being run (its ``spec_builder``,
                ``page_decoder``, and ``quota_scope``).
            client: The transport client for this endpoint's provider.
            resume: The run's resume window. Must be a ``DateWindow`` — a
                watermark endpoint always resumes from one; any other
                value is a wiring bug.

        Yields:
            One ``FetchedPage`` per leaf window, left-to-right, each
            holding only the records anchored in that leaf.

        Raises:
            TypeError: ``resume`` is not a ``DateWindow``.
            ProviderResponseError: A floor-width window still returned a
                full page (the loud no-narrower failure), or a record
                arrived without a parseable anchor timestamp.
        """
        if not isinstance(resume, DateWindow):
            raise TypeError(
                'BisectingWindowDriver requires a DateWindow resume, '
                f'got {type(resume).__name__}.'
            )
        yield from self._drive_window(definition, client, resume)

    def _drive_window(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        window: DateWindow,
    ) -> Iterator[FetchedPage]:
        """Fetch one window; recurse on overflow, yield the leaf otherwise.

        Args:
            definition: The endpoint being run.
            client: The transport client for this endpoint's provider.
            window: The window to fetch — the unit's resume window at the
                top of the recursion, a half of a parent below it.

        Yields:
            The leaf batches under this window, left-to-right.

        Raises:
            ProviderResponseError: Per ``record_batches``.
        """
        spec = definition.spec_builder.build_spec(resume=window, path_values={})
        pages = list(
            client.fetch_pages(
                spec, definition.page_decoder, definition.quota_scope.value
            )
        )
        # The endpoint's decoder is single-page (terminal on the first
        # page), so the chain is exactly one page.
        records = [record for page in pages for record in page.records]
        if len(records) < self.bisection.results_limit:
            yield FetchedPage(
                records=self._anchored_in(records, window, definition),
                durable_progress=None,
            )
            return
        width = window.end - window.start
        if width <= self.bisection.floor:
            raise ProviderResponseError(
                detail=(
                    f'{definition.provider.value}.{definition.name}: a '
                    f'{width} window starting {window.start.isoformat()} '
                    f'still returned {len(records)} records — the window '
                    f'cannot be narrowed under the provider record cap. '
                    f'The stream is denser than windowed fetching can '
                    f'enumerate; the provider feed transport is the '
                    f'escape for this endpoint at this density.'
                )
            )
        midpoint = _whole_second_midpoint(window.start, window.end)
        yield from self._drive_window(
            definition, client, DateWindow(start=window.start, end=midpoint)
        )
        yield from self._drive_window(
            definition, client, DateWindow(start=midpoint, end=window.end)
        )

    def _anchored_in(
        self,
        records: list[JsonObject],
        window: DateWindow,
        definition: EndpointDefinition[ResponseModel],
    ) -> list[JsonObject]:
        """Keep the records whose anchor timestamp falls in the window.

        Under overlap-matched retrieval a straddler of an internal split
        boundary is fetched by both neighboring leaves; anchoring gives it
        exactly one owner. Records anchored outside the unit's whole
        window (overlap edge returns) drop here too — the same records
        the runner's window filter would drop after modeling.

        Args:
            records: The leaf page's raw records.
            window: The leaf window that owns anchored records.
            definition: The endpoint being run, for the error detail.

        Returns:
            The records anchored in ``[window.start, window.end)``.

        Raises:
            ProviderResponseError: A record's anchor key is missing or
                unparseable — the routing anchor is load-bearing, so a
                record without one fails loudly rather than being
                silently kept or dropped.
        """
        wire_key = self.bisection.event_time_wire_key
        anchored: list[JsonObject] = []
        for record in records:
            raw_value = record.get(wire_key)
            if not isinstance(raw_value, str):
                raise ProviderResponseError(
                    detail=(
                        f'{definition.provider.value}.{definition.name}: '
                        f'record is missing the anchor timestamp '
                        f'{wire_key!r} bisection routes by.'
                    )
                )
            try:
                anchor = datetime.fromisoformat(raw_value)
            except ValueError as error:
                raise ProviderResponseError(
                    detail=(
                        f'{definition.provider.value}.{definition.name}: '
                        f'unparseable anchor timestamp {raw_value!r} under '
                        f'{wire_key!r}.'
                    )
                ) from error
            if window.start <= anchor < window.end:
                anchored.append(record)
        return anchored


def _whole_second_midpoint(start: datetime, end: datetime) -> datetime:
    """The window's midpoint, floored to a whole second.

    Fractional-second search bounds are unprobed on the wire, so splits
    stay second-granular; the halves differ by at most one second, which
    the recursion absorbs.

    Args:
        start: The window's inclusive start.
        end: The window's exclusive end.

    Returns:
        The floored midpoint, strictly between ``start`` and ``end`` for
        any window wider than one second.
    """
    half_seconds = int((end - start).total_seconds() // 2)
    return start + timedelta(seconds=half_seconds)
