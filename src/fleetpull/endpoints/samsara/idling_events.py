# src/fleetpull/endpoints/samsara/idling_events.py
"""The Samsara idling_events binding: the fleet-wide windowed cursor walk --
the first windowed+cursor pairing, composed entirely from existing parts.

``GET /idling/events`` is a modern-envelope surface (``data`` +
``pagination {endCursor, hasNextPage}``, terminal on ``hasNextPage:
false`` beside an empty-string ``endCursor``; the cursor walk proven
live 2026-07-20: 11 pages at limit=200, 2,200/2,200 unique). Events are
fleet-wide with per-record asset attribution, so there is NO fan-out --
the default ``SingleFetch`` shape, declared by declaring nothing (the
Motive driving_periods template on the Samsara cursor decoder).

The windowed leaf builder composes with the existing
``SamsaraCursorPageDecoder`` because pagination parameters persist by
construction: the decoder's ``first_request`` merges ``limit`` onto the
builder's spec, and its ``after`` advance merges onto the SENT spec, so
the builder's ``startTime``/``endTime`` ride every page of the walk --
the mechanism proven live on the drivers sweep, now carrying a window
instead of a status.

The per-endpoint ``limit`` maximum is 200, NOT the 512 of
vehicles/drivers: limit=512 returns a loud JSON 400 (``"limit must be
lesser or equal than 200 but got value 512"``) -- the first captured
instance of Samsara's per-endpoint limit tiers; never assume a
sibling's limit (DESIGN §8, captured 2026-07-20).

Retrieval is START-anchored on UTC, proven by a discriminating pair on
a 6.5-hour event: a 60-second window strictly inside its span does NOT
return it, while a 60-second window straddling only its start DOES
(the fourth distinct anchoring datum across providers -- notably NOT
the company-local overlap behavior of Motive's idle_events sibling;
never assume the rule, per endpoint, ever). Consequence:
``event_time_column='start_time'``, the retrieval anchor and the
routing anchor coincide natively, no wire pad exists, and the runner's
post-fetch window filter is pure hygiene (the driving_periods
situation).

Records carry NO end key -- the interval is start plus
``durationMilliseconds``; events were only ever observed complete
(in-progress idles appear to materialize on completion), and the
watermark lookback absorbs late materialization (accepted residual).

The provider enforces a loud sub-3-months range cap -- 91 days
accepted; 180 days returns JSON 400 ``"Total duration must be less
than 3 months."``. No builder guard exists for it (the Motive
driving_periods stance): default 7-day backfill chunks sit far inside,
and a ``backfill_chunk_days`` raised past the cap fails loudly on the
first request rather than losing data silently -- the fix is a smaller
chunk width.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Final

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    ResumeValue,
    StorageKind,
    WatermarkMode,
    require_date_window,
)
from fleetpull.models.samsara import IdlingEvent
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.network.decoders import SamsaraCursorPageDecoder
from fleetpull.timing import to_iso8601
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = [
    'SamsaraIdlingEventsSpecBuilder',
    'build_endpoint',
]

_IDLING_EVENTS_PATH: Final[str] = '/idling/events'
_RECORDS_KEY: Final[str] = 'data'

# The per-page record count. 200 is THIS endpoint's probed maximum --
# NOT the 512 of vehicles/drivers: limit=512 earns a loud JSON 400
# naming the 200 cap (captured 2026-07-20), the first captured instance
# of Samsara's per-endpoint limit tiers. Never assume a sibling's limit.
_RESULTS_LIMIT: Final[int] = 200


@dataclass(frozen=True, slots=True)
class SamsaraIdlingEventsSpecBuilder:
    """Build the fleet-wide, date-windowed first request for idling events.

    The ``SpecBuilder`` for the idling_events single chain: a fixed
    ``GET base_url + path`` carrying the resume window as RFC3339
    ``startTime``/``endTime`` (the timing codec's ``to_iso8601``). The
    decoder owns pagination: its ``first_request`` merges ``limit``
    onto this spec and its ``after`` advance merges onto the sent spec,
    so the window parameters persist across the whole cursor walk.

    The canonical half-open ``[start, end)`` window maps to the wire as
    ``startTime = start`` and ``endTime = end``. Retrieval is
    START-anchored on UTC (module docstring), so a window's records are
    exactly those starting inside it; any boundary-instant reading of
    ``endTime`` the wire might take is absorbed by the runner's
    post-fetch window filter, which keeps only events whose start lies
    in ``[start, end)`` -- pure hygiene, never load-bearing.

    Attributes:
        base_url: Root of the Samsara API, trailing-slash-normalized by
            the provider config so the leading-slash path joins
            directly.
        path: The endpoint's leading-slash request path
            (``'/idling/events'``).
    """

    base_url: str
    path: str

    def build_spec(
        self, resume: ResumeValue, member_values: Mapping[str, str]
    ) -> RequestSpec:
        """Build the fleet-wide, date-windowed GET.

        Args:
            resume: The run's resume window. Must be a ``DateWindow`` --
                a watermark endpoint always resumes from one; any other
                value is a wiring bug.
            member_values: Accepted to satisfy the protocol; unused -- a
                fleet-wide single chain binds no member.

        Returns:
            A credential-less ``GET`` for ``base_url + path`` carrying
            the window's bounds as RFC3339 ``startTime``/``endTime``.
            Auth headers are layered on by the client's
            ``ProviderProfile``; pagination parameters are injected by
            the page decoder's ``first_request``.

        Raises:
            TypeError: ``resume`` is not a ``DateWindow``.
            ValueError: A window bound is not canonical UTC.

        Side Effects:
            None.
        """
        resume_window = require_date_window(resume, type(self).__name__)
        params = {
            'startTime': to_iso8601(resume_window.start),
            'endTime': to_iso8601(resume_window.end),
        }
        return RequestSpec(
            method=HttpMethod.GET,
            url=f'{self.base_url}{self.path}',
            params=params,
        )


def build_endpoint(config: SamsaraConfig) -> EndpointDefinition[IdlingEvent]:
    """Build the Samsara idling_events watermark binding.

    Fleet-wide idling events fetched incrementally: the run resumes
    from a ``DateWindow`` (watermark with the provider's late-arrival
    lookback from config -- which also absorbs idles materializing on
    completion), the fetched events are written to ``date=YYYY-MM-DD``
    partitions on ``start_time``, and each refetched partition is
    replaced. Records arrive as a top-level list under ``data``, walked
    by explicit cursor pages (``limit`` on page one, ``after`` merged
    thereafter, the window parameters persisting throughout), terminal
    on ``hasNextPage: false``. No request shape is declared -- the
    endpoint is a fleet-wide ``SingleFetch``, the default.

    Args:
        config: The validated Samsara configuration; supplies the base
            URL the spec-builder joins to the idling-events path and
            the lookback and cutoff the watermark mode carries.

    Returns:
        The frozen idling_events ``EndpointDefinition``. Construction
        validates the ``WatermarkMode`` / ``DATE_PARTITIONED`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.SAMSARA,
        name='idling_events',
        spec_builder=SamsaraIdlingEventsSpecBuilder(
            base_url=config.base_url, path=_IDLING_EVENTS_PATH
        ),
        page_decoder=SamsaraCursorPageDecoder(
            records_key=_RECORDS_KEY, results_limit=_RESULTS_LIMIT
        ),
        response_model=IdlingEvent,
        quota_scope=QuotaScope.SAMSARA,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(
            lookback=timedelta(days=config.lookback_days),
            cutoff=timedelta(days=config.cutoff_days),
        ),
        event_time_column='start_time',
    )
