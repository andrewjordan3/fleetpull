# src/fleetpull/endpoints/motive/_spec_builders.py
"""The shared Motive fleet-wide date-range spec-builder.

The builder for Motive endpoints that take day-granular ``start_date`` /
``end_date`` query parameters with no per-vehicle fan-out
(driving_periods, idle_events) — the promotion the vehicle_locations
module anticipated once a real second user arrived. vehicle_locations
keeps its own builder because it renders a per-vehicle path; this one
serves the fleet-wide pair and any future sibling.

``window_pad_days`` exists for endpoints whose server-side window
matching is not UTC: idle_events interprets its date range on
company-local day boundaries and matches by overlap (DESIGN §8, captured
2026-07-15), so its leaf pads the wire window one day on each side and
the true UTC window — the post-fetch window filter and the writer's
partition tripwire — does the trimming. A pad only ever widens what is
fetched, never what is written.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta

from fleetpull.endpoints.shared import ResumeValue, require_date_window
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.timing import to_utc_date_string

__all__: list[str] = ['MotiveFleetDateRangeSpecBuilder']


@dataclass(frozen=True, slots=True)
class MotiveFleetDateRangeSpecBuilder:
    """Build the fleet-wide, date-windowed first request.

    The ``SpecBuilder`` for Motive watermark endpoints with no fan-out:
    injects the resume window as ``start_date`` / ``end_date``, mapped
    exactly as the vehicle_locations builder maps them — ``start_date``
    is the UTC date of ``window.start`` and ``end_date`` the UTC date of
    ``window.end - 1 microsecond`` (the window's last covered date) —
    then widened by ``window_pad_days`` whole days on each side.

    Attributes:
        base_url: Root of the Motive API, trailing-slash-normalized by
            the provider config so the leading-slash path joins directly.
        path: The endpoint's leading-slash request path.
        window_pad_days: Whole days added to each side of the wire
            window. Zero for UTC-matched endpoints; idle_events sets one
            to cover its company-local overlap matching under any account
            timezone.
    """

    base_url: str
    path: str
    window_pad_days: int = 0

    def build_spec(
        self, resume: ResumeValue, member_values: Mapping[str, str]
    ) -> RequestSpec:
        """Build the fleet-wide, date-windowed GET.

        Args:
            resume: The run's resume window. Must be a ``DateWindow`` — a
                watermark endpoint always resumes from one; any other
                value is a wiring bug.
            member_values: Accepted to satisfy the protocol; unused — a
                fleet-wide single chain binds no member.

        Returns:
            A credential-less ``GET`` for ``base_url + path`` carrying
            the padded ``start_date`` / ``end_date``. Auth headers are
            layered on by the client's ``ProviderProfile``; pagination
            parameters are injected by the page decoder's
            ``first_request``.

        Raises:
            TypeError: ``resume`` is not a ``DateWindow``.

        Side Effects:
            None.
        """
        resume_window = require_date_window(resume, type(self).__name__)
        pad = timedelta(days=self.window_pad_days)
        start_date = to_utc_date_string(resume_window.start - pad)
        end_date = to_utc_date_string(
            resume_window.end - timedelta(microseconds=1) + pad
        )
        params = {'start_date': start_date, 'end_date': end_date}
        return RequestSpec(
            method=HttpMethod.GET,
            url=f'{self.base_url}{self.path}',
            params=params,
        )
