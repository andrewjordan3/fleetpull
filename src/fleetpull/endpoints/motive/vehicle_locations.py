# src/fleetpull/endpoints/motive/vehicle_locations.py
"""The Motive vehicle_locations watermark spec-builder.

A per-vehicle breadcrumb endpoint: the orchestrator fans out over vehicles and
calls ``build_spec`` once per vehicle, passing that vehicle's id in ``path_values``
and the run's resume ``DateWindow`` as ``resume``. The builder renders the
per-vehicle path and maps the window to Motive's ``start_date`` / ``end_date``
query parameters.

Motive's ``start_date`` / ``end_date`` are inclusive on both ends and day-granular,
anchored on ``located_at`` -- the endpoint returns every breadcrumb whose date falls
in ``[start_date, end_date]`` (confirmed against the predecessor's production
fetcher, whose comment records that Motive "returns full days"). The date-partitioned
watermark path wants exactly those whole days -- each ``date=`` partition is replaced
wholesale -- so unlike the predecessor (which post-filtered to an exact datetime
range for its single-file output), this builder does not trim: it fetches the whole
days and lets the writer replace whole partitions.

The mapping aligns with the storage layer's covered dates (``window_dates``):
``start_date`` is the UTC date of ``window.start`` and ``end_date`` is the UTC date
of ``window.end - 1 microsecond`` -- the window's last covered date. The epsilon is
exact because the window's datetimes are microsecond-precision end to end. So what
this builder fetches equals what the writer replaces and prunes: the same date set,
no edge double-count, idempotent on refetch. Both strings come from
``to_utc_date_string`` (the timing codec), the house encoder for a date-only param.

This is the dedicated builder for vehicle_locations specifically -- a date-range
fetch plus a per-vehicle path fan-out. Other Motive date-range endpoints
(driving_periods, idle_events) are fleet-wide with no fan-out, so they would take a
different builder; promotion to a shared Motive builder waits for a real second user.
"""

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta

from fleetpull.endpoints.shared import ResumeValue, render_url_path_template
from fleetpull.incremental import DateWindow
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.timing import to_utc_date_string

__all__: list[str] = ['MotiveVehicleLocationsSpecBuilder']

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MotiveVehicleLocationsSpecBuilder:
    """Build the per-vehicle, date-windowed first request for vehicle_locations.

    The ``SpecBuilder`` for the Motive vehicle_locations watermark endpoint:
    renders the per-vehicle path and injects the resume window as Motive's
    ``start_date`` / ``end_date`` parameters. The endpoint is not paginated, so this
    first request is the only request; the page decoder returns no successor.

    Attributes:
        base_url: Root of the Motive API, trailing-slash-normalized by the provider
            config so the leading-slash path joins directly.
        path_template: The per-vehicle request path with the ``{vehicle_id}``
            placeholder (``'/v3/vehicle_locations/{vehicle_id}'``).
    """

    base_url: str
    path_template: str

    def build_spec(
        self, resume: ResumeValue, path_values: Mapping[str, str]
    ) -> RequestSpec:
        """Build the per-vehicle, date-windowed GET.

        Args:
            resume: The run's resume window. Must be a ``DateWindow`` -- a watermark
                endpoint always resumes from one; any other value is a wiring bug.
            path_values: The path fan-out values; must carry ``vehicle_id`` (the
                strict template renderer rejects a missing or extra key).

        Returns:
            A credential-less ``GET`` for the per-vehicle URL, carrying
            ``start_date`` / ``end_date`` for the window's covered dates. Auth
            headers are layered on by the client's ``ProviderProfile``.

        Raises:
            TypeError: ``resume`` is not a ``DateWindow``.
            UrlPathTemplateError: ``path_values`` does not match the template's
                placeholders, or a value is empty.

        Side Effects:
            None.
        """
        if not isinstance(resume, DateWindow):
            raise TypeError(
                'MotiveVehicleLocationsSpecBuilder requires a DateWindow resume, '
                f'got {type(resume).__name__}.'
            )
        rendered_path = render_url_path_template(self.path_template, path_values)
        url = f'{self.base_url}{rendered_path}'
        start_date = to_utc_date_string(resume.start)
        end_date = to_utc_date_string(resume.end - timedelta(microseconds=1))
        params = {'start_date': start_date, 'end_date': end_date}
        return RequestSpec(method=HttpMethod.GET, url=url, params=params)
