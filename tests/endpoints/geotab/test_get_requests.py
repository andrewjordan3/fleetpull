"""Tests for fleetpull.endpoints.geotab._get_requests.

The leaf tests pin the wire bodies; this module pins the machinery's
own contract seam: the snapshot builder rejecting any non-``None``
resume. A window reaching the snapshot builder is a wiring bug that
would otherwise silently fetch the entire entity set unwindowed.
"""

from datetime import UTC, datetime

import pytest

from fleetpull.endpoints.geotab._get_requests import GeotabGetSpecBuilder
from fleetpull.incremental import DateWindow, FeedToken


def _build_builder() -> GeotabGetSpecBuilder:
    return GeotabGetSpecBuilder(
        server='my.geotab.com',
        type_name='Device',
        results_limit=5000,
    )


class TestGeotabGetSpecBuilder:
    def test_rejects_a_date_window_resume(self) -> None:
        window = DateWindow(
            start=datetime(2026, 7, 6, tzinfo=UTC),
            end=datetime(2026, 7, 13, tzinfo=UTC),
        )
        with pytest.raises(TypeError, match='DateWindow'):
            _build_builder().build_spec(resume=window, path_values={})

    def test_rejects_a_feed_token_resume(self) -> None:
        with pytest.raises(TypeError, match='FeedToken'):
            _build_builder().build_spec(
                resume=FeedToken(from_version='0000000000000000'), path_values={}
            )
