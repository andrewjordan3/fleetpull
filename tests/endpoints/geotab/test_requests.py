"""Tests for fleetpull.endpoints.geotab._requests.

The leaf tests pin the ``Get`` wire bodies; this module pins the
machinery's own contract seams: the snapshot builder rejecting any
non-``None`` resume (a window reaching it would silently fetch the
entire entity set unwindowed), and the shared ``GetFeed`` builder's
seed-versus-resume request shapes -- the wire half of the seed-once
invariant (DESIGN section 14, I4): ``search.fromDate`` rides ONLY a
``FeedSeed``, ``fromVersion`` rides ONLY a ``FeedToken``, and the two
keys never co-occur.
"""

from collections.abc import Mapping
from datetime import UTC, datetime

import pytest

from fleetpull.endpoints.geotab._requests import (
    GeotabGetFeedSpecBuilder,
    GeotabGetSpecBuilder,
)
from fleetpull.incremental import DateWindow, FeedSeed, FeedToken
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.vocabulary import JsonValue


def _build_builder() -> GeotabGetSpecBuilder:
    return GeotabGetSpecBuilder(
        server='my.geotab.com',
        type_name='Device',
        results_limit=5000,
    )


def _feed_builder() -> GeotabGetFeedSpecBuilder:
    return GeotabGetFeedSpecBuilder(
        server='my.geotab.com',
        type_name='LogRecord',
        results_limit=50000,
    )


def _params_of(spec: RequestSpec) -> Mapping[str, JsonValue]:
    assert spec.json_body is not None
    params = spec.json_body['params']
    assert isinstance(params, Mapping)
    return params


class TestGeotabGetSpecBuilder:
    def test_rejects_a_date_window_resume(self) -> None:
        window = DateWindow(
            start=datetime(2026, 7, 6, tzinfo=UTC),
            end=datetime(2026, 7, 13, tzinfo=UTC),
        )
        with pytest.raises(TypeError, match='DateWindow'):
            _build_builder().build_spec(resume=window, member_values={})

    def test_rejects_a_feed_token_resume(self) -> None:
        with pytest.raises(TypeError, match='FeedToken'):
            _build_builder().build_spec(
                resume=FeedToken(from_version='0000000000000000'), member_values={}
            )


class TestGeotabGetFeedSpecBuilder:
    def test_seed_carries_from_date_and_no_from_version(self) -> None:
        # THE SEED SHAPE (I4's wire half): search.fromDate, never
        # fromVersion. Wire-proven 2026-07-21 -- seeding positions the
        # feed at a version covering all entities with date >= fromDate.
        spec = _feed_builder().build_spec(
            resume=FeedSeed(start=datetime(2024, 1, 1, tzinfo=UTC)),
            member_values={},
        )
        assert spec.method is HttpMethod.POST
        assert spec.url == 'https://my.geotab.com/apiv1'
        assert spec.json_body is not None
        assert spec.json_body['method'] == 'GetFeed'
        params = _params_of(spec)
        assert params['typeName'] == 'LogRecord'
        assert params['search'] == {'fromDate': '2024-01-01T00:00:00Z'}
        assert params['resultsLimit'] == 50000
        assert 'fromVersion' not in params
        assert 'sort' not in params

    def test_resume_carries_from_version_and_no_search(self) -> None:
        # THE RESUME SHAPE: fromVersion, never search -- matching the
        # decoder's own advances, which strip search the same way.
        spec = _feed_builder().build_spec(
            resume=FeedToken(from_version='00000000000000aa'), member_values={}
        )
        params = _params_of(spec)
        assert params['fromVersion'] == '00000000000000aa'
        assert params['resultsLimit'] == 50000
        assert 'search' not in params

    def test_rejects_a_none_resume(self) -> None:
        # A feed always resumes from something; None reaching the builder
        # is a wiring bug, not a bootstrap case.
        with pytest.raises(TypeError, match='FeedSeed or FeedToken'):
            _feed_builder().build_spec(resume=None, member_values={})

    def test_rejects_a_date_window_resume(self) -> None:
        window = DateWindow(
            start=datetime(2026, 7, 6, tzinfo=UTC),
            end=datetime(2026, 7, 13, tzinfo=UTC),
        )
        with pytest.raises(TypeError, match='got DateWindow'):
            _feed_builder().build_spec(resume=window, member_values={})

    def test_member_values_are_ignored(self) -> None:
        # A single-chain endpoint binds no member; the protocol parameter
        # is accepted and unused.
        spec = _feed_builder().build_spec(
            resume=FeedToken(from_version='00000000000000aa'),
            member_values={'vehicle_id': 'ignored'},
        )
        assert 'vehicle_id' not in _params_of(spec)
