"""Tests for fleetpull.network.decoders._window_stamp.

The shared window-stamp helper is exercised end-to-end through both
providers' report decoders (test_motive.py / test_samsara.py); these
tests pin its own contract directly: the provider-uniform synthesized
keys, the verbatim copy under arbitrary param names, and the
missing-param loudness.
"""

import pytest

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.network.decoders._window_stamp import (
    WINDOW_END_KEY,
    WINDOW_START_KEY,
    window_stamp_from_sent_spec,
)


def _spec(params: dict[str, str] | None) -> RequestSpec:
    return RequestSpec(
        method=HttpMethod.GET,
        url='https://api.example.com/reports',
        params=params,
    )


class TestWindowStampFromSentSpec:
    def test_copies_the_sent_params_verbatim_onto_the_uniform_keys(self) -> None:
        # The stamp keys are OUR provider-uniform vocabulary; the param
        # NAMES are each provider's own -- here Motive's snake_case
        # labels, values copied verbatim (date labels stay date labels).
        stamp = window_stamp_from_sent_spec(
            _spec({'start_date': '2026-01-05', 'end_date': '2026-01-05'}),
            start_param='start_date',
            end_param='end_date',
        )
        assert stamp == {
            WINDOW_START_KEY: '2026-01-05',
            WINDOW_END_KEY: '2026-01-05',
        }
        assert (WINDOW_START_KEY, WINDOW_END_KEY) == (
            'windowStartDate',
            'windowEndDate',
        )

    def test_param_names_are_caller_facts_not_baked_in(self) -> None:
        # The Samsara report family's camelCase names produce the same
        # uniform stamp keys -- only the lookup names differ.
        stamp = window_stamp_from_sent_spec(
            _spec(
                {
                    'startDate': '2026-01-02T00:00:00Z',
                    'endDate': '2026-01-03T00:00:00Z',
                }
            ),
            start_param='startDate',
            end_param='endDate',
        )
        assert stamp == {
            WINDOW_START_KEY: '2026-01-02T00:00:00Z',
            WINDOW_END_KEY: '2026-01-03T00:00:00Z',
        }

    @pytest.mark.parametrize(
        'params',
        [
            None,
            {},
            {'start_date': '2026-01-05'},
            {'end_date': '2026-01-05'},
        ],
    )
    def test_a_spec_lacking_either_param_raises_loudly(
        self, params: dict[str, str] | None
    ) -> None:
        # Never silently unstamped rows: a missing window param is a
        # wiring bug, and the message names both expected params.
        with pytest.raises(ProviderResponseError, match=r"'start_date'/'end_date'"):
            window_stamp_from_sent_spec(
                _spec(params), start_param='start_date', end_param='end_date'
            )
