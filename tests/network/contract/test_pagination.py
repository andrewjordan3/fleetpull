"""Tests for fleetpull.network.contract.pagination."""

import dataclasses

import pytest

from fleetpull.network.contract.pagination import PageAdvance


class TestPageAdvance:
    def test_is_frozen(self) -> None:
        verdict = PageAdvance(next_spec=None, durable_progress=None)
        with pytest.raises(dataclasses.FrozenInstanceError):
            verdict.durable_progress = 'other'  # type: ignore[misc]

    def test_has_slots_no_dict(self) -> None:
        verdict = PageAdvance(next_spec=None, durable_progress=None)
        assert not hasattr(verdict, '__dict__')

    def test_complete_verdicts_may_still_carry_progress(self) -> None:
        # The GeoTab terminal shape, pinned as a vocabulary-level fact:
        # the terminal page's durable progress is the resume point.
        verdict = PageAdvance(next_spec=None, durable_progress='0000000000000001')
        assert verdict.next_spec is None
        assert verdict.durable_progress == '0000000000000001'
