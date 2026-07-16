"""Tests for fleetpull.model_contract.coercions.

The function is composed per field at its use site (Motive
``VehicleSummary.year`` -- the type-recovery case); the composition is
exercised against the real model in ``tests/models/motive/test_shared``.
"""

from fleetpull.model_contract import empty_str_to_none


class TestEmptyStrToNone:
    def test_empty_string_lifts_to_none(self) -> None:
        assert empty_str_to_none('') is None

    def test_non_empty_string_passes_through(self) -> None:
        assert empty_str_to_none('Kenworth') == 'Kenworth'

    def test_none_passes_through(self) -> None:
        assert empty_str_to_none(None) is None

    def test_non_string_values_pass_through(self) -> None:
        # 0 and False compare unequal to '' -- neither is lifted.
        assert empty_str_to_none(0) == 0
        assert empty_str_to_none(False) is False
