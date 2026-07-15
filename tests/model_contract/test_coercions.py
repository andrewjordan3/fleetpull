"""Tests for fleetpull.model_contract.coercions."""

from fleetpull.model_contract import (
    EmptyStrIsNone,
    ResponseModel,
    empty_str_to_none,
)


class _Annotated(ResponseModel):
    value: EmptyStrIsNone = None


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


class TestEmptyStrIsNoneAnnotation:
    def test_wire_empty_string_validates_to_none(self) -> None:
        assert _Annotated.model_validate({'value': ''}).value is None

    def test_wire_value_survives(self) -> None:
        assert _Annotated.model_validate({'value': 'Box'}).value == 'Box'

    def test_wire_null_stays_none(self) -> None:
        assert _Annotated.model_validate({'value': None}).value is None
