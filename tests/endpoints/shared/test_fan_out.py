"""Tests for fleetpull.endpoints.shared.fan_out."""

import dataclasses

import pytest

from fleetpull.endpoints.shared.fan_out import FanOutSource, FanOutSpec
from fleetpull.vocabulary import Provider


def _source() -> FanOutSource:
    return FanOutSource(
        provider=Provider.MOTIVE, endpoint='vehicles', column='vehicle_id'
    )


class TestFanOutSource:
    def test_holds_its_fields(self) -> None:
        source = _source()
        assert source.provider is Provider.MOTIVE
        assert source.endpoint == 'vehicles'
        assert source.column == 'vehicle_id'

    def test_discriminator_joins_provider_endpoint_column(self) -> None:
        assert _source().discriminator == 'motive.vehicles.vehicle_id'

    def test_is_frozen(self) -> None:
        source = _source()
        with pytest.raises(dataclasses.FrozenInstanceError):
            source.column = 'other'  # type: ignore[misc]


class TestFanOutSpec:
    def test_holds_source_and_placeholder(self) -> None:
        spec = FanOutSpec(source=_source(), path_placeholder='vehicle_id')
        assert spec.source == _source()
        assert spec.path_placeholder == 'vehicle_id'

    def test_is_frozen(self) -> None:
        spec = FanOutSpec(source=_source(), path_placeholder='vehicle_id')
        with pytest.raises(dataclasses.FrozenInstanceError):
            spec.path_placeholder = 'other'  # type: ignore[misc]
