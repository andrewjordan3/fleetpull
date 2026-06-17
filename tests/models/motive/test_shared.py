"""Tests for fleetpull.models.motive.shared."""

from fleetpull.models.motive.shared import DriverSummary, EldDeviceInfo

# Obviously-synthetic identifiers — never real VINs or fleet IDs in commits.
_DRIVER_ID: int = 9000001
_DEVICE_ID: int = 8800001


class TestEldDeviceInfo:
    def test_validates_and_aliases_id(self) -> None:
        device: EldDeviceInfo = EldDeviceInfo.model_validate(
            {'id': _DEVICE_ID, 'identifier': 'TESTELD0001', 'model': 'lbb-3.6ca'}
        )
        assert device.device_id == _DEVICE_ID
        assert device.identifier == 'TESTELD0001'
        assert device.model == 'lbb-3.6ca'


class TestDriverSummary:
    def test_validates_with_free_form_status_and_role(self) -> None:
        driver: DriverSummary = DriverSummary.model_validate(
            {
                'id': _DRIVER_ID,
                'first_name': 'Sam',
                'last_name': 'Synthetic',
                'status': 'whatever-motive-sends',
                'role': 'whatever-role',
            }
        )
        assert driver.driver_id == _DRIVER_ID
        assert driver.status == 'whatever-motive-sends'
        assert driver.role == 'whatever-role'

    def test_optional_fields_default_none(self) -> None:
        driver: DriverSummary = DriverSummary.model_validate(
            {'id': _DRIVER_ID, 'first_name': 'Sam', 'last_name': 'Synthetic'}
        )
        assert driver.username is None
        assert driver.email is None
        assert driver.status is None
