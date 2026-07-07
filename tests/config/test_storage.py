"""Tests for fleetpull.config.storage."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from fleetpull.config import StorageConfig


class TestStorageConfig:
    def test_requires_a_dataset_root(self) -> None:
        with pytest.raises(ValidationError):
            StorageConfig()  # type: ignore[call-arg]

    def test_string_dataset_root_coerces_to_path(self) -> None:
        assert StorageConfig(dataset_root='/data').dataset_root == Path('/data')

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(ValidationError):
            StorageConfig(dataset_root='/data', unknown='x')  # type: ignore[call-arg]

    def test_is_frozen(self) -> None:
        config = StorageConfig(dataset_root='/data')
        with pytest.raises(ValidationError):
            config.dataset_root = Path('/other')  # type: ignore[misc]
