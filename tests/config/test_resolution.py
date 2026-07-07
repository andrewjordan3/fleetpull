"""Tests for fleetpull.config.resolution -- the pure functions, no models.

Each function takes a raw document mapping and returns one; these tests
feed dicts directly and never construct a config model, proving the
resolution layer stands alone.
"""

from pathlib import Path

from fleetpull.config.resolution import (
    with_log_path_defaulted,
    with_provider_knobs_applied,
    with_state_path_defaulted,
)


def _document_with_knobs(
    sync_extra: dict[str, int], motive_extra: dict[str, int]
) -> dict[str, object]:  # typing-justified: a raw YAML-shaped document
    return {
        'sync': {'default_start_date': '2026-06-01', **sync_extra},
        'storage': {'dataset_root': '/data'},
        'providers': {'motive': {'endpoints': ['vehicles'], **motive_extra}},
    }


class TestProviderKnobPrecedence:
    def test_sync_knobs_fan_into_a_provider_without_its_own(self) -> None:
        document = _document_with_knobs({'lookback_days': 3, 'cutoff_days': 1}, {})
        resolved = with_provider_knobs_applied(document)
        motive = resolved['providers']['motive']  # type: ignore[index]
        assert motive['lookback_days'] == 3
        assert motive['cutoff_days'] == 1

    def test_a_provider_key_wins_over_the_sync_key(self) -> None:
        document = _document_with_knobs({'lookback_days': 3}, {'lookback_days': 9})
        resolved = with_provider_knobs_applied(document)
        motive = resolved['providers']['motive']  # type: ignore[index]
        assert motive['lookback_days'] == 9

    def test_no_sync_knobs_changes_nothing(self) -> None:
        document = _document_with_knobs({}, {})
        resolved = with_provider_knobs_applied(document)
        assert 'lookback_days' not in resolved['providers']['motive']  # type: ignore[index]

    def test_knobs_fan_independently(self) -> None:
        document = _document_with_knobs({'cutoff_days': 2}, {'lookback_days': 9})
        resolved = with_provider_knobs_applied(document)
        motive = resolved['providers']['motive']  # type: ignore[index]
        assert motive == {
            'endpoints': ['vehicles'],
            'lookback_days': 9,
            'cutoff_days': 2,
        }

    def test_malformed_sections_pass_through(self) -> None:
        document = {'sync': 5, 'providers': {'motive': {}}}
        assert with_provider_knobs_applied(document) == document
        document = {'sync': {'lookback_days': 1}, 'providers': 'oops'}
        assert with_provider_knobs_applied(document) == document

    def test_non_mapping_provider_entries_pass_through(self) -> None:
        document = {
            'sync': {'lookback_days': 1},
            'providers': {'motive': 'oops'},
        }
        resolved = with_provider_knobs_applied(document)
        assert resolved['providers'] == {'motive': 'oops'}


class TestStatePathDefault:
    def test_defaults_under_dataset_root_when_absent(self) -> None:
        document = {'storage': {'dataset_root': '/data'}}
        resolved = with_state_path_defaulted(document)
        assert resolved['state'] == {
            'database_path': Path('/data/.fleetpull/state.sqlite3')
        }

    def test_an_explicit_path_stands(self) -> None:
        document = {
            'storage': {'dataset_root': '/data'},
            'state': {'database_path': '/elsewhere/db.sqlite3'},
        }
        assert with_state_path_defaulted(document) == document

    def test_unreadable_dataset_root_changes_nothing(self) -> None:
        for storage in ({}, {'dataset_root': 5}, {'dataset_root': '   '}, 'oops'):
            document = {'storage': storage}
            assert with_state_path_defaulted(document) == document


class TestLogPathDefault:
    def test_file_level_alone_injects_the_default_path(self) -> None:
        document = {
            'storage': {'dataset_root': '/data'},
            'logging': {'file_level': 'INFO'},
        }
        resolved = with_log_path_defaulted(document)
        assert resolved['logging'] == {
            'file_level': 'INFO',
            'file_path': Path('/data/.fleetpull/fleetpull.log'),
        }

    def test_an_explicit_file_path_stands(self) -> None:
        document = {
            'storage': {'dataset_root': '/data'},
            'logging': {'file_level': 'INFO', 'file_path': '/var/log/fp.log'},
        }
        assert with_log_path_defaulted(document) == document

    def test_file_path_alone_needs_no_help(self) -> None:
        document = {
            'storage': {'dataset_root': '/data'},
            'logging': {'file_path': '/var/log/fp.log'},
        }
        assert with_log_path_defaulted(document) == document

    def test_neither_file_key_changes_nothing(self) -> None:
        document = {
            'storage': {'dataset_root': '/data'},
            'logging': {'console_level': 'INFO'},
        }
        assert with_log_path_defaulted(document) == document

    def test_absent_logging_section_changes_nothing(self) -> None:
        document = {'storage': {'dataset_root': '/data'}}
        assert with_log_path_defaulted(document) == document
