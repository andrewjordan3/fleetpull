# tests/paths/test_resolution.py
"""Tests for fleetpull.paths.resolution."""

from pathlib import Path

import pytest

from fleetpull.paths import resolve_path


class TestExpansion:
    def test_expands_bare_home(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.setenv('HOME', str(tmp_path))
        assert resolve_path('~') == tmp_path

    def test_expands_home_with_subpath(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.setenv('HOME', str(tmp_path))
        assert resolve_path('~/data/raw') == tmp_path / 'data' / 'raw'

    def test_expands_home_from_path_input(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.setenv('HOME', str(tmp_path))
        assert resolve_path(Path('~/data')) == tmp_path / 'data'


class TestAnchoring:
    def test_anchors_relative_path_to_cwd(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.chdir(tmp_path)
        assert resolve_path('data/raw') == tmp_path / 'data' / 'raw'

    def test_leaves_absolute_path_anchored(self, tmp_path: Path) -> None:
        absolute_input = tmp_path / 'data'
        assert resolve_path(absolute_input) == absolute_input

    def test_result_is_always_absolute(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.chdir(tmp_path)
        assert resolve_path('data').is_absolute()


class TestNormalization:
    def test_collapses_parent_references(self, tmp_path: Path) -> None:
        cluttered = tmp_path / 'a' / 'b' / '..' / 'c'
        assert resolve_path(cluttered) == tmp_path / 'a' / 'c'

    def test_collapses_redundant_separators_and_dots(self, tmp_path: Path) -> None:
        cluttered = f'{tmp_path}//a/./b'
        assert resolve_path(cluttered) == tmp_path / 'a' / 'b'

    def test_does_not_dereference_symlinks(self, tmp_path: Path) -> None:
        real_directory = tmp_path / 'real'
        real_directory.mkdir()
        link = tmp_path / 'link'
        link.symlink_to(real_directory)
        assert resolve_path(link / 'file.txt') == link / 'file.txt'

    def test_does_not_require_existence(self, tmp_path: Path) -> None:
        missing = tmp_path / 'does' / 'not' / 'exist'
        assert resolve_path(missing) == missing


class TestRejections:
    @pytest.mark.parametrize('blank', ['', '   ', '\t', '\n'])
    def test_rejects_empty_or_whitespace_strings(self, blank: str) -> None:
        with pytest.raises(ValueError, match='must not be empty'):
            resolve_path(blank)

    @pytest.mark.parametrize('bad_value', [123, 3.14, None, ['x']])
    def test_rejects_unsupported_types(
        self, bad_value: int | float | None | list[str]
    ) -> None:
        with pytest.raises(TypeError, match='str or Path'):
            resolve_path(bad_value)  # type: ignore[arg-type]

    def test_wraps_home_expansion_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def raise_runtime_error(self: Path) -> Path:
            raise RuntimeError('no home directory')

        monkeypatch.setattr(Path, 'expanduser', raise_runtime_error)
        with pytest.raises(ValueError, match='user home'):
            resolve_path('~/data')
