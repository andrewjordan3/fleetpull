"""Tree-wide enforcement of the four-clause import rule over src/fleetpull.

``import-linter`` (the fifth gate) sees module-to-module edges but not
import *style* (face vs submodule) or re-export shape. This AST walk
covers the clauses it cannot:

    Clause 1 — no module imports from a directory it lives inside
               (no child-into-parent).
    Clause 3 — crossing into a different package routes through that
               package's ``__init__`` face, never a submodule of it.
    Clause 4 — a package ``__init__`` re-exports only its own package's
               submodules, never a foreign package's symbols.

Clause 2 (siblings file-direct) needs no enforcement — it is the absence
of face-routing within a directory, which the other clauses already
permit. The check runs inside the ``pytest`` gate, so it rides every
run without a separate command.
"""

import ast
import pathlib

import fleetpull

PACKAGE_ROOT: pathlib.Path = pathlib.Path(fleetpull.__file__).parent
PACKAGE_PREFIX = 'fleetpull'


def _module_name_for(path: pathlib.Path) -> str:
    """Dotted ``fleetpull...`` module name for a source file path."""
    relative = path.relative_to(PACKAGE_ROOT.parent).with_suffix('')
    parts = list(relative.parts)
    if parts[-1] == '__init__':
        parts = parts[:-1]
    return '.'.join(parts)


def _is_package(module_name: str) -> bool:
    """True when the dotted name resolves to a package (a directory)."""
    relative = pathlib.Path(*module_name.split('.')[1:])
    return (PACKAGE_ROOT / relative / '__init__.py').is_file()


def _internal_imports(path: pathlib.Path) -> list[tuple[int, str]]:
    """Every ``from fleetpull... import`` target in a file, with line numbers."""
    tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    targets: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level == 0:
            module = node.module or ''
            if module == PACKAGE_PREFIX or module.startswith(f'{PACKAGE_PREFIX}.'):
                targets.append((node.lineno, module))
    return targets


def _all_source_files() -> list[pathlib.Path]:
    return sorted(
        path for path in PACKAGE_ROOT.rglob('*.py') if '__pycache__' not in path.parts
    )


def _check_file(path: pathlib.Path) -> list[str]:
    """Return one diagnostic per violating import in ``path`` (empty when clean)."""
    importer = _module_name_for(path)
    importer_package = importer.rsplit('.', 1)[0] if '.' in importer else importer
    is_init = path.name == '__init__.py'
    # For an __init__, the package it builds is the module name itself;
    # for a regular module, the package it lives in is its parent.
    own_package = importer if is_init else importer_package

    violations: list[str] = []
    for lineno, target in _internal_imports(path):
        location = f'{path.relative_to(PACKAGE_ROOT.parent)}:{lineno}'

        # Clause 4 — a face re-exports only its own package's submodules.
        if is_init and not (
            target == own_package or target.startswith(f'{own_package}.')
        ):
            violations.append(
                f'{location}: clause 4 — face re-exports foreign symbol '
                f'from {target!r} (own package is {own_package!r})'
            )
            continue

        # The face building its own surface (OK-build) and any same-package
        # reach are legal regardless of the clauses below.
        if target == own_package or target.startswith(f'{own_package}.'):
            continue

        # Same-directory sibling (clause 2): target lives in the importer's
        # own package. Covered by the own_package check above for non-inits;
        # nothing further to assert.

        # Clause 1 — the target package must not be a strict ancestor
        # directory the importer lives inside.
        if importer.startswith(f'{target}.'):
            violations.append(
                f'{location}: clause 1 — child imports ancestor directory {target!r}'
            )
            continue

        # Clause 3 — crossing into a foreign package must name the package
        # face, not a submodule inside it. A package target IS a face, so
        # exempt. A module target is exempt only when it is a root-level
        # single-file module (parent is the root package, so it has no
        # face and is its own unit, e.g. fleetpull.exceptions); a module
        # inside a foreign SUBPACKAGE must route through that subpackage's
        # face.
        if _is_package(target):
            continue
        parent_of_target = target.rsplit('.', 1)[0] if '.' in target else target
        if parent_of_target != PACKAGE_PREFIX and _is_package(parent_of_target):
            violations.append(
                f'{location}: clause 3 — cross-directory import names '
                f'submodule {target!r}, not the package face '
                f'{parent_of_target!r}'
            )
    return violations


def test_no_import_discipline_violations() -> None:
    all_violations: list[str] = []
    for path in _all_source_files():
        all_violations.extend(_check_file(path))
    assert not all_violations, 'import-discipline violations:\n' + '\n'.join(
        all_violations
    )
