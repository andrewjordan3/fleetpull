"""Tree-wide enforcement of the canonical-UTC temporal discipline over src/fleetpull.

Ruff's DTZ rules catch naive-datetime construction, but not a timezone-aware
``datetime.now(tz=UTC)`` (legal by DTZ, yet it bypasses the injectable
``Clock`` seam) and not a foreign tzinfo entering the domain (``zoneinfo``,
``timezone(...)``), which the canonical-UTC identity guards in
``timing/canon.py`` would reject only later, at a distance from the cause.
This AST walk covers those two rules:

    Rule 1 — no direct wall-clock reads outside ``timing/``:
             ``datetime.now(...)``, ``datetime.today()``, ``date.today()``,
             ``datetime.utcnow()``. Current time flows through the injected
             ``Clock``.
    Rule 2 — no tzinfo construction or zoneinfo usage outside ``timing/``:
             ``zoneinfo`` imports and ``timezone(...)`` construction.
             Referencing the canonical constant (``datetime.UTC`` /
             ``timezone.utc``) stays legal everywhere — a reference is not a
             construction.

``timing/`` is the allowlist: it owns the one wall-clock read
(``SystemClock``) and the canonicalization surface itself. Tests are exempt
(they legitimately construct foreign tzinfo to exercise rejection). The
check is AST-based,
not textual, so a docstring mentioning ``datetime.now`` never trips it.
"""

import ast
import pathlib

import fleetpull

PACKAGE_ROOT: pathlib.Path = pathlib.Path(fleetpull.__file__).parent

# The one subpackage allowed to read the wall clock and construct tzinfo.
_ALLOWLISTED_SUBPACKAGE = 'timing'

# Wall-clock reading methods on the datetime/date types (Rule 1).
_WALL_CLOCK_METHODS: frozenset[str] = frozenset({'now', 'today', 'utcnow'})

# Names the stdlib datetime/date types are bound to at call sites, in both
# import styles: `from datetime import datetime, date` and `import datetime`.
_DATETIME_TYPE_NAMES: frozenset[str] = frozenset({'datetime', 'date'})


def _names_datetime_type(node: ast.expr) -> bool:
    """Whether an expression denotes the stdlib ``datetime``/``date`` type.

    True for the bare names ``datetime`` / ``date`` (the from-import style
    this codebase uses) and for the module-qualified ``datetime.datetime`` /
    ``datetime.date``.
    """
    if isinstance(node, ast.Name):
        return node.id in _DATETIME_TYPE_NAMES
    if isinstance(node, ast.Attribute):
        return (
            isinstance(node.value, ast.Name)
            and node.value.id == 'datetime'
            and node.attr in _DATETIME_TYPE_NAMES
        )
    return False


def _call_violation(node: ast.Call) -> str | None:
    """The diagnostic for a forbidden call, or ``None`` when the call is fine."""
    func = node.func
    if (
        isinstance(func, ast.Attribute)
        and func.attr in _WALL_CLOCK_METHODS
        and _names_datetime_type(func.value)
    ):
        return f'rule 1 — wall-clock read {func.attr!r} bypasses the injected Clock'
    # timezone(...) construction, from-import style. `astimezone(...)` is a
    # distinct attribute name and never matches; `timezone.utc` is an
    # attribute reference, not a call of `timezone`.
    if isinstance(func, ast.Name) and func.id == 'timezone':
        return 'rule 2 — timezone(...) constructs a non-canonical tzinfo'
    # timezone(...) construction, module-qualified style.
    if (
        isinstance(func, ast.Attribute)
        and func.attr == 'timezone'
        and isinstance(func.value, ast.Name)
        and func.value.id == 'datetime'
    ):
        return 'rule 2 — datetime.timezone(...) constructs a non-canonical tzinfo'
    return None


def _import_violation(node: ast.Import | ast.ImportFrom) -> str | None:
    """The diagnostic for a zoneinfo import, or ``None`` when the import is fine."""
    if isinstance(node, ast.Import):
        imported_names = [alias.name for alias in node.names]
    else:
        imported_names = [node.module or '']
    for name in imported_names:
        if name == 'zoneinfo' or name.startswith('zoneinfo.'):
            return 'rule 2 — zoneinfo brings a non-canonical tzinfo into the domain'
    return None


def _all_checked_files() -> list[pathlib.Path]:
    """Every source file under the package except the allowlisted subpackage."""
    return sorted(
        path
        for path in PACKAGE_ROOT.rglob('*.py')
        if '__pycache__' not in path.parts
        and path.relative_to(PACKAGE_ROOT).parts[0] != _ALLOWLISTED_SUBPACKAGE
    )


def _check_file(path: pathlib.Path) -> list[str]:
    """Return one diagnostic per violation in ``path`` (empty when clean)."""
    tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    violations: list[str] = []
    for node in ast.walk(tree):
        match node:
            case ast.Call():
                diagnostic = _call_violation(node)
                lineno = node.lineno
            case ast.Import() | ast.ImportFrom():
                diagnostic = _import_violation(node)
                lineno = node.lineno
            case _:
                continue
        if diagnostic is not None:
            location = f'{path.relative_to(PACKAGE_ROOT.parent)}:{lineno}'
            violations.append(f'{location}: {diagnostic}')
    return violations


def test_no_temporal_discipline_violations() -> None:
    all_violations: list[str] = []
    for path in _all_checked_files():
        all_violations.extend(_check_file(path))
    assert not all_violations, 'temporal-discipline violations:\n' + '\n'.join(
        all_violations
    )
