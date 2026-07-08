"""Tree-wide enforcement of the typing discipline over src/fleetpull and tests.

CLAUDE.md's Type System rules, mechanically: no ``Any`` and no ``object``
in annotation position (parameters, returns, annotated assignments,
class attributes) unless the annotation's source line or the line
immediately above carries a ``# typing-justified: <reason>`` marker.
Two exemptions, both for ``object`` only — dunder methods whose typeshed
signatures require it (``__eq__`` and kin), and deliberate variadic
catch-alls (``*args`` / ``**kwargs``), where ``object`` is the strictest
annotation available. ``Any`` is never exempt.

AST-based, so prose in docstrings and comments never trips it (string
annotations are out of scope — the tree forbids
``from __future__ import annotations``, so none exist). The checker is a
pure function over source text, so the synthetic-shape tests below feed
it planted violations directly (the parity-checker pattern).
"""

import ast
import pathlib
from collections.abc import Iterator

import fleetpull

PACKAGE_ROOT: pathlib.Path = pathlib.Path(fleetpull.__file__).parent
TESTS_ROOT: pathlib.Path = pathlib.Path(__file__).parent

# The justification marker CLAUDE.md's Type System section names.
_MARKER = 'typing-justified:'

# The two names the discipline governs in annotation position.
_FLAGGED_NAMES: frozenset[str] = frozenset({'Any', 'object'})

# Dunders whose typeshed signatures force `object` parameters; their
# parameters (only) may carry `object` unmarked.
_OBJECT_REQUIRING_DUNDERS: frozenset[str] = frozenset(
    {'__eq__', '__ne__', '__contains__', '__setattr__', '__delattr__'}
)


def _flagged_names(annotation: ast.expr) -> Iterator[tuple[str, int]]:
    """Every ``Any``/``object`` name inside an annotation, with its line."""
    for node in ast.walk(annotation):
        if isinstance(node, ast.Name) and node.id in _FLAGGED_NAMES:
            yield node.id, node.lineno
        elif isinstance(node, ast.Attribute) and node.attr == 'Any':
            # Module-qualified typing.Any. An attribute named `object`
            # (foo.object) is not the builtin, so it is not flagged.
            yield 'Any', node.lineno


class _FileChecker:
    """Checks one module's annotations; holds the lines the marker scan reads."""

    def __init__(self, source: str, path_label: str) -> None:
        self._lines: list[str] = source.splitlines()
        self._path_label: str = path_label
        self.violations: list[str] = []

    def check_module(self, tree: ast.Module) -> None:
        for node in ast.walk(tree):
            match node:
                case ast.FunctionDef() | ast.AsyncFunctionDef():
                    self._check_function(node)
                case ast.AnnAssign():
                    self._check(
                        node.annotation,
                        symbol=ast.unparse(node.target),
                        allow_object=False,
                    )
                case _:
                    pass

    def _check_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        allow_object_parameters = node.name in _OBJECT_REQUIRING_DUNDERS
        plain_parameters = [
            *node.args.posonlyargs,
            *node.args.args,
            *node.args.kwonlyargs,
        ]
        for parameter in plain_parameters:
            if parameter.annotation is not None:
                self._check(
                    parameter.annotation,
                    symbol=f'parameter {parameter.arg!r} of {node.name}',
                    allow_object=allow_object_parameters,
                )
        # Variadic catch-alls: `object` is the strictest annotation available.
        for variadic in (node.args.vararg, node.args.kwarg):
            if variadic is not None and variadic.annotation is not None:
                self._check(
                    variadic.annotation,
                    symbol=f'parameter {variadic.arg!r} of {node.name}',
                    allow_object=True,
                )
        if node.returns is not None:
            self._check(
                node.returns, symbol=f'return of {node.name}', allow_object=False
            )

    def _check(self, annotation: ast.expr, symbol: str, allow_object: bool) -> None:
        for flagged_name, lineno in _flagged_names(annotation):
            if flagged_name == 'object' and allow_object:
                continue
            if self._justified(lineno):
                continue
            self.violations.append(
                f'{self._path_label}:{lineno}: {symbol} is annotated with '
                f'{flagged_name!r} and carries no {_MARKER} marker'
            )

    def _justified(self, annotation_lineno: int) -> bool:
        """Marker on the annotation's line or the line immediately above."""
        line_index = annotation_lineno - 1
        if _MARKER in self._lines[line_index]:
            return True
        return line_index > 0 and _MARKER in self._lines[line_index - 1]


def typing_violations(source: str, path_label: str) -> list[str]:
    """One diagnostic per unjustified ``Any``/``object`` annotation in ``source``."""
    checker = _FileChecker(source, path_label)
    checker.check_module(ast.parse(source, filename=path_label))
    return checker.violations


def _labeled_source_files() -> list[tuple[str, pathlib.Path]]:
    """Every checked file as (diagnostic label, path), across both trees."""
    src_files = [
        (str(path.relative_to(PACKAGE_ROOT.parent)), path)
        for path in PACKAGE_ROOT.rglob('*.py')
        if '__pycache__' not in path.parts
    ]
    test_files = [
        (f'tests/{path.relative_to(TESTS_ROOT)}', path)
        for path in TESTS_ROOT.rglob('*.py')
        if '__pycache__' not in path.parts
    ]
    return sorted(src_files + test_files)


def test_no_typing_discipline_violations() -> None:
    all_violations: list[str] = []
    for label, path in _labeled_source_files():
        all_violations.extend(
            typing_violations(path.read_text(encoding='utf-8'), label)
        )
    assert not all_violations, 'typing-discipline violations:\n' + '\n'.join(
        all_violations
    )


# --------------------------------------------------------------------------- #
# Permanent synthetic shapes: each direction of the rule, fed to the checker.
# --------------------------------------------------------------------------- #
def test_unjustified_object_parameter_fires() -> None:
    source = 'def guard(value: object) -> None:\n    return None\n'
    violations = typing_violations(source, 'planted.py')
    assert len(violations) == 1
    assert "parameter 'value' of guard" in violations[0]
    assert "'object'" in violations[0]


def test_unjustified_any_fires_including_nested_forms() -> None:
    source = 'def load(raw: Any) -> dict[str, Any]:\n    return {}\n'
    violations = typing_violations(source, 'planted.py')
    assert len(violations) == 2
    assert "parameter 'raw' of load" in violations[0]
    assert 'return of load' in violations[1]


def test_unjustified_annotated_assignment_fires() -> None:
    source = 'class Holder:\n    blob: Any = None\n'
    violations = typing_violations(source, 'planted.py')
    assert len(violations) == 1
    assert 'blob' in violations[0]


def test_marker_on_the_annotation_line_passes() -> None:
    source = 'value: object = None  # typing-justified: synthetic boundary\n'
    assert typing_violations(source, 'marked.py') == []


def test_marker_on_the_line_above_passes() -> None:
    source = (
        '# typing-justified: synthetic boundary\n'
        'def guard(value: object) -> None:\n'
        '    return None\n'
    )
    assert typing_violations(source, 'marked.py') == []


def test_object_requiring_dunder_is_exempt() -> None:
    source = (
        'class Point:\n'
        '    def __eq__(self, other: object) -> bool:\n'
        '        return True\n'
    )
    assert typing_violations(source, 'dunder.py') == []


def test_variadic_object_catch_all_is_exempt() -> None:
    source = 'def sink(*args: object, **kwargs: object) -> None:\n    return None\n'
    assert typing_violations(source, 'variadic.py') == []


def test_exemptions_never_cover_any() -> None:
    # The dunder and variadic exemptions are object-only; Any still fires.
    source = (
        'class Point:\n'
        '    def __eq__(self, other: Any) -> bool:\n'
        '        return True\n'
        'def sink(*args: Any) -> None:\n'
        '    return None\n'
    )
    violations = typing_violations(source, 'planted.py')
    assert len(violations) == 2
