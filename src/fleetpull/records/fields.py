# src/fleetpull/records/fields.py
"""The shared field walk: classify each model field's annotation and
enumerate the flat leaf columns a model produces.

Schema derivation and record flattening both consume this one walk, so
the column NAME a field produces (the type side) and the value PULLED for
it (the value side) can never drift -- they are computed from a single
traversal. Nested models flatten with double-underscore-joined column
names; top-level fields keep their bare name.
"""

import enum
import types
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel

__all__: list[str] = [
    'FieldKind',
    'FlatField',
    'classify_annotation',
    'iter_flat_fields',
]

# Column-name join between a nested model and its child. Double, not
# single: field names already contain single underscores, so a single
# separator is ambiguous about the level boundary and lets a top-level
# field collide with a nested one. Module-private Final.
_NESTING_JOIN: str = '__'

# The closed scalar set. datetime resolves to a tz-aware microsecond
# dtype downstream and timedelta to a microsecond duration; the others
# to their obvious Polars scalars.
_SCALAR_TYPES: frozenset[type] = frozenset({int, float, str, bool, datetime, timedelta})


class FieldKind(enum.Enum):
    """How a resolved field annotation maps to a column.

    A closed set the walk and the schema both dispatch over.
    """

    SCALAR = enum.auto()
    ENUM = enum.auto()
    LIST_OF_SCALAR = enum.auto()
    NESTED_MODEL = enum.auto()


@dataclass(frozen=True, slots=True)
class FlatField:
    """One leaf column a model produces.

    Attributes:
        column: The flattened column name (nested levels double-underscore
            joined; a top-level field keeps its bare name).
        kind: The leaf's classification (never NESTED_MODEL -- nesting is
            resolved away into the column path).
        resolved: The resolved leaf type -- the scalar ``type`` for SCALAR,
            the ``Enum`` subclass for ENUM, the inner scalar ``type`` for
            LIST_OF_SCALAR.
        path: The attribute-access path from the root model to this leaf,
            used to pull the value.
    """

    column: str
    kind: FieldKind
    resolved: type
    path: tuple[str, ...]


def classify_annotation(
    # typing-justified: annotation forms (unions, aliases) are not `type` values
    annotation: Any,
    owning_model_name: str,
    field_name: str,
) -> tuple[FieldKind, type]:
    """Reduce a field annotation to a kind and its resolved leaf type.

    Unwraps ``Optional`` / ``T | None``; rejects multi-arm unions. A
    nested ``BaseModel`` is NESTED_MODEL (the walk recurses); an ``Enum``
    subclass is ENUM; one of the closed scalars is SCALAR; ``list[scalar]``
    is LIST_OF_SCALAR. Anything else -- ``Any``, ``dict``, ``Literal``,
    a list of models, an untyped ``list`` -- is a derivation gap and
    raises (fail fast; there is no override path yet).

    Args:
        annotation: The Pydantic ``FieldInfo.annotation`` to classify.
        owning_model_name: The model class name, for error messages.
        field_name: The field name, for error messages.

    Returns:
        ``(kind, resolved)`` -- the nested model class for NESTED_MODEL,
        the enum class for ENUM, the scalar type for SCALAR, the inner
        scalar type for LIST_OF_SCALAR.

    Raises:
        TypeError: When the annotation does not resolve to a supported
            kind.
    """
    # typing-justified: union arms are arbitrary annotation forms
    non_none: list[Any] = _strip_none_from_union(annotation)
    if len(non_none) != 1:
        raise TypeError(
            f'{owning_model_name}.{field_name}: cannot derive a column from '
            f'{annotation!r} -- a single non-None type is required, '
            f'found {len(non_none)}.'
        )
    candidate: Any = non_none[0]  # typing-justified: an arbitrary annotation form

    if isinstance(candidate, type) and issubclass(candidate, BaseModel):
        return (FieldKind.NESTED_MODEL, candidate)
    if isinstance(candidate, type) and issubclass(candidate, enum.Enum):
        return (FieldKind.ENUM, candidate)
    if candidate in _SCALAR_TYPES:
        return (FieldKind.SCALAR, candidate)
    if get_origin(candidate) is list:
        # typing-justified: get_args yields arbitrary annotation forms (typeshed)
        inner: tuple[Any, ...] = get_args(candidate)
        if len(inner) == 1 and inner[0] in _SCALAR_TYPES:
            return (FieldKind.LIST_OF_SCALAR, inner[0])
        raise TypeError(
            f'{owning_model_name}.{field_name}: cannot derive a column from '
            f'{annotation!r} -- only list[scalar] is supported, not lists '
            f'of models or untyped lists.'
        )
    raise TypeError(
        f'{owning_model_name}.{field_name}: cannot derive a column from '
        f'{annotation!r} -- unsupported annotation (no schema-override path '
        f'exists yet; model the shape or narrow the type).'
    )


def iter_flat_fields(model_class: type[BaseModel]) -> Iterator[FlatField]:
    """Yield one ``FlatField`` per leaf column the model produces.

    Recurses into nested models, double-underscore joining each level into
    the column name and extending the attribute path. Top-level scalar
    fields keep their bare name and a single-element path.

    Args:
        model_class: The model class to walk.

    Yields:
        One ``FlatField`` per leaf, in declaration order (depth-first).

    Raises:
        TypeError: Propagated from ``classify_annotation`` for any field
            whose annotation cannot be resolved.
    """
    yield from _walk(model_class, name_prefix=(), path_prefix=())


def _walk(
    model_class: type[BaseModel],
    name_prefix: tuple[str, ...],
    path_prefix: tuple[str, ...],
) -> Iterator[FlatField]:
    """Depth-first leaf walk; see :func:`iter_flat_fields`."""
    for field_name, field_info in model_class.model_fields.items():
        kind, resolved = classify_annotation(
            annotation=field_info.annotation,
            owning_model_name=model_class.__name__,
            field_name=field_name,
        )
        path: tuple[str, ...] = (*path_prefix, field_name)
        if kind is FieldKind.NESTED_MODEL:
            yield from _walk(
                model_class=resolved,
                name_prefix=(*name_prefix, field_name),
                path_prefix=path,
            )
        else:
            column: str = _NESTING_JOIN.join((*name_prefix, field_name))
            yield FlatField(column=column, kind=kind, resolved=resolved, path=path)


# typing-justified: annotation forms (unions, aliases) in, annotation forms out
def _strip_none_from_union(annotation: Any) -> list[Any]:
    """Return the non-``NoneType`` arms of a union, else ``[annotation]``."""
    origin: Any = get_origin(annotation)  # typing-justified: typeshed returns Any
    if origin is Union or origin is types.UnionType:
        return [
            argument for argument in get_args(annotation) if argument is not type(None)
        ]
    return [annotation]
