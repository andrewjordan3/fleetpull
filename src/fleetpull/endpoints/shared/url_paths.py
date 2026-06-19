# src/fleetpull/endpoints/shared/url_paths.py
"""Strict URL-path template rendering for endpoint fan-out paths.

A pure leaf -- stdlib only, imports nothing internal. It renders a small, closed
template dialect: a URL path may carry named ``{placeholder}`` segments and
nothing else. Python's general ``str.format`` grammar (format specs, conversions,
attribute/item access, ``{{`` escapes) is deliberately not borrowed -- defining
the tiny dialect we want is less surface than subtracting the features we do not
from a large one.

Strict both ways and loud on any mismatch: the placeholder set must equal the
supplied-value key set (a missing or unused key is a binding/fan-out bug), no
value may be empty, and a template with malformed braces is rejected. Substituted
values are URL-encoded as a single path segment (``quote(..., safe='')``), so a
value like ``'abc/123'`` becomes ``'abc%2F123'`` and can never restructure the
path. That encoding is also load-bearing for safety: because an encoded value can
contain neither ``{`` nor ``}``, the literal placeholder replacement below can
never have a substituted value forge a second placeholder.

Errors are ``UrlPathTemplateError`` (a ``ValueError`` subclass defined here,
importing nothing internal) -- a template/key mismatch is a programmer bug with no
user input in the loop, so it raises and stays stdlib-typed, exactly as the timing
codec does.
"""

import re
from collections.abc import Mapping
from urllib.parse import quote

__all__: list[str] = ['UrlPathTemplateError', 'render_url_path_template']

_PLACEHOLDER_PATTERN = re.compile(r'\{([A-Za-z_][A-Za-z0-9_]*)\}')


class UrlPathTemplateError(ValueError):
    """Raised when a URL-path template or its supplied values are invalid."""


def render_url_path_template(path_template: str, path_values: Mapping[str, str]) -> str:
    """Render a strict URL-path template, URL-encoding each substituted value.

    Args:
        path_template: A URL path with zero or more named ``{placeholder}``
            segments (e.g. ``'/v3/vehicle_locations/{vehicle_id}'``). Only named
            placeholders are supported; format specs, conversions, attribute or
            item access, and escaped braces are not.
        path_values: The substitution values. Its key set must exactly equal the
            template's placeholder set, and no value may be empty.

    Returns:
        The rendered path with every placeholder replaced by its URL-encoded
        (single-segment) value. A template with no placeholders and an empty
        mapping is returned unchanged.

    Raises:
        UrlPathTemplateError: The template has malformed or unsupported braces;
            the value keys do not exactly match the placeholders (missing or
            unused keys); or a supplied value is empty.

    Side Effects:
        None -- pure function.
    """
    placeholder_names = _extract_placeholder_names(path_template)

    expected = set(placeholder_names)
    supplied = set(path_values)
    if expected != supplied:
        raise UrlPathTemplateError(
            _mismatch_message(
                path_template=path_template,
                missing=expected - supplied,
                unused=supplied - expected,
            )
        )

    rendered = path_template
    for placeholder_name in placeholder_names:
        value = path_values[placeholder_name]
        if value == '':
            raise UrlPathTemplateError(
                f'URL path value {placeholder_name!r} must not be empty.'
            )
        # safe='' encodes every reserved character, so the value is a single
        # inert path segment. The literal replace below relies on this: an
        # encoded value contains no braces, so it cannot forge a placeholder.
        rendered = rendered.replace(f'{{{placeholder_name}}}', quote(value, safe=''))
    return rendered


def _extract_placeholder_names(path_template: str) -> tuple[str, ...]:
    """Extract a template's placeholder names in first-seen order.

    A repeated placeholder is returned once -- it must take a single value. Any
    brace left after removing every valid placeholder means the template has
    malformed or unsupported brace syntax, which raises.

    Args:
        path_template: The template to inspect.

    Returns:
        The distinct placeholder names, in first-seen order.

    Raises:
        UrlPathTemplateError: The template contains malformed or unsupported brace
            syntax (a dangling brace, or a non-named-placeholder use).

    Side Effects:
        None -- pure function.
    """
    residual = _PLACEHOLDER_PATTERN.sub('', path_template)
    if '{' in residual or '}' in residual:
        raise UrlPathTemplateError(
            'URL path template has malformed or unsupported brace syntax: '
            f"{path_template!r}. Only named placeholders like '{{vehicle_id}}' "
            'are supported.'
        )

    seen: set[str] = set()
    ordered_names: list[str] = []
    for match in _PLACEHOLDER_PATTERN.finditer(path_template):
        name = match.group(1)
        if name not in seen:
            seen.add(name)
            ordered_names.append(name)
    return tuple(ordered_names)


def _mismatch_message(path_template: str, missing: set[str], unused: set[str]) -> str:
    """Build a precise placeholder/value mismatch message.

    Args:
        path_template: The template being rendered.
        missing: Placeholder names the template needs but the values omit.
        unused: Value keys the caller supplied but the template never uses.

    Returns:
        A human-readable validation message naming the missing and unused keys.

    Side Effects:
        None -- pure function.
    """
    parts = [f'URL path values do not match placeholders for {path_template!r}.']
    if missing:
        names = ', '.join(repr(name) for name in sorted(missing))
        parts.append(f'Missing: {names}.')
    if unused:
        names = ', '.join(repr(name) for name in sorted(unused))
        parts.append(f'Unused: {names}.')
    return ' '.join(parts)
