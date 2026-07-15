# src/fleetpull/model_contract/coercions.py
"""Wire-error coercions response models opt into per field.

The coercion boundary rule: fix what is obviously a wire error, keep what
is merely ugly. An empty string standing where a value is absent is the
error class handled here — providers emit ``""`` where ``null`` belongs,
and consumers lose hours to the difference. A formatted value someone
chose (``"22.3 mi"``) is never touched: parsing it would presume a use
case. Nothing applies globally — fields opt in via the ``Annotated``
alias, so shipped models that deliberately mirror empty-string sentinels
(the GeoTab Device VIN fields) are unaffected.
"""

from typing import Annotated

from pydantic import BeforeValidator

__all__: list[str] = ['EmptyStrIsNone', 'empty_str_to_none']


# A BeforeValidator receives the raw wire value, and the layering
# contract bars model_contract from vocabulary's JsonValue.
# typing-justified: object is the strictest annotation available here.
def empty_str_to_none(value: object) -> object:
    """Lift a bare empty string to ``None``; everything else passes through.

    Args:
        value: The raw wire value, ahead of field validation.

    Returns:
        ``None`` when ``value`` is exactly ``''``; otherwise ``value``
        unchanged.
    """
    if value == '':
        return None
    return value


EmptyStrIsNone = Annotated[str | None, BeforeValidator(empty_str_to_none)]
