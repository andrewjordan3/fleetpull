# src/fleetpull/model_contract/coercions.py
"""Type-recovery coercion for stringly wire values.

Value-level wire-cleaning on a model is allowed only where recovering
the declared type is structural (DESIGN section 9): a field typed
``int`` receiving ``""`` cannot validate at all, so a before-validator
lifts the empty string ahead of parsing (Motive ``VehicleSummary.year``
is the shipped case). String fields never use this — models preserve
``""`` faithfully from the wire, and empty strings normalize to null
once, at the DataFrame boundary (``records.normalize_empty_strings``).
A formatted value someone chose (``"22.3 mi"``) is never touched
anywhere: parsing it would presume a use case.
"""

__all__: list[str] = ['empty_str_to_none']


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
