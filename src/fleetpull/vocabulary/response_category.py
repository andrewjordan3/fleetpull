# src/fleetpull/vocabulary/response_category.py
"""
The closed response-classification vocabulary.

Shared, dependency-free package vocabulary: the classification layer
produces it, the client dispatches on it, the retry layer takes it as
input, and ``RetriesExhaustedError`` carries it on a public field.
Homed in a leaf that imports nothing internal so every layer — root
exceptions included — can depend on it without forming a cycle.
"""

from enum import StrEnum

__all__: list[str] = ['ResponseCategory']


class ResponseCategory(StrEnum):
    """
    Closed vocabulary of "what the client does next."

    Each member earns its slot by demanding a distinct client action.
    Closure invariant (DESIGN.md §8): a new category is admissible only
    if it arrives with a new client action.
    """

    SUCCESS = 'success'  # parse and yield records
    TRANSIENT = 'transient'  # retry with backoff
    RATE_LIMITED = 'rate_limited'  # penalize the shared quota scope, then retry
    AUTH_FAILURE = (
        'auth_failure'  # ask the auth strategy whether one retry is worthwhile
    )
    FATAL = 'fatal'  # raise
