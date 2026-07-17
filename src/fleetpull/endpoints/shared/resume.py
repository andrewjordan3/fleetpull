# src/fleetpull/endpoints/shared/resume.py
"""The shared resume-value type guard.

Every watermark spec-builder's first act is the same proof: the
``ResumeValue`` it was handed is the ``DateWindow`` a watermark endpoint
always resumes from, and anything else is a wiring bug that must fail
loudly before a request is built. The guard is a pure type narrowing
with no provider-specific behavior, so it lives on the shared surface --
placement here couples nothing.
"""

from fleetpull.endpoints.shared.base import ResumeValue
from fleetpull.incremental import DateWindow

__all__: list[str] = ['require_date_window']


def require_date_window(resume: ResumeValue, requirer: str) -> DateWindow:
    """Narrow a resume value to the ``DateWindow`` a watermark run carries.

    Args:
        resume: The resume value handed to a spec-builder -- a
            ``DateWindow`` for a watermark endpoint; any other value is a
            wiring bug.
        requirer: The name blamed in the error, conventionally the
            calling builder's class name.

    Returns:
        ``resume``, narrowed to ``DateWindow``.

    Raises:
        TypeError: ``resume`` is not a ``DateWindow``.

    Side Effects:
        None.
    """
    if not isinstance(resume, DateWindow):
        raise TypeError(
            f'{requirer} requires a DateWindow resume, got {type(resume).__name__}.'
        )
    return resume
