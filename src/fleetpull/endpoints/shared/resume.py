# src/fleetpull/endpoints/shared/resume.py
"""The shared resume-value type guards.

Every incremental spec-builder's first act is the same proof: the
``ResumeValue`` it was handed is the shape its endpoint always resumes from
-- the ``DateWindow`` for a watermark endpoint, a ``FeedToken`` or
``FeedSeed`` for a feed endpoint -- and anything else is a wiring bug that
must fail loudly before a request is built. The guards are pure type
narrowings with no provider-specific behavior, so they live on the shared
surface -- placement here couples nothing.
"""

from fleetpull.endpoints.shared.base import ResumeValue
from fleetpull.incremental import DateWindow, FeedResume, FeedSeed, FeedToken

__all__: list[str] = ['require_date_window', 'require_feed_resume']


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


def require_feed_resume(resume: ResumeValue, requirer: str) -> FeedResume:
    """Narrow a resume value to the seed-or-token a feed run carries.

    A feed endpoint always resumes from something -- the cold-start
    ``FeedSeed`` on the tokenless first run, the stored ``FeedToken`` on
    every run after -- so ``None`` (like any other shape) is a wiring bug,
    not a bootstrap case.

    Args:
        resume: The resume value handed to a spec-builder -- a ``FeedSeed``
            or ``FeedToken`` for a feed endpoint; any other value is a
            wiring bug.
        requirer: The name blamed in the error, conventionally the
            calling builder's class name.

    Returns:
        ``resume``, narrowed to ``FeedSeed | FeedToken``.

    Raises:
        TypeError: ``resume`` is neither a ``FeedSeed`` nor a ``FeedToken``.

    Side Effects:
        None.
    """
    if not isinstance(resume, FeedSeed | FeedToken):
        raise TypeError(
            f'{requirer} requires a FeedSeed or FeedToken resume, '
            f'got {type(resume).__name__}.'
        )
    return resume
