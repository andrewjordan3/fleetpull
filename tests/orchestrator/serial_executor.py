"""The determinism seam for fan-out tests: a synchronous same-thread executor.

Injected through the same ``FetchPool`` seam the production
``ThreadPoolExecutor`` rides, so every fan-out test that does not need real
overlap runs the identical channel machinery with fully deterministic
execution: ``submit`` runs the task immediately on the calling thread and
returns an already-settled ``Future``. Under submission-order draining this
reproduces the serial member loop's observable behavior exactly.

Not a test module (no ``test_`` prefix); fan-out test modules import it.
"""

from collections.abc import Callable
from concurrent.futures import Executor, Future


class SerialExecutor(Executor):
    """An ``Executor`` that runs each submitted task inline, synchronously."""

    def submit[**TaskParams, TaskResult](
        self,
        fn: Callable[TaskParams, TaskResult],
        /,
        *args: TaskParams.args,
        **kwargs: TaskParams.kwargs,
    ) -> Future[TaskResult]:
        future: Future[TaskResult] = Future()
        try:
            result = fn(*args, **kwargs)
        # The Executor contract: any failure is captured into the Future and
        # re-raised at result(), exactly as ThreadPoolExecutor does.
        except BaseException as error:
            future.set_exception(error)
        else:
            future.set_result(result)
        return future
