"""AsyncRunner: every operation in this process, on one event loop."""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING

from interloper.operation.base import OperationResult
from interloper.runner.base import Runner
from interloper.runner.run import Run

if TYPE_CHECKING:
    from interloper.operation.base import Operation


class AsyncRun(Run):
    """One run of an AsyncRunner: every operation executes in this process, on its event loop.

    A sync ``data()`` is offloaded to a thread by the asset itself, so one
    event loop serves the whole run.
    """

    async def execute(self, operation: Operation) -> OperationResult:
        """Execute the operation here, under its retry policy, reporting each retry as it happens.

        Args:
            operation: The operation to execute.

        Returns:
            Its result.
        """
        return await OperationResult.from_execution(
            operation,
            self.state.context(operation),
            on_retry=partial(self.state.retry, operation),
            on_restart=partial(self.state.start, operation),
        )


class AsyncRunner(Runner):
    """Run every operation in this process: ``max_workers`` at a time, sync ones offloaded to threads.

    ``AsyncRunner(max_workers=1)`` is serial and deterministic (see
    `SerialRunner`); the default runs four operations at once:

    ```py
    result = await AsyncRunner(max_workers=2, on_event=log_event).run(dag)   # async
    result = il.run(AsyncRunner(on_event=log_event).run(dag))                 # sync edge
    ```
    """

    Run = AsyncRun

    max_workers: int = 4
    fail_fast: bool = True
    reraise: bool = False

    @property
    def capacity(self) -> int:
        """How many operations a run executes at once: ``max_workers``."""
        return self.max_workers
