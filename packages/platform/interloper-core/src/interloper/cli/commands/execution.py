"""What ``interloper run`` and ``interloper launch`` share: one operation's DAG, and executing a DAG to its exit.

A process started to execute one operation of a host's run (``--operation-id``)
executes it once, here, its upstreams read rather than executed, and walks
nothing: the host owns the operation's retries and its lifecycle. It leaves
what the execution came to in the termination message the host reads back.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import nullcontext
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.events import EventRecorder
    from interloper.operation import OperationResult
    from interloper.partitioning.base import PartitionWindow
    from interloper.runner import Runner

logger = logging.getLogger(__name__)


def configure_logging(*, quiet: bool = False, verbose: bool = False) -> None:
    """Set up the one logging stack everything the command prints goes through.

    Framework log lines and run events share the same format, stream and level.

    Args:
        quiet: Only warnings and errors; takes precedence over *verbose*.
        verbose: Debug verbosity.
    """
    from interloper.cli.console import configure_console_logging
    from interloper.settings import LogSettings

    configure_console_logging(
        logging.WARNING if quiet else logging.DEBUG if verbose else logging.INFO,
        "%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s",
        "%H:%M:%S",
        log_format=LogSettings.get().format,
    )


def for_operation(
    dag: DAG, window: PartitionWindow | None, operation_id: str, metadata: dict[str, Any]
) -> tuple[DAG, PartitionWindow | None]:
    """The DAG and window executing one operation of *dag* for its host, its upstreams read, not executed.

    Args:
        dag: The DAG the operation belongs to.
        window: The run's window.
        operation_id: The operation to execute.
        metadata: The run's metadata.

    Returns:
        The operation's DAG, and the window it consumes from the run's.

    Raises:
        SystemExit: If the operation is not in the DAG.
    """
    from interloper.errors import AssetNotFoundError

    try:
        operation_dag = dag.mini_dag(operation_id)
    except AssetNotFoundError as error:
        raise SystemExit(f"Error: {error}") from error
    return operation_dag, operation_dag.operation_map[operation_id].effective_window(window, metadata)


def leave_failure(operation_id: str | None, reason: str) -> None:
    """Leave why an operation failed, before or while it executed, for the host that started this process.

    Args:
        operation_id: The operation this process executes for its host;
            ``None`` leaves nothing.
        reason: Why it failed.
    """
    from interloper.operation import OperationResult

    if operation_id is not None:
        OperationResult(error=reason).write_termination_message()


def execute(
    runner: Runner,
    dag: DAG,
    window: PartitionWindow | None,
    metadata: dict[str, Any],
    *,
    operation_id: str | None,
    recorder: EventRecorder | None = None,
) -> None:
    """Execute *dag*, or one of its operations for its host, and exit as it ended.

    A whole run walks the DAG with *runner*. One operation executes once, in
    this process, whatever kind of run *runner* makes, and a failed execution
    leaves its error in the termination message. Events reach *recorder*, or
    the runner's own ``on_event``.

    Args:
        runner: The runner to execute it with, whose ``on_event`` receives the
            events unless *recorder* is given.
        dag: The DAG.
        window: The window it covers.
        metadata: Run-level metadata, spread into every event.
        operation_id: The one operation this process executes for its host;
            ``None`` for a whole run.
        recorder: The recorder a platform run reports through; ``None``
            leaves the runner's own ``on_event``.

    Raises:
        SystemExit: If the run could not be executed or was lost, or did not
            complete.
    """
    from interloper.errors import PartitionError, RunLostError, format_exception
    from interloper.runner import ExecutionStatus

    if recorder is not None:
        runner = runner.model_copy(update={"on_event": recorder, "reraise": False})
    try:
        with recorder if recorder is not None else nullcontext():
            if operation_id is not None:
                completed = asyncio.run(_execute_here(runner, dag, window, metadata, operation_id)).succeeded
            else:
                enabled = [operation for operation in dag.operations if operation.enabled]
                logger.info(
                    "Running DAG with %d enabled operation(s) (%d total) using %s",
                    len(enabled),
                    len(dag.operations),
                    type(runner).__name__,
                )
                result = asyncio.run(runner.run(dag, window, metadata))
                if recorder is not None:
                    recorder.finish(result)
                logger.info("Run completed: %s", result.status.name)
                completed = result.status is ExecutionStatus.COMPLETED
    except Exception as error:
        leave_failure(operation_id, format_exception(error))
        if isinstance(error, (PartitionError, RunLostError)):
            raise SystemExit(f"Error: {error}") from error
        raise
    if not completed:
        raise SystemExit(1)


async def _execute_here(
    runner: Runner, dag: DAG, window: PartitionWindow | None, metadata: dict[str, Any], operation_id: str
) -> OperationResult:
    """Execute one operation of *dag* once, in this process, leaving a failure's error for the host.

    The run it executes in walks nothing, so it emits no lifecycle event: the
    host's run reports those.

    Args:
        runner: The runner whose ``on_event`` receives the execution's events.
        dag: The operation's DAG.
        window: The window the operation consumes.
        metadata: The host run's metadata.
        operation_id: The operation to execute.

    Returns:
        What the execution came to.
    """
    from interloper.runner import AsyncRun

    operation = dag.operation_map[operation_id]
    async with AsyncRun(runner, dag, window, metadata) as run:
        result = await run.execute(operation)
    if not result.succeeded:
        result.write_termination_message()
    logger.info("Operation %s %s", operation.key, "completed" if result.succeeded else "failed")
    return result
