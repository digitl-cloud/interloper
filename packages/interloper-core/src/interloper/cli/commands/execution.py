"""What ``interloper run`` and ``interloper launch`` share: one operation's DAG, and executing a DAG to its exit.

A process started to execute one operation of a host's run (``--operation-id``)
executes it once, its upstreams read rather than executed, since the host owns
its retries and its lifecycle, and leaves what it came to in the termination
message the host reads back.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.events import EventRecorder
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
    from interloper.settings import AppSettings

    configure_console_logging(
        logging.WARNING if quiet else logging.DEBUG if verbose else logging.INFO,
        "%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s",
        "%H:%M:%S",
        log_format=AppSettings.get().log.format,
    )


def for_operation(dag: DAG, window: PartitionWindow | None, operation_id: str) -> tuple[DAG, PartitionWindow | None]:
    """The DAG and window executing one operation of *dag* for its host.

    The operation's upstreams are read, not executed, and it carries no retry
    policy, since the host owns its retries.

    Args:
        dag: The DAG the operation belongs to.
        window: The run's window.
        operation_id: The operation to execute.

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
    operation = operation_dag.operation_map[operation_id]
    operation.retry = None
    return operation_dag, operation.effective_window(window)


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
    """Execute *dag* to its end, its events through *recorder* or the runner's own ``on_event``, and exit as it ended.

    Args:
        runner: The runner to execute it with.
        dag: The DAG.
        window: The window it covers.
        metadata: Run-level metadata, spread into every event.
        operation_id: The one operation this process executes for its host,
            whose verdict it leaves in the termination message; ``None`` for a
            whole run.
        recorder: The recorder a platform run reports through; ``None``
            leaves the runner's own ``on_event``.

    Raises:
        SystemExit: If the run could not be executed or was lost, or did not
            complete.
    """
    from interloper.errors import PartitionError, RunLostError, format_exception
    from interloper.runner import ExecutionStatus

    enabled = [operation for operation in dag.operations if operation.enabled]
    logger.info(
        "Running DAG with %d enabled operation(s) (%d total) using %s",
        len(enabled),
        len(dag.operations),
        type(runner).__name__,
    )
    try:
        if recorder is None:
            result = asyncio.run(runner.run(dag, window, metadata=metadata or None))
        else:
            with recorder:
                recorded = runner.model_copy(update={"on_event": recorder, "reraise": False})
                result = asyncio.run(recorded.run(dag, window, metadata))
                recorder.finish(result)
    except Exception as error:
        leave_failure(operation_id, format_exception(error))
        if isinstance(error, (PartitionError, RunLostError)):
            raise SystemExit(f"Error: {error}") from error
        raise

    failed = result.executions[operation_id].result if operation_id is not None else None
    if failed is not None and not failed.succeeded:
        failed.write_termination_message()

    logger.info("Run completed: %s", result.status.name)

    if result.status != ExecutionStatus.COMPLETED:
        raise SystemExit(1)
