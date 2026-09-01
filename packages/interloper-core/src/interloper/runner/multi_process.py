"""Multi-process runner using concurrent.futures."""

from __future__ import annotations

import asyncio
import traceback
from concurrent.futures import Future, ProcessPoolExecutor
from typing import Any

from pydantic import PrivateAttr

from interloper.errors import RunnerError, format_exception
from interloper.operation.base import Operation, OperationContext, OperationResult
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runner.sync_runner import SyncRunner


def _worker(
    operation_id: str,
    dag_spec: dict[str, Any],
    partition_or_window: Partition | PartitionWindow | None,
    metadata: dict[str, Any],
) -> tuple[str, bool, str | None, str | None, dict[str, Any]]:
    """Execute a single operation in a worker process.

    Reconstructs the DAG from its serialized spec, looks up the target
    node by ID, and executes it. The operation's effects travel back as a
    plain dict so the parent process can record them on the execution info.

    Args:
        operation_id: Id of the node to execute within the reconstructed DAG.
        dag_spec: JSON-mode dump of the DAG spec to reconstruct.
        partition_or_window: Partition or window to scope the run; narrowed
            to the node's own effective partition.
        metadata: Run metadata, also carrying the parent span context.

    Returns:
        Tuple of ``(id, success, error_message, formatted_traceback, effects)``.
    """
    from opentelemetry import context as otel_context

    from interloper.dag import DAGSpec
    from interloper.settings import AppSettings
    from interloper.telemetry import extract_metadata, force_flush, init_telemetry

    # Fresh interpreter: re-init from the inherited environment; metadata
    # carries the parent span context.
    init_telemetry(AppSettings.get().otel)
    context = extract_metadata(metadata)
    token = otel_context.attach(context) if context is not None else None

    operation: Operation | None = None
    try:
        dag = DAGSpec(**dag_spec).reconstruct()
        operation = dag.operation_map[operation_id]
        result = asyncio.run(
            operation.execute(
                OperationContext(
                    partition_or_window=operation.effective_partition(partition_or_window),
                    dag=dag,
                    metadata=metadata,
                )
            )
        )
    except Exception as e:  # noqa: BLE001
        if operation is None:
            return (operation_id, False, format_exception(e), traceback.format_exc(), {})
        failed = operation.failure(e)
        tb = traceback.format_exc() if type(operation).capture_traceback else None
        effects = {"config": failed.config, "state": failed.state}
        return (operation_id, False, failed.error or format_exception(e), tb, effects)
    finally:
        if token is not None:
            otel_context.detach(token)
        # Pool workers are reused; exit hooks may never run.
        force_flush()
    return (operation_id, True, None, None, {"config": result.config, "state": result.state})


class MultiProcessRunner(SyncRunner):
    """Process-based parallel runner.

    Executes independent operations concurrently using a process pool.
    Each operation is serialized via :class:`~interloper.dag.base.DAGSpec`,
    reconstructed in a child process, and executed there.  State
    tracking remains in the main process.

    Use this runner when operations perform CPU-bound work or when
    you need true parallelism (bypassing the GIL)::

        result = await MultiProcessRunner(max_workers=2, on_event=log_event).run(dag)
        # or, from a sync edge: il.run(MultiProcessRunner().run(dag))
    """

    max_workers: int = 4
    fail_fast: bool = True
    reraise: bool = False

    _pool: ProcessPoolExecutor | None = PrivateAttr(default=None)
    _futures: dict[Future[Any], Operation] = PrivateAttr(default_factory=dict)
    _dag_spec: dict[str, Any] | None = PrivateAttr(default=None)

    @property
    def _capacity(self) -> int:
        """Maximum number of concurrent operations this runner can execute.

        Returns:
            ``max_workers``, the size of the process pool.
        """
        return self.max_workers

    def _on_start(self) -> None:
        """Create the process pool and serialize the DAG for the workers."""
        self._pool = ProcessPoolExecutor(max_workers=self.max_workers)
        self._dag_spec = self.state.dag.to_spec().model_dump(mode="json")

    def _on_end(self) -> None:
        """Shut down the process pool and drop the per-run worker inputs."""
        if self._pool is not None:
            self._pool.shutdown(wait=True, cancel_futures=False)
            self._pool = None
        self._futures.clear()
        self._dag_spec = None

    def _submit_operation(
        self,
        operation: Operation,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> Future[Any]:
        """Submit an operation for execution in a child process.

        Args:
            operation: The operation to execute.
            partition_or_window: Partition or window to scope the run.

        Returns:
            A Future representing the worker result.

        Raises:
            RunnerError: If the process pool is not initialized.
        """
        if self._pool is None or self._dag_spec is None:
            raise RunnerError("Process pool not initialized")

        self.state.mark_running(operation)

        future = self._pool.submit(
            _worker,
            operation.id,
            self._dag_spec,
            partition_or_window,
            self.state.metadata,
        )
        self._futures[future] = operation
        return future

    def _handle_completed(self, future: Future[Any], operation: Operation) -> None:
        """Process a completed future from a worker process.

        Unlike the base ``_handle_completed``, this interprets the
        ``(id, success, error_msg, tb, effects)`` tuple returned by
        ``_worker``.

        Args:
            future: The finished future returned by ``_submit_operation``.
            operation: The operation the future was submitted for.

        Raises:
            RunnerError: If the operation failed and ``fail_fast`` or ``reraise`` is set.
        """
        self._futures.pop(future, None)

        try:
            _key, success, error_message, tb, effects = future.result()
        except Exception as e:
            self.state.mark_failed(operation, format_exception(e), tb=traceback.format_exc())
            if self.fail_fast or self.reraise:
                raise
            return

        if success:
            self.state.mark_completed(operation, effects=OperationResult(**effects))
        else:
            self.state.mark_failed(
                operation,
                error_message or "Unknown error",
                tb=tb,
                effects=OperationResult(error=error_message, **effects),
            )
            if self.fail_fast or self.reraise:
                raise RunnerError(f"Operation '{operation.key}' failed: {error_message}")

    def _handle_flushed(self, future: Future[Any], operation: Operation) -> None:
        """Interpret the worker ``(id, success, error_message, tb, effects)`` tuple during flush.

        Args:
            future: The finished future to interpret.
            operation: The operation the future was submitted for.
        """
        try:
            _key, success, error_message, tb, effects = future.result()
        except Exception as e:  # noqa: BLE001
            self.state.mark_failed(operation, format_exception(e), tb=traceback.format_exc())
            return

        if success:
            self.state.mark_completed(operation, effects=OperationResult(**effects))
        else:
            self.state.mark_failed(
                operation,
                error_message or "Unknown error",
                tb=tb,
                effects=OperationResult(error=error_message, **effects),
            )
