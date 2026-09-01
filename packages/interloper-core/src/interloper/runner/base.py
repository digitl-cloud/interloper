"""Abstract base runner and shared infrastructure."""

from __future__ import annotations

import asyncio
import uuid
from abc import abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from opentelemetry.trace import StatusCode
from pydantic import Field, PrivateAttr
from typing_extensions import Self

from interloper.errors import ConfigError, PartitionError, RunnerError
from interloper.events import Event, EventBus
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.partitioning.time import TimePartitionConfig
from interloper.registry import Registry
from interloper.runner.results import ExecutionStatus, RunResult
from interloper.runner.state import RunState
from interloper.serializable import Serializable
from interloper.telemetry import attributes
from interloper.telemetry.propagation import context_from_env, extract_metadata, inject_metadata
from interloper.telemetry.tracer import tracer

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.settings import RunnerSettings

RUNNERS: Registry[type[Runner]] = Registry("interloper.runners")


class Runner(Serializable):
    """Abstract base class for all runners.

    Async-native: :meth:`run` is a coroutine. It owns the ``on_event``
    subscription lifecycle and delegates the actual DAG walk to the
    subclass :meth:`_run`. ``await`` it from async code, or drive it from a
    sync entrypoint with :func:`interloper.run`::

        result = await runner.run(dag)     # async
        result = il.run(runner.run(dag))   # sync edge
    """

    fail_fast: bool = False
    reraise: bool = True
    on_event: Callable[[Event], None] | None = Field(default=None, exclude=True, repr=False)

    _state: RunState | None = PrivateAttr(default=None)

    # -- Construction ----------------------------------------------------------

    @classmethod
    def from_settings(cls, settings: RunnerSettings) -> Self:
        """Construct the runner the settings describe.

        Resolves ``settings.type`` in ``RUNNERS`` and constructs it with
        ``settings.config``. Called on a subclass, the resolved runner must
        be of that subclass.

        Args:
            settings: Runner settings carrying the registry key (``type``) and
                the keyword arguments to construct it with (``config``).

        Returns:
            The configured runner instance.

        Raises:
            ConfigError: If no runner is registered under ``settings.type``,
                or the registered class is not of the receiving class.
        """
        runner_cls = RUNNERS.get(settings.type)
        if runner_cls is None:
            raise ConfigError(
                f"Unknown runner: {settings.type!r} (available: {list(RUNNERS.keys())}). "
                f"Is the matching interloper package installed?"
            )
        if not issubclass(runner_cls, cls):
            raise ConfigError(f"Runner '{settings.type}' does not resolve to a {cls.__name__}")
        return runner_cls(**settings.config)

    # -- State -----------------------------------------------------------------

    @property
    def state(self) -> RunState:
        """The current run state.

        Raises:
            RunnerError: If state has not been initialized via ``run()``.
        """
        if self._state is None:
            raise RunnerError("State not initialized")
        return self._state

    # -- Public API ------------------------------------------------------------

    async def run(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RunResult:
        """Materialize the DAG and return the result.

        Subscribes the ``on_event`` handler (if any) for the duration of the
        run, delegates to :meth:`_run`, then flushes pending events and
        unsubscribes — so every emitted event is delivered before returning.

        The subscription is scoped to this run: the EventBus is a
        process-wide singleton, so when several runs execute concurrently
        (e.g. the in-process launcher) every handler would otherwise
        receive every run's events. The run's ``run_id`` is assigned here
        (before any event is emitted) and only events carrying it in their
        metadata are delivered to ``on_event``.

        Args:
            dag: The DAG to execute.
            partition_or_window: Partition or window to scope the run.
            metadata: Arbitrary metadata (e.g. run_id, backfill_id).

        Returns:
            A RunResult summarizing the execution outcome.
        """
        metadata = dict(metadata or {})
        metadata.setdefault("run_id", str(uuid.uuid4()))
        run_id = metadata["run_id"]

        # Remote parent: metadata in-process, environment across containers.
        parent_context = extract_metadata(metadata) or context_from_env()

        handler: Callable[[Event], None] | None = None
        if self.on_event is not None:
            on_event = self.on_event

            def handler(event: Event) -> None:
                if event.metadata.get("run_id") == run_id:
                    on_event(event)

            EventBus.subscribe(handler)
        try:
            span_attrs = attributes.from_metadata(metadata)
            span_attrs[attributes.RUNNER_TYPE] = self.key
            if partition_or_window is not None:
                span_attrs[attributes.PARTITION] = str(partition_or_window)
            with tracer().start_as_current_span(
                "interloper.runner.run", context=parent_context, attributes=span_attrs
            ) as span:
                inject_metadata(metadata)
                result = await self._run(dag, partition_or_window, metadata)
                # Runners swallow asset failures into the result; surface them.
                if result.status is ExecutionStatus.FAILED:
                    span.set_status(StatusCode.ERROR, f"{len(result.failed_ids)} operation(s) failed")
                return result
        finally:
            if handler is not None:
                await asyncio.to_thread(EventBus.flush, 5.0)
                EventBus.unsubscribe(handler)

    @abstractmethod
    async def _run(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any] | None,
    ) -> RunResult:
        """Walk the DAG and return the result (implemented by subclasses).

        Args:
            dag: The DAG to execute.
            partition_or_window: Partition or window to scope the run.
            metadata: Arbitrary metadata (e.g. run_id, backfill_id).

        Returns:
            A RunResult summarizing the execution outcome.
        """

    # -- Hooks -----------------------------------------------------------------

    def _on_start(self) -> None:
        """Lifecycle hook called before a run begins (e.g. create pools)."""

    def _on_end(self) -> None:
        """Lifecycle hook called after a run ends (e.g. shutdown pools)."""

    # -- Shared helpers --------------------------------------------------------

    def _init_run(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any] | None,
    ) -> None:
        """Initialize run state and start the run.

        Args:
            dag: The DAG to execute.
            partition_or_window: Partition or window to scope the run.
            metadata: Arbitrary metadata (e.g. run_id, backfill_id).
        """
        self._preflight_validation(dag, partition_or_window)
        self._state = RunState(dag, metadata=metadata)
        self.state.start_run(partition_or_window)

    def _finalize_run(
        self,
        error: str | None = None,
    ) -> RunResult:
        """Finalize the run and return the result.

        Args:
            error: Run-level error message, set when the DAG walk itself
                aborted. ``None`` means the walk finished, in which case the
                run only fails if individual operations did.

        Returns:
            A RunResult summarizing the execution outcome.
        """
        status = ExecutionStatus.FAILED if (self.state.failed_operations or error) else ExecutionStatus.COMPLETED
        executions = self.state.end_run(status, error)

        return RunResult(
            partition_or_window=self.state.partition_or_window,
            status=status,
            executions=executions,
            execution_time=self.state.elapsed_time or 0,
        )

    def _preflight_validation(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> None:
        """Run preflight validations before execution begins.

        Scope errors are raised here rather than left to the operations: a run
        whose scope no operation can serve should fail as a whole, instead of
        failing the offending operations while the rest of the DAG executes.

        Args:
            dag: The DAG whose operations are validated against the run scope.
            partition_or_window: Partition or window to scope the run.
                ``None`` means an unpartitioned run.

        Raises:
            PartitionError: If partitioned operations are run without a partition,
                a windowed run includes operations that do not support windows, or
                the scope disagrees with an operation's time partitioning.
        """
        if partition_or_window is None:
            partitioned = sorted(
                operation.key
                for operation in dag.operations
                if operation.materializable and operation.partitioning is not None
            )
            if partitioned:
                raise PartitionError(
                    "This run requires a partition or partition window. "
                    f"Partitioned operations: {partitioned}."
                )
            return

        if isinstance(partition_or_window, PartitionWindow):
            unsupported = [
                operation.key
                for operation in dag.operations
                if operation.materializable
                and operation.partitioning is not None
                and not operation.partitioning.allow_window
            ]
            if unsupported:
                raise PartitionError(
                    "Windowed runs require all partitioned operations to set allow_window=True. "
                    f"Unsupported operations: {sorted(unsupported)}."
                )

        for operation in dag.operations:
            if operation.materializable and isinstance(operation.partitioning, TimePartitionConfig):
                operation._validate_time_partitioning(operation.partitioning, partition_or_window)
