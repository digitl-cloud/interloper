"""Abstract base runner and shared infrastructure."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from pydantic import Field, PrivateAttr

from interloper.component import Component
from interloper.errors import PartitionError, RunnerError

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.runner.sync_runner import SyncRunner
from interloper.events.event import Event
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runner.results import ExecutionStatus, RunResult
from interloper.runner.state import RunState


def build_runner(
    runner_type: str = "multi_thread",
    runner_config: dict[str, Any] | None = None,
) -> tuple[type[SyncRunner], dict[str, Any]]:
    """Resolve a runner type string to a concrete class and forward its kwargs.

    Supported types: ``serial``, ``multi_thread``, ``multi_process``, ``docker``.
    ``docker`` requires the ``interloper-docker`` package to be installed.

    Args:
        runner_type: Runner type name.
        runner_config: Runner-specific kwargs forwarded to the constructor.

    Returns:
        A tuple of ``(runner_class, runner_kwargs)``.

    Raises:
        ValueError: If ``runner_type`` is unknown or its package is missing.
    """
    cls: type[SyncRunner]
    match runner_type:
        case "serial":
            from interloper.runner.serial import SerialRunner

            cls = SerialRunner
        case "multi_thread":
            from interloper.runner.multi_thread import MultiThreadRunner

            cls = MultiThreadRunner
        case "multi_process":
            from interloper.runner.multi_process import MultiProcessRunner

            cls = MultiProcessRunner
        case "docker":
            try:
                from interloper_docker import DockerRunner
            except ImportError as exc:
                raise ValueError("Runner 'docker' requires the 'interloper-docker' package to be installed.") from exc
            cls = DockerRunner
        case "k8s":
            try:
                from interloper_k8s import KubernetesRunner
            except ImportError as exc:
                raise ValueError("Runner 'k8s' requires the 'interloper-k8s' package to be installed.") from exc

            cls = KubernetesRunner
        case _:
            raise ValueError(f"Unknown runner: {runner_type!r}. Available: serial, multi_thread, multi_process, docker")

    return cls, dict(runner_config or {})


class Runner(Component):
    """Abstract base class for all runners.

    Provides shared config, state management, lifecycle hooks, and
    preflight validation.  Subclasses implement their own ``run()``
    method (sync or async) and the scheduling primitives.
    """

    fail_fast: bool = False
    reraise: bool = True
    on_event: Callable[[Event], None] | None = Field(default=None, exclude=True, repr=False)

    _state: RunState | None = PrivateAttr(default=None)

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    @property
    def state(self) -> RunState:
        """The current run state.

        Raises:
            RunnerError: If state has not been initialized via ``run()``.
        """
        if self._state is None:
            raise RunnerError("State not initialized")
        return self._state

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def _on_start(self) -> None:
        """Lifecycle hook called before a run begins (e.g. create pools)."""

    def _on_end(self) -> None:
        """Lifecycle hook called after a run ends (e.g. shutdown pools)."""

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _init_run(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any] | None,
    ) -> None:
        """Initialize run state and start the run."""
        self._preflight_validation(dag, partition_or_window)
        self._state = RunState(dag, metadata=metadata)
        self.state.start_run(partition_or_window)

    def _finalize_run(
        self,
        error: str | None = None,
    ) -> RunResult:
        """Finalize the run and return the result.

        Returns:
            A RunResult summarizing the execution outcome.
        """
        status = ExecutionStatus.FAILED if (self.state.failed_assets or error) else ExecutionStatus.COMPLETED
        asset_executions = self.state.end_run(status, error)

        return RunResult(
            partition_or_window=self.state.partition_or_window,
            status=status,
            asset_executions=asset_executions,
            execution_time=self.state.elapsed_time or 0,
        )

    def _preflight_validation(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> None:
        """Run preflight validations before execution begins.

        Raises:
            PartitionError: If any partitioned asset does not support windowed execution.
        """
        if not isinstance(partition_or_window, PartitionWindow):
            return

        unsupported = [
            type(asset).key
            for asset in dag.assets
            if asset.materializable and asset.partitioning is not None and not asset.partitioning.allow_window
        ]
        if unsupported:
            raise PartitionError(
                "Windowed runs require all partitioned assets to set allow_window=True. "
                f"Unsupported assets: {sorted(unsupported)}."
            )
