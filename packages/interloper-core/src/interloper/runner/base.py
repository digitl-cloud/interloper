"""Abstract base runner and shared infrastructure."""

from __future__ import annotations

from collections.abc import Callable
from functools import cache
from importlib.metadata import entry_points
from typing import TYPE_CHECKING, Any

from pydantic import Field, PrivateAttr

from interloper.component import Component
from interloper.errors import ConfigError, PartitionError, RunnerError

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.runner.sync_runner import SyncRunner
from interloper.events.event import Event
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runner.results import ExecutionStatus, RunResult
from interloper.runner.state import RunState

_ENTRY_POINT_GROUP = "interloper.runners"


@cache
def runners() -> dict[str, type[SyncRunner]]:
    """Load the runner registry from installed entry points.

    Every runner — including the built-ins — registers through the
    ``interloper.runners`` entry-point group; the entry name is the runner
    type key used in ``RunnerSettings.type``. Installed means discovered: a
    new runner is one new package with one entry point.

    Returns:
        Mapping of runner type key to runner class.
    """
    return {entry_point.name: entry_point.load() for entry_point in entry_points(group=_ENTRY_POINT_GROUP)}


def build_runner(
    runner_type: str = "multi_thread",
    runner_config: dict[str, Any] | None = None,
) -> tuple[type[SyncRunner], dict[str, Any]]:
    """Resolve a runner type key to a concrete class and forward its kwargs.

    Args:
        runner_type: Runner type key (an ``interloper.runners`` entry name).
        runner_config: Runner-specific kwargs forwarded to the constructor.

    Returns:
        A tuple of ``(runner_class, runner_kwargs)``.

    Raises:
        ConfigError: If no runner is registered under ``runner_type``.
    """
    registry = runners()
    if runner_type not in registry:
        raise ConfigError(
            f"Unknown runner: {runner_type!r} (available: {sorted(registry)}). "
            f"Is the matching interloper package installed?"
        )
    return registry[runner_type], dict(runner_config or {})


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
