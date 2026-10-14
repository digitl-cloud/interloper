"""Runner: configuration a run is made from."""

from __future__ import annotations

import uuid
from abc import abstractmethod
from collections.abc import Callable
from contextlib import nullcontext
from typing import TYPE_CHECKING, Any, ClassVar

from opentelemetry.trace import StatusCode
from pydantic import Field
from typing_extensions import Self

from interloper.errors import ConfigError
from interloper.events import Event
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.registry import Registry
from interloper.runner.results import ExecutionStatus, RunResult
from interloper.runner.run import Run
from interloper.runner.subscription import RunSubscription
from interloper.serializable import Serializable
from interloper.telemetry import attributes
from interloper.telemetry.propagation import context_from_env, extract_metadata, inject_metadata
from interloper.telemetry.tracer import tracer

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.settings import RunnerSettings

RUNNERS: Registry[type[Runner]] = Registry("interloper.runners")


class Runner(Serializable):
    """Configuration a run is made from: how many operations run at once, where, and what a failure does.

    A runner holds nothing per run, so one runner serves any number of runs,
    concurrently too. Each kind of runner names its kind of `Run`, which owns
    the walk, what the run holds, and where an operation executes. ``await``
    `run` from async code, or drive it from a sync entrypoint with
    `interloper.run`:

    ```py
    result = await runner.run(dag)     # async
    result = il.run(runner.run(dag))   # sync edge
    ```
    """

    Run: ClassVar[type[Run]]

    fail_fast: bool = False
    reraise: bool = True
    on_event: Callable[[Event], None] | None = Field(default=None, exclude=True, repr=False)

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

    # -- Public API ------------------------------------------------------------

    @property
    @abstractmethod
    def capacity(self) -> int:
        """How many operations a run executes at once."""

    async def run(
        self,
        dag: DAG,
        window: Partition | PartitionWindow | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RunResult:
        """Run the DAG and return the result.

        Events carrying the run's id reach ``on_event`` (when set) before this
        returns: the EventBus is process-wide, so several runs in one process
        would otherwise reach every handler. The run is traced as
        ``interloper.runner.run``, under the parent span the metadata or the
        environment carries. Operation failures are recorded on the result;
        with ``reraise`` set, the first one is raised once the run has finished.

        Args:
            dag: The DAG to run.
            window: The window the run covers; a partition is accepted as the window of one.
            metadata: Run-level metadata (``run_id``, ``backfill_id``, ...); a
                ``run_id`` is generated when absent.

        Returns:
            The run's result.
        """
        window = PartitionWindow.of(window)
        metadata = dict(metadata or {})
        metadata.setdefault("run_id", str(uuid.uuid4()))
        subscription = RunSubscription(self.on_event, metadata["run_id"]) if self.on_event else nullcontext()
        async with subscription:
            parent = extract_metadata(metadata) or context_from_env()
            with tracer().start_as_current_span(
                "interloper.runner.run", context=parent, attributes=self._span_attributes(metadata, window)
            ) as span:
                inject_metadata(metadata)
                async with self.Run(self, dag, window, metadata) as run:
                    result = await run.walk()
                if result.status is ExecutionStatus.FAILED:
                    span.set_status(StatusCode.ERROR, f"{len(result.failed_ids)} operation(s) failed")
        if self.reraise:
            run.reraise()
        return result

    def _span_attributes(self, metadata: dict[str, Any], window: PartitionWindow | None) -> dict[str, str]:
        """The run span's attributes: the metadata's, the runner's key and the window.

        Args:
            metadata: Run-level metadata.
            window: The run's window.

        Returns:
            The attributes.
        """
        span_attributes = attributes.from_metadata(metadata)
        span_attributes[attributes.RUNNER_TYPE] = self.key
        if window is not None:
            span_attributes[attributes.PARTITION] = str(window)
        return span_attributes
