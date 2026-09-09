"""Job: a named, schedulable materialization workload."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from pydantic import BaseModel, Field

from interloper.component import Component, Relation
from interloper.operation import Operation, Workload

if TYPE_CHECKING:
    from interloper.asset.base import Asset
    from interloper.destination import Destination
    from interloper.source.base import Source


class JobState(BaseModel):
    """Machine-owned job state (see ``Component.state_model``).

    Timestamps are canonical timezone-aware ISO-8601 strings; the scheduler
    compares them lexicographically in SQL, so they are validated here but
    never rewritten.
    """

    next_run_at: str | None = None
    last_run_at: str | None = None


class Job(Component, Workload):
    """A materialization workload: the anchor of the ``job`` kind.

    A job declares *what* to materialize (``targets``); concrete job classes
    add *when*: :class:`~interloper.job.cron.CronJob` carries a cron trigger.
    Trigger fields are inert declarative intent: the framework carries them,
    and an operator (the scheduler) acts on them. The workload itself
    compiles to the same :class:`~interloper.dag.base.DAG` that every other
    entry point executes.

    A job also carries workload-level defaults, cascading to its targets the
    way a source cascades to its assets: every relation the job holds fills
    the same relation on any target (and any destination) that leaves it
    unbound, ``destinations`` included.
    """

    icon: ClassVar[str] = "carbon:event-schedule"
    state_model: ClassVar[type[BaseModel] | None] = JobState

    targets: list[Source | Asset] = Relation(["source", "asset"], many=True, optional=True, on_delete="detach")
    destinations: list[Destination] = Relation("destination", many=True, optional=True)

    enabled: bool = Field(default=True, description="Job will run on the configured schedule")
    tags: list[str] = Field(default_factory=list)

    def operations(self) -> list[Operation]:
        """The targets' operations, flattened.

        Returns:
            Every operation the job's targets provide.
        """
        return [operation for target in self.targets for operation in target.operations()]

    def _trickle_down(self) -> None:
        """Fill the unbound relations of this job's targets and destinations from its own.

        A binding is the only moment this can run: relation keyword arguments
        reach a component after ``model_post_init``, so a job knows neither
        its targets nor its destinations until :meth:`on_rebind` has seen them.
        """
        for target in self.targets:
            self.trickle(target)
        for destination in self.destinations:
            self.trickle(destination)

    def on_rebind(self, name: str) -> None:
        """Trickle this job's bindings down whenever one of them changes.

        Args:
            name: The relation name whose binding changed; every relation the
                job holds trickles, so the name itself is not read.
        """
        self._trickle_down()
