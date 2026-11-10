"""Job: a named, schedulable materialization workload."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, Field

from interloper.component import Component, Relation
from interloper.operation import Operation, Workload
from interloper.retry import RetryPolicy

if TYPE_CHECKING:
    from interloper.asset.base import Asset
    from interloper.destination import Destination
    from interloper.source.base import Source


class JobState(BaseModel):
    """Machine-owned job state (see ``Component.state_model``).

    Timestamps are canonical timezone-aware ISO-8601 strings; the scheduler
    compares them lexicographically in SQL, so they are validated here but
    never rewritten. ``last_run_status`` qualifies ``last_run_at`` — it reads
    as that timestamp's badge rather than a column of its own.
    """

    next_run_at: str | None = Field(default=None, title="Next run")
    last_run_at: str | None = Field(default=None, title="Last run")
    last_run_status: str | None = Field(default=None, json_schema_extra={"x-hidden": True})


class Job(Component, Workload):
    """A materialization workload: the anchor of the ``job`` kind.

    A job declares *what* to materialize (``targets``); concrete job classes
    add *when*: `CronJob` carries a cron trigger.
    Trigger fields are inert declarative intent: the framework carries them,
    and an operator (the scheduler) acts on them. The workload itself
    compiles to the same `DAG` that every other
    entry point executes.

    A job also carries workload-level defaults: every relation the job holds
    is the default for the same relation on any target that resolves none,
    ``destinations`` included. The default applies where the job runs
    (`operations`), on a copy of the target; the target itself is never changed.
    """

    icon: ClassVar[str] = "carbon:event-schedule"
    state_model: ClassVar[type[BaseModel] | None] = JobState

    targets: list[Source | Asset] = Relation(["source", "asset"], many=True, optional=True, on_delete="detach")
    destinations: list[Destination] = Relation("destination", many=True, optional=True)

    tags: list[str] = Field(default_factory=list)
    enabled: bool = Field(
        default=True,
        description="Job will run on the configured schedule",
        json_schema_extra={"x-section": "Operation"},
    )
    retry: RetryPolicy | None = Field(
        default=None,
        title="Retry",
        description="Attempt budget for this job's runs",
        json_schema_extra={"x-section": "Operation"},
    )
    timeout: int | None = Field(
        default=None,
        gt=0,
        title="Timeout",
        description="Seconds a run may take before it is failed; empty uses the instance default",
        json_schema_extra={"x-section": "Operation"},
    )

    def operations(self) -> list[Operation]:
        """The targets' operations, each target run with this job's bindings where it resolves none.

        Returns:
            Every operation the job's targets provide.
        """
        return [operation for target in self.targets for operation in self._run_as(target).operations()]

    def _run_as(self, target: Source | Asset) -> Source | Asset:
        """The target as this job runs it: a copy carrying the job's bindings it lacks, or itself.

        For every relation the job binds other than ``targets``, a target that
        declares a relation of that name and resolves nothing under it (so a
        source's own destinations, and an owned asset's source-provided ones,
        win) receives the job's targets it accepts: every one for a ``many``
        relation, the first otherwise. The target itself is never modified.

        Args:
            target: One of the job's targets.

        Returns:
            The target, or a copy of it (same ids) carrying what it lacks.
        """
        lent: dict[str, Any] = {}
        for name in type(self).relations:
            relation = type(target).relations.get(name)
            if name == "targets" or relation is None or target._targets(name):
                continue
            owner = target.identity
            accepted = [held for held in self._targets(name) if relation.accepts(held.kind, held.identity, owner=owner)]
            if accepted:
                lent[name] = accepted if relation.many else accepted[0]
        return target(**lent) if lent else target
