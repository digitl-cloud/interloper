"""Job: a named, schedulable materialization workload."""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import BaseModel, Field, field_validator

from interloper.asset.base import Asset
from interloper.component.base import Component, RelationDefinition
from interloper.destination import Destination
from interloper.operation import Operation, Workload
from interloper.source.base import Source


class JobState(BaseModel):
    """Machine-owned job state (see ``Component.state_model``).

    Timestamps are canonical timezone-aware ISO-8601 strings — the scheduler
    compares them lexicographically in SQL, so they are validated here but
    never rewritten.
    """

    next_run_at: str | None = None
    last_run_at: str | None = None


class Job(Component, Workload):
    """A materialization workload: the anchor of the ``job`` kind.

    A job declares *what* to materialize (``targets``); concrete job classes
    add *when* — :class:`~interloper.job.cron.CronJob` carries a cron trigger.
    Trigger fields are inert declarative intent: the framework carries them,
    and an operator (the scheduler) acts on them. The workload itself
    compiles to the same :class:`~interloper.dag.base.DAG` that every other
    entry point executes.

    A job also carries workload-level defaults, cascading to its targets the
    way a source cascades to its assets: ``destinations`` become the
    destinations of any target that declares none, and ``resources`` fill
    targets' (and destinations') empty resource slots by name, then by type.
    """

    icon: ClassVar[str] = "carbon:event-schedule"
    relation_types: ClassVar[dict[str, RelationDefinition]] = {
        # A target is an orchestration pointer, not an input: deleting it just
        # shrinks the job's scope, so it detaches rather than blocking.
        "target": RelationDefinition(kinds=["source", "asset"], field="targets", on_delete="detach"),
        "destination": RelationDefinition(kinds=["destination"], field="destinations"),
        "resource": RelationDefinition(kinds=["connection", "config", "resource"], field="resources", slotted=True),
    }
    internal_fields: ClassVar[frozenset[str]] = frozenset({"targets", "destinations"})
    state_model: ClassVar[type[BaseModel] | None] = JobState

    targets: list[Source | Asset] = Field(default_factory=list)
    destinations: list[Destination] = Field(default_factory=list)
    enabled: bool = Field(default=True, description="Job will run on the configured schedule")
    tags: list[str] = Field(default_factory=list)

    @field_validator("destinations", mode="before")
    @classmethod
    def _coerce_destinations(cls, value: Any) -> Any:
        """Accept a single destination or ``None`` where a list is expected.

        Args:
            value: The raw field value: a single destination, a list or tuple of
                them, or ``None``.

        Returns:
            The value as a list.
        """
        if value is None:
            return []
        return value if isinstance(value, (list, tuple)) else [value]

    def operations(self) -> list[Operation]:
        """The targets' operations, flattened.

        Returns:
            Every operation the job's targets provide.
        """
        return [operation for target in self.targets for operation in target.operations()]

    def model_post_init(self, context: Any) -> None:
        """Cascade workload-level defaults down to targets and destinations.

        Args:
            context: Pydantic's post-init context, forwarded to ``super()``.
        """
        super().model_post_init(context)
        for target in self.targets:
            if not target.destinations and self.destinations:
                target.destinations = list(self.destinations)
            self.trickle_resources(target)
        for destination in self.destinations:
            self.trickle_resources(destination)
