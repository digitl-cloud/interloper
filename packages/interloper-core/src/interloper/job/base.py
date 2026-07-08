"""Job: a named, schedulable materialization workload."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, Field, field_validator

from interloper.asset.base import Asset
from interloper.component.base import Component, RelationDefinition
from interloper.destination import Destination
from interloper.source.base import Source

if TYPE_CHECKING:
    from interloper.dag.base import DAG


class JobState(BaseModel):
    """Machine-owned job state (see ``Component.state_model``).

    Timestamps are canonical timezone-aware ISO-8601 strings — the scheduler
    compares them lexicographically in SQL, so they are validated here but
    never rewritten.
    """

    next_run_at: str | None = None
    last_run_at: str | None = None


class Job(Component):
    """A materialization workload with an optional schedule.

    A job declares *what* to materialize (``targets``) and optionally *when*
    (``cron``). Like an asset's partitioning, the trigger fields are inert
    declarative intent: the framework carries them, and an operator (the
    scheduler) acts on them. The workload itself compiles to the same
    :class:`~interloper.dag.base.DAG` that every other entry point executes.

    A job also carries workload-level defaults, cascading to its targets the
    way a source cascades to its assets: ``destinations`` become the
    destinations of any target that declares none, and ``resources`` fill
    targets' (and destinations') empty resource slots by name, then by type.
    """

    icon: ClassVar[str] = "carbon:event-schedule"
    runnable: ClassVar[bool] = True
    relation_types: ClassVar[dict[str, RelationDefinition]] = {
        "target": RelationDefinition(kinds=["source", "asset"], field="targets"),
        "destination": RelationDefinition(kinds=["destination"], field="destinations"),
        "resource": RelationDefinition(kinds=["connection", "config", "resource"], field="resources", slotted=True),
    }
    internal_fields: ClassVar[frozenset[str]] = frozenset({"targets", "destinations"})
    state_model: ClassVar[type[BaseModel] | None] = JobState

    targets: list[Source | Asset] = Field(default_factory=list)
    destinations: list[Destination] = Field(default_factory=list)
    cron: str | None = Field(default=None, description="Cron trigger; interpreted by the scheduler")
    enabled: bool = Field(default=True)
    tags: list[str] = Field(default_factory=list)
    partitioned: bool = Field(default=False)
    backfill_days: int | None = Field(default=None)

    @field_validator("destinations", mode="before")
    @classmethod
    def _coerce_destinations(cls, value: Any) -> Any:
        """Accept a single destination or ``None`` where a list is expected.

        Returns:
            The value as a list.
        """
        if value is None:
            return []
        return value if isinstance(value, (list, tuple)) else [value]

    def model_post_init(self, context: Any) -> None:
        """Cascade workload-level defaults down to targets and destinations."""
        super().model_post_init(context)
        for target in self.targets:
            if not target.destinations and self.destinations:
                target.destinations = list(self.destinations)
            self.trickle_resources(target)
        for dest in self.destinations:
            self.trickle_resources(dest)

    def dag(self) -> DAG:
        """Compile the job's targets into an executable DAG.

        Returns:
            A DAG over all target sources and assets.
        """
        from interloper.dag.base import DAG

        return DAG(*self.targets)
