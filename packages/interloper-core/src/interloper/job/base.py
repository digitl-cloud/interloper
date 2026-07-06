"""Job: a named, schedulable materialization workload."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from pydantic import Field

from interloper.asset.base import Asset
from interloper.component.base import Component, RelationDefinition
from interloper.source.base import Source

if TYPE_CHECKING:
    from interloper.dag.base import DAG


class Job(Component):
    """A materialization workload with an optional schedule.

    A job declares *what* to materialize (``targets``) and optionally *when*
    (``cron``). Like an asset's partitioning, the trigger fields are inert
    declarative intent: the framework carries them, and an operator (the
    scheduler) acts on them. The workload itself compiles to the same
    :class:`~interloper.dag.base.DAG` that every other entry point executes.
    """

    icon: ClassVar[str] = "carbon:event-schedule"
    relation_types: ClassVar[dict[str, RelationDefinition]] = {
        "target": RelationDefinition(kinds=["source", "asset"]),
    }
    internal_fields: ClassVar[frozenset[str]] = frozenset({"targets"})

    targets: list[Source | Asset] = Field(default_factory=list)
    cron: str | None = Field(default=None, description="Cron trigger; interpreted by the scheduler")
    enabled: bool = Field(default=True)
    tags: list[str] = Field(default_factory=list)
    partitioned: bool = Field(default=False)
    backfill_days: int | None = Field(default=None)

    def dag(self) -> DAG:
        """Compile the job's targets into an executable DAG.

        Returns:
            A DAG over all target sources and assets.
        """
        from interloper.dag.base import DAG

        return DAG(*self.targets)
