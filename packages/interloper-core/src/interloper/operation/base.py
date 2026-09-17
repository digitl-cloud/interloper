"""Operation: the unit of work a runner executes.

Two contracts, one hierarchy. A :class:`Workload` is what a run may target:
it flattens into operations through :meth:`Workload.operations` and declares
whether its runs bill against quotas. An :class:`Operation` is the node the
DAG orders and a runner drives: materializing an asset is one operation,
renewing a connection's credentials is another, and every operation is
trivially the workload of itself. Groupings (a source, a job) are workloads
only: they provide operations, they never execute.

The runner is agnostic of what a node does: it calls
:meth:`Operation.execute` with an :class:`OperationContext` of plain facts,
records the returned :class:`OperationResult` effects on the node's
execution info, and consults :meth:`Operation.failure` for a
persistence-safe message when execution raises. Effects are values: which
fields to merge into the component's config, which to stamp onto its state,
applied by the platform envelope after the run; core never holds a handle
to the store.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import Field

from interloper.component.base import Component
from interloper.errors import format_exception
from interloper.partitioning.base import PartitionConfig

if TYPE_CHECKING:
    from interloper.component.relation import Relation
    from interloper.dag.base import DAG
    from interloper.partitioning.base import Partition, PartitionWindow


@dataclass
class OperationContext:
    """The facts an operation executes with.

    Handed down by the runner, one per node execution: the operation-level
    counterpart of the asset-level ``ExecutionContext``. ``dag`` is the graph
    the node belongs to, how an operation reaches its upstream nodes'
    outputs; operations without dependencies ignore it.
    """

    partition_or_window: Partition | PartitionWindow | None = None
    dag: DAG | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class OperationResult:
    """What an operation hands back for the platform to persist.

    ``config`` fields are merged into the target component's stored config
    (through its encryption path) and ``state`` fields onto its
    machine-owned state: how an operation's effects reach the row without
    the operation knowing the store. ``error`` carries the
    persistence-safe message of a failed execution, set by
    :meth:`Operation.failure`; success paths leave it unset (terminal
    status is the runner's, not the operation's).
    """

    error: str | None = None
    config: dict[str, Any] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)


class Workload(ABC):
    """What a run may target: a provider of operations.

    ``billable`` declares whether runs of this workload count against the
    org's run quota; the platform records it on each run at creation so
    billing stays correct even after the component is deleted.
    """

    billable: ClassVar[bool] = True

    @abstractmethod
    def operations(self) -> list[Operation]:
        """The operations this workload flattens into.

        Returns:
            The operations, before any graph assembly.
        """


class Operation(Component, Workload):
    """A unit of work: the node a DAG orders and a runner drives.

    An operation is a component: the DAG, the runner and the platform all
    address it by its component identity, and its events and executions are
    keyed by its row id. What it adds to a component is the execution
    contract (:meth:`execute`, :meth:`failure`) and the node attributes the
    graph machinery reads, which ``Asset`` (the graph-structured, partitioned
    operation) narrows with its own fields and properties.

    ``kind`` is deliberately empty: an operation is a contract several kinds
    satisfy, not a kind of its own, and declaring it here is what stops
    ``Component.__init_subclass__`` deriving one. Each implementor declares
    its own.

    ``capture_traceback`` controls whether a failed execution's traceback
    is attached to its failure event; off for operations whose raw errors
    embed secrets (credential exchanges carry them in URLs).
    """

    kind: ClassVar[str] = ""
    capture_traceback: ClassVar[bool] = True
    partitioning: ClassVar[PartitionConfig | None] = None

    materializable: bool = Field(default=True, json_schema_extra={"x-hidden": True})

    @property
    def source(self) -> Any:
        """The source that owns this node, or ``None`` when nothing does.

        Read by the graph and by the event metadata on any node, including
        the operations no source owns, which is why it is answered here
        rather than only on ``Asset``.

        Returns:
            The owning source, or ``None``.
        """
        return None

    def operations(self) -> list[Operation]:
        """An operation is trivially its own workload.

        Returns:
            A single-element list holding this operation.
        """
        return [self]

    def effective_partition(
        self, partition_or_window: Partition | PartitionWindow | None
    ) -> Partition | PartitionWindow | None:
        """The partition scope this operation actually consumes.

        Unpartitioned operations ignore any requested scope.

        Args:
            partition_or_window: The partition or partition window the run was
                scoped to, or ``None`` when it was unscoped.

        Returns:
            The scope unchanged for partitioned operations, ``None`` otherwise.
        """
        return partition_or_window if self.partitioning is not None else None

    def upstream_relations(self) -> dict[str, Relation]:
        """The node's relations that another node in the graph fills.

        An ``asset``-kind relation is an edge: whatever fills it is a node the
        graph must order before this one. Every other relation points at
        something outside the graph (a connection, a config, a destination).

        Returns:
            Relation name to declaration, for the relations accepting assets.
        """
        return {name: relation for name, relation in type(self).relations.items() if "asset" in relation.kinds}

    def _validate_time_partitioning(self, partitioning: Any, partition_or_window: Any) -> None:
        """Validate a time-partitioned run scope against this node.

        Preflight hook; the default has no partitioning to validate
        (``partitioning`` is ``None`` unless a subclass declares it).

        Args:
            partitioning: The node's time-partition configuration.
            partition_or_window: The run's partition scope.
        """

    def _event_metadata(
        self,
        metadata: dict[str, Any],
        partition_or_window: Partition | PartitionWindow | None = None,
    ) -> dict[str, Any]:
        """Build the base event metadata dict for this node.

        Args:
            metadata: Run-level metadata (e.g. run_id, backfill_id).
            partition_or_window: Current partition scope.

        Returns:
            The merged metadata dict.
        """
        return {
            **metadata,
            "component_id": self.id,
            "component_kind": self.kind,
            "component_key": self.key,
            "partition_or_window": str(partition_or_window) if partition_or_window else None,
        }

    # -- Execution -----------------------------------------------------------

    @abstractmethod
    async def execute(self, context: OperationContext) -> OperationResult:
        """Execute this operation.

        Args:
            context: The facts this execution is scoped to.

        Returns:
            The effects to persist (often none).
        """

    def failure(self, error: Exception) -> OperationResult:
        """Describe a failed execution in terms the platform can persist.

        Consulted by the runner when :meth:`execute` raises. Override to
        curate the message or attach state effects (e.g. a retry slot); the
        default formats the error with pydantic input values stripped.

        Args:
            error: The exception :meth:`execute` raised.

        Returns:
            A result carrying the message and any state effects.
        """
        return OperationResult(error=format_exception(error))
