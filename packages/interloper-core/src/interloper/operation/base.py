"""Operation: the unit of work a runner executes.

Two contracts, one hierarchy. A `Workload` is what a run may target:
it flattens into operations through `Workload.operations` and declares
whether its runs bill against quotas. An `Operation` is the node the
DAG orders and a runner drives: materializing an asset is one operation,
renewing a connection's credentials is another, and every operation is
trivially the workload of itself. Groupings (a source, a job) are workloads
only: they provide operations, they never execute.

The runner is agnostic of what a node does: it calls
`Operation.execute` with an `OperationContext` of plain facts,
records the returned `OperationResult` effects on the node's
execution info, and consults `Operation.failure` for a
persistence-safe message when execution raises. Effects are values: which
fields to merge into the component's config, which to stamp onto its state,
applied by the platform envelope after the run; core never holds a handle
to the store.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from traceback import format_exc
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import Field

from interloper.component.base import Component
from interloper.errors import format_exception
from interloper.partitioning.base import PartitionConfig
from interloper.retry import RetryPolicy
from interloper.telemetry import attributes as telemetry_attributes
from interloper.telemetry.tracer import tracer

if TYPE_CHECKING:
    from collections.abc import Callable

    from interloper.component.relation import Relation
    from interloper.dag.base import DAG
    from interloper.partitioning.base import PartitionWindow


@dataclass
class OperationContext:
    """The facts an operation executes with.

    Handed down by the runner, one per node execution: the operation-level
    counterpart of the asset-level ``ExecutionContext``. ``dag`` is the graph
    the node belongs to, how an operation reaches its upstream nodes'
    outputs; operations without dependencies ignore it.
    """

    window: PartitionWindow | None = None
    dag: DAG | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Effect:
    """What an operation writes back to its own component, as the platform receives it.

    ``config`` fields are merged into the component's stored config, through
    its encryption path; ``state`` fields onto its machine-owned state. An
    operation returns them on its `OperationResult`; a run hands them to the
    platform as one ``Effect`` per operation.
    """

    config: dict[str, Any] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)


@dataclass
class OperationResult:
    """What an operation came to: the effects the platform persists, and the verdict.

    ``config`` fields are merged into the target component's stored config
    (through its encryption path) and ``state`` fields onto its
    machine-owned state: how an operation's effects reach the row without
    the operation knowing the store. An operation's `execute` returns one
    carrying its effects, and its `failure` hook one carrying the
    persistence-safe ``error``. The attempt loop (`from_execution`) completes
    a failure with the ``traceback`` (when the operation's class allows one)
    and the original ``exception``, kept in memory only so a runner can
    re-raise it. An unset ``error`` means the operation succeeded.
    """

    error: str | None = None
    config: dict[str, Any] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)
    traceback: str | None = None
    exception: BaseException | None = field(default=None, compare=False, repr=False)

    @property
    def succeeded(self) -> bool:
        """Whether the operation succeeded, which is whether no error is set.

        Returns:
            ``True`` when ``error`` is unset.
        """
        return self.error is None

    @classmethod
    async def from_execution(
        cls,
        operation: Operation,
        context: OperationContext,
        *,
        on_retry: Callable[[str], None],
        on_restart: Callable[[], None],
    ) -> OperationResult:
        """Execute an operation under its retry policy and return what it came to.

        Each attempt runs inside the ``interloper.operation.execute`` span. A
        failure is retried when the operation's `RetryPolicy` allows another
        attempt and `Operation.retryable` agrees: ``on_retry`` is told the
        error as it happens, the policy's backoff is slept, ``on_restart`` is
        told the next attempt starts, and the operation executes again. Any
        other failure becomes the operation's `failure` result, completed with
        the formatted error when the hook left it empty, the traceback when the
        class's ``capture_traceback`` allows, and the original exception.

        Args:
            operation: The operation to execute.
            context: The facts it executes with.
            on_retry: Told the formatted error of each attempt that will be
                retried, before its backoff.
            on_restart: Told when the next attempt starts, after the backoff.

        Returns:
            The result: the operation's own on success, its failure otherwise.
        """
        span_attributes = telemetry_attributes.from_metadata(operation.event_metadata(context.metadata, context.window))
        attempt = 1
        while True:
            try:
                with tracer().start_as_current_span("interloper.operation.execute", attributes=span_attributes):
                    return await operation.execute(context)
            except Exception as error:  # noqa: BLE001 - every failure becomes the operation's result
                policy = operation.retry
                if policy is not None and policy.allows(attempt + 1) and operation.retryable(error):
                    on_retry(format_exception(error))
                    await asyncio.sleep(policy.delay_before(attempt + 1))
                    attempt += 1
                    on_restart()
                    continue
                failed = operation.failure(error)
                return replace(
                    failed,
                    error=failed.error or format_exception(error),
                    traceback=format_exc() if type(operation).capture_traceback else None,
                    exception=error,
                )


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
    contract (`execute`, `failure`) and the node attributes the
    graph machinery reads, which ``Asset`` (the graph-structured, partitioned
    operation) narrows with its own fields and properties.

    ``kind`` is deliberately empty: an operation is a contract several kinds
    satisfy, not a kind of its own, and declaring it here is what stops
    ``Component.__init_subclass__`` deriving one. Each implementor declares
    its own.

    ``enabled`` is the same word ``Job`` and ``Hook`` already use for "this
    will run": a disabled operation stays in the graph, ordering whatever
    depends on it, and does not execute. ``Asset`` adds the half only an
    asset has, which is that a disabled one is read-only and its downstreams
    resolve its data from its destination instead of recomputing it.
    ``enabled`` and ``retry`` are inherited: an owned operation reads them
    through its owner, which decides (a source's assets run only while the
    source is enabled; one left without a retry policy uses its owner's).

    ``capture_traceback`` controls whether a failed execution's traceback
    is attached to its failure event; off for operations whose raw errors
    embed secrets (credential exchanges carry them in URLs).

    ``reads_upstream_data`` says whether executing the node reads its
    upstreams' data for the run's partition. An asset does; a view or a
    reference does not, which is what lets them sit, unpartitioned, on
    partitioned upstreams: the partition rule in the DAG only applies to
    nodes that read.
    """

    kind: ClassVar[str] = ""
    capture_traceback: ClassVar[bool] = True
    partitioning: ClassVar[PartitionConfig | None] = None
    reads_upstream_data: ClassVar[bool] = True

    enabled: bool = Field(
        default=True,
        description="Operation will execute",
        json_schema_extra={"x-section": "Operation", "x-inherit": True},
    )
    retry: RetryPolicy | None = Field(
        default=None,
        title="Retry",
        description="Attempt budget for this operation's execution",
        json_schema_extra={"x-section": "Operation", "x-inherit": True},
    )

    def operations(self) -> list[Operation]:
        """An operation is trivially its own workload.

        Returns:
            A single-element list holding this operation.
        """
        return [self]

    def effective_window(self, window: PartitionWindow | None) -> PartitionWindow | None:
        """The window this operation actually consumes.

        Unpartitioned operations ignore any window the run was given.

        Args:
            window: The window the run was scoped to, ``None`` when it was unscoped.

        Returns:
            The window unchanged for a partitioned operation, ``None`` otherwise.
        """
        return window if self.partitioning is not None else None

    def upstream_relations(self) -> dict[str, Relation]:
        """The node's relations that another node in the graph fills.

        An ``asset``-kind relation is an edge: whatever fills it is a node the
        graph must order before this one. Every other relation points at
        something outside the graph (a connection, a config, a destination).

        Returns:
            Relation name to declaration, for the relations accepting assets.
        """
        return {name: relation for name, relation in type(self).relations.items() if "asset" in relation.kinds}

    # -- Execution -----------------------------------------------------------

    @abstractmethod
    async def execute(self, context: OperationContext) -> OperationResult:
        """Execute this operation.

        Args:
            context: The facts this execution is scoped to.

        Returns:
            The effects to persist (often none).
        """

    def retryable(self, error: Exception) -> bool:
        """Whether another attempt at this operation is worth making.

        Consulted by the runner before it spends an attempt from
        `retry`. Override to recognise a permanent error, such as a
        vendor rejecting a request it will reject identically every time. The
        default is permissive: the budget, not the classifier, is what bounds
        waste.

        Args:
            error: The exception `execute` raised.

        Returns:
            ``True`` when the failure may be transient.
        """
        return True

    def failure(self, error: Exception) -> OperationResult:
        """Describe a failed execution in terms the platform can persist.

        Consulted by the runner when `execute` raises. Override to
        curate the message or attach state effects (e.g. a retry slot); the
        default formats the error with pydantic input values stripped.

        Args:
            error: The exception `execute` raised.

        Returns:
            A result carrying the message and any state effects.
        """
        return OperationResult(error=format_exception(error))
