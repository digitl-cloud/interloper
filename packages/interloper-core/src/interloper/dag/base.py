"""Directed Acyclic Graph for operation dependency resolution and execution ordering."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from opentelemetry.trace import StatusCode
from pydantic import BaseModel

from interloper.asset.base import Asset
from interloper.component import Component
from interloper.errors import AssetNotFoundError, CircularDependencyError, DAGError
from interloper.operation import Operation, Workload
from interloper.partitioning import Partition, PartitionWindow, TimePartitionConfig
from interloper.runner.results import ExecutionStatus, RunResult
from interloper.serializable import Spec
from interloper.telemetry import attributes
from interloper.telemetry.tracer import tracer

if TYPE_CHECKING:
    from interloper.catalog.base import Catalog


# -- Specs ---------------------------------------------------------------------
class DAGSpec(BaseModel):
    """Serializable representation of a DAG.

    Holds a flat list of component specs which may be either
    :class:`~interloper.source.Source` specs (each carrying their
    asset-override map) or individual standalone
    :class:`~interloper.asset.Asset` specs.  The DAG constructor flattens
    sources back into their asset lists on reconstruction.
    """

    items: list[Spec] = []

    def reconstruct(
        self,
        catalog: Catalog | None = None,
        *,
        resolve: Callable[[str], Component] | None = None,
    ) -> DAG:
        """Reconstruct the DAG from its spec.

        Each source spec materialises a live source (with its assets
        pre-bound through ``Source.model_post_init`` and ``_resolve``),
        and each standalone asset spec materialises a bare asset.  All
        reconstructed items are then handed to the :class:`DAG`
        constructor which re-infers the dependency graph from the
        preserved asset ids.

        The items are one document, so they build against one shared
        registry: a ``{"ref": id}`` in one item resolves to the component
        another item carries, and the references are bound once every item
        exists.

        Args:
            catalog: Catalog used to resolve ``key`` references, shared
                across all items. Defaults to the settings-configured
                catalog, built lazily.
            resolve: Called with the id of a reference no item of the
                document carries; ``None`` makes such a reference an error.

        Returns:
            A new DAG instance with the same structure as the original.
        """
        with tracer().start_as_current_span(
            "interloper.dag_spec.reconstruct",
            attributes={attributes.DAG_SPEC_ITEMS: len(self.items)},
        ):
            registry: dict[str, Component] = {}
            roots = [spec.reconstruct(catalog, resolve=resolve, registry=registry) for spec in self.items]
            Component._bind_references(registry, resolve)
            return DAG(*roots)  # ty: ignore[invalid-argument-type]


# -- DAG -----------------------------------------------------------------------
class DAG:
    """Directed acyclic graph of operations.

    Edges come from the nodes' own bindings: whatever fills an
    ``asset``-kind relation (see
    :meth:`~interloper.operation.base.Operation.upstream_relations`) is a
    node this one runs after. The DAG validates every live node's relations
    and provides topological ordering for parallel execution.
    """

    def __init__(self, *items: Workload | type[Workload]) -> None:
        """Create a DAG from workloads (assets, sources, jobs, ...).

        Args:
            *items: Workload instances or classes to include; each flattens
                into the operations it provides.
        """
        self.operations: list[Operation] = []
        self.operation_map: dict[str, Operation] = {}
        self.predecessors: dict[str, list[str]] = {}
        self.successors: dict[str, list[str]] = {}
        self._build_graph(items)
        self._validate()

    def _build_graph(self, items: tuple[Workload | type[Workload], ...]) -> None:
        """Build the dependency graph from the workloads' operations.

        Args:
            items: The DAG's constructor arguments, Workload instances or
                classes; classes are instantiated and every workload is
                flattened into the operations it provides.

        Raises:
            DAGError: If the input is empty, contains duplicates, or has invalid types.
        """
        if not items:
            raise DAGError("DAG must contain at least one workload")

        for item in items:
            if isinstance(item, type) and issubclass(item, Workload):
                item = item()

            if isinstance(item, Workload):
                self.operations.extend(item.operations())
            else:
                raise DAGError(f"Expected a workload (asset, source, ...), got {type(item)}")

        self.operation_map = {operation.id: operation for operation in self.operations}

        if len(self.operation_map) != len(self.operations):
            seen: set[str] = set()
            duplicates: list[str] = []
            for operation in self.operations:
                if operation.id in seen:
                    duplicates.append(operation.id)
                seen.add(operation.id)
            raise DAGError(f"Duplicate operation id found: {duplicates}")

        self._resolve_declared()
        self._include_read_only_upstreams()

        for operation in self.operations:
            self.successors[operation.id] = []

        for operation in self.operations:
            if not operation.materializable:
                continue

            self.predecessors[operation.id] = []
            for upstream in self._upstream_targets(operation):
                self.predecessors[operation.id].append(upstream.id)
                self.successors[upstream.id].append(operation.id)

    def _upstream_targets(self, operation: Operation) -> list[Asset]:
        """The assets bound to one operation's upstream relations.

        Args:
            operation: The node whose bindings are read.

        Returns:
            Every bound asset, ordered by declaration and then by binding.
        """
        targets: list[Component] = []
        for name in operation.upstream_relations():
            bound = operation.bound(name)
            targets.extend(bound if isinstance(bound, list) else [] if bound is None else [bound])
        # Bind-time validation is what makes the cast sound: only an asset is accepted here.
        return cast("list[Asset]", targets)

    def _resolve_declared(self) -> None:
        """Bind the declared upstream keys nothing has bound yet.

        For every materializing asset and every unbound upstream relation that
        declares keys, the candidates are the DAG's other assets the relation
        :meth:`~interloper.component.relation.Relation.accepts`; a
        :attr:`~interloper.component.relation.Relation.source_local` key is
        further restricted to the asset's own source instance, since it names
        a sibling. A ``many`` relation binds every candidate and a
        single-valued one the only candidate there is; no candidate leaves the
        relation unbound for :meth:`_check_relations` to judge.

        Binding goes through
        :meth:`~interloper.component.base.Component.bind`, so an explicit
        binding is never overwritten, and it writes into the asset instance,
        so one reused across several DAGs keeps its first resolution.

        Raises:
            DAGError: If a single-valued relation has several candidates; the
                caller has to bind it explicitly.
        """
        assets = [operation for operation in self.operations if isinstance(operation, Asset)]
        for asset in assets:
            if not asset.materializable:
                continue
            for name, relation in asset.upstream_relations().items():
                if asset.bound(name) or not relation.keys():
                    continue
                candidates = [
                    candidate
                    for candidate in assets
                    if candidate is not asset
                    and relation.accepts("asset", candidate.identity, owner=asset.identity)
                    and (not relation.source_local or candidate.parent is asset.parent)
                ]
                if not candidates:
                    continue
                if not relation.many and len(candidates) > 1:
                    listed = ", ".join(f"{candidate.qualified_key}#{candidate.id[:8]}" for candidate in candidates)
                    raise DAGError(
                        f"'{asset.qualified_key}' relation '{name}' depends on "
                        f"'{', '.join(relation.keys())}' and the DAG holds {len(candidates)} matching assets "
                        f"({listed}); bind '{name}' explicitly."
                    )
                asset.bind(name, *candidates)

    def _include_read_only_upstreams(self) -> None:
        """Add every bound upstream the run itself does not materialize.

        A materializing node reads its upstreams through the DAG's own node
        (see :meth:`~interloper.asset.base.Asset._read_upstreams`), so an
        upstream nobody in the run materializes still has to be one: it joins
        as a non-materializable copy, same id, same bindings, same parent,
        which the runners skip and the dependent reads. Those copies never
        execute, so their own upstreams are not pulled in with them.
        """
        for operation in list(self.operations):
            if not operation.materializable:
                continue
            for upstream in self._upstream_targets(operation):
                if upstream.id in self.operation_map:
                    continue
                read_only = upstream(materializable=False)
                self.operations.append(read_only)
                self.operation_map[read_only.id] = read_only

    # -- Validation ------------------------------------------------------------

    def _validate(self) -> None:
        """Validate the DAG structure."""
        self._check_relations()
        self._check_circular_dependencies()
        self._check_partition_dependencies()

    def _check_relations(self) -> None:
        """Let every live node check its own relations against the DAG's nodes.

        See :meth:`~interloper.component.base.Component.validate_relations`;
        the DAG is what can answer whether a bound upstream is actually
        running in this graph.
        """
        nodes = {id: node for id, node in self.operation_map.items() if isinstance(node, Component)}
        for operation in self.operations:
            if operation.materializable and isinstance(operation, Component):
                operation.validate_relations(nodes)

    def _check_circular_dependencies(self) -> None:
        """Check for circular dependencies using DFS.

        Raises:
            CircularDependencyError: If a cycle is detected.
        """
        visited: set[str] = set()
        stack: set[str] = set()

        def has_cycle(node: str) -> bool:
            visited.add(node)
            stack.add(node)
            for neighbor in self.predecessors.get(node, []):
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in stack:
                    return True
            stack.remove(node)
            return False

        for operation_id in self.predecessors:
            if operation_id not in visited and has_cycle(operation_id):
                operation = self.operation_map[operation_id]
                raise CircularDependencyError(f"Circular dependency detected involving '{operation.key}'")

    def _check_partition_dependencies(self) -> None:
        """Check partition compatibility along every edge.

        No unpartitioned dependent of a partitioned upstream, and equal
        granularity between time-partitioned ends. Read-only upstreams are
        included on purpose; the read uses the dependent's partition against
        the upstream table.

        Raises:
            DAGError: If a non-partitioned operation depends on a partitioned
                one, or if two time-partitioned ends of an edge have
                different granularities.
        """
        for operation_id, preds in self.predecessors.items():
            operation = self.operation_map[operation_id]
            for pred_id in preds:
                upstream = self.operation_map[pred_id]
                if upstream.partitioning is not None and operation.partitioning is None:
                    raise DAGError(
                        f"Invalid upstream: partitioned asset '{upstream.key}' "
                        f"cannot be an upstream of non-partitioned asset '{operation.key}'"
                    )
                if (
                    isinstance(upstream.partitioning, TimePartitionConfig)
                    and isinstance(operation.partitioning, TimePartitionConfig)
                    and upstream.partitioning.granularity is not operation.partitioning.granularity
                ):
                    raise DAGError(
                        f"Invalid upstream: '{upstream.key}' is partitioned by "
                        f"{upstream.partitioning.granularity.value} but its dependent '{operation.key}' by "
                        f"{operation.partitioning.granularity.value}; a run has one partition scope, so both "
                        f"ends of an edge must share a granularity."
                    )

    # -- Traversal -------------------------------------------------------------

    def topological_generations(self) -> list[list[Operation]]:
        """Return operations grouped by parallelizable generations.

        Each inner list contains operations that can be executed in parallel.
        Lists are ordered so that all dependencies of a level appear in
        previous levels (Kahn's algorithm).

        Only materializable operations appear in the generations.  Edges from
        non-materializable operations count as already satisfied — mirroring
        the runners, which mark those nodes as skipped (e.g. the parents
        in a :meth:`mini_dag`).

        Returns:
            A list of operation groups ordered by dependency level.

        Raises:
            CircularDependencyError: If a cycle is detected.
        """
        in_degree = {
            key: sum(1 for pred in preds if pred in self.predecessors) for key, preds in self.predecessors.items()
        }
        current_level = sorted(key for key, degree in in_degree.items() if degree == 0)
        levels: list[list[Operation]] = []

        processed = 0
        while current_level:
            levels.append([self.operation_map[key] for key in current_level])

            next_level: list[str] = []
            for operation_id in current_level:
                processed += 1
                for dependent_id, preds in self.predecessors.items():
                    if operation_id in preds:
                        in_degree[dependent_id] -= 1
                        if in_degree[dependent_id] == 0:
                            next_level.append(dependent_id)

            current_level = sorted(next_level)

        if processed != len(self.predecessors):
            raise CircularDependencyError("Circular dependency detected in DAG")

        return levels

    def get_predecessors(self, operation_id: str) -> list[str]:
        """Return upstream dependency ids for the given operation.

        Args:
            operation_id: Id of the operation to look up.

        Raises:
            AssetNotFoundError: If the operation id is not in the DAG.
        """
        if operation_id not in self.operation_map:
            raise AssetNotFoundError(f"Operation '{operation_id}' not found in DAG")
        return self.predecessors.get(operation_id, [])

    def get_successors(self, operation_id: str) -> list[str]:
        """Return downstream dependent ids for the given operation.

        Args:
            operation_id: Id of the operation to look up.

        Raises:
            AssetNotFoundError: If the operation id is not in the DAG.
        """
        if operation_id not in self.operation_map:
            raise AssetNotFoundError(f"Operation '{operation_id}' not found in DAG")
        return self.successors.get(operation_id, [])

    # -- Materialization -------------------------------------------------------

    def materialize(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
    ) -> RunResult:
        """Execute all operations in dependency order using a default ``AsyncRunner``.

        Sync entrypoint for scripts, REPLs, and notebooks — drives
        :meth:`materialize_async` to completion on the bridge loop
        (see :func:`interloper.run`)::

            result = dag.materialize(partition)

        Async code awaits :meth:`materialize_async` instead.

        Args:
            partition_or_window: Partition or PartitionWindow every operation in
                the DAG is run for. ``None`` for an unpartitioned DAG.

        Returns:
            The result of the DAG execution.
        """
        from interloper.utils import concurrency

        return concurrency.run(self.materialize_async(partition_or_window))

    async def materialize_async(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
    ) -> RunResult:
        """Execute all operations in dependency order using a default ``AsyncRunner``.

        Args:
            partition_or_window: Partition or PartitionWindow every operation in
                the DAG is run for. ``None`` for an unpartitioned DAG.

        Returns:
            The result of the DAG execution.
        """
        from interloper.runner.async_runner import AsyncRunner

        span_attrs: dict[str, Any] = {attributes.DAG_OPERATION_COUNT: len(self.operations)}
        if partition_or_window is not None:
            span_attrs[attributes.PARTITION] = str(partition_or_window)
        with tracer().start_as_current_span("interloper.dag.materialize", attributes=span_attrs) as span:
            result = await AsyncRunner().run(dag=self, partition_or_window=partition_or_window)
            # A failed run is returned, not raised — without this the trace's
            # root span reads OK while the failure sits on a descendant.
            if result.status is ExecutionStatus.FAILED:
                span.set_status(StatusCode.ERROR, f"{len(result.failed_ids)} operation(s) failed")
            return result

    # -- Serialization ---------------------------------------------------------

    def to_spec(self) -> DAGSpec:
        """Serialize this DAG to a reconstructible spec.

        One item per root of the graph: a standalone asset is an item of its
        own, and a source-owned asset travels inside its owning source's item
        through the asset-override map, which is what also gives the parent
        of an upstream this run only reads an item of its own, carrying that
        one asset and nothing else of the source.

        The override map is built from the DAG's **actual** asset
        instances, which may differ from the source's originals: in a
        mini-DAG the parents are flagged ``materializable=False``, and a run
        may hold only some of a source's assets.

        Every item shares one traversal, so a destination bound to several
        roots is written out once and referenced everywhere else, and the
        document is closed: a binding pointing outside this graph is left
        out rather than written as a reference nothing answers (see
        :meth:`_unwritable_relations`).

        Returns:
            A DAGSpec that can reconstruct an equivalent DAG.
        """
        items: list[Spec] = []
        seen: set[str] = set()

        # Group the DAG's operations by owning source, preserving their state
        source_operations: dict[str, list[Operation]] = {}
        for operation in self.operations:
            source = operation.source
            if source is None:
                node = cast("Component", operation)
                items.append(node._to_spec(seen=seen, drop=self._unwritable_relations(operation)))
                continue
            source_operations.setdefault(source.id, []).append(operation)

        for operations in source_operations.values():
            source = operations[0].source
            assert source is not None
            items.append(
                source._source_spec(
                    seen=seen,
                    assets=cast("list[Asset]", operations),
                    asset_drop={operation.key: self._unwritable_relations(operation) for operation in operations},
                )
            )

        return DAGSpec(items=items)

    def _unwritable_relations(self, operation: Operation) -> dict[str, set[str]]:
        """The target ids of one node's relations this document cannot carry.

        A target that has an owner travels inside that owner's own item, so a
        binding pointing at one this graph does not hold has nowhere to go: a
        node the run only reads keeps the bindings of the live asset it was
        copied from, whose own upstreams were never pulled in with it
        (see :meth:`_include_read_only_upstreams`). Writing those out would
        put a reference in the document that reconstruction cannot answer,
        so they are dropped per target rather than the whole relation: a
        relation keeping some of its targets emits a shorter list than the
        live binding, and one losing all of them is left out of the document
        entirely.

        Args:
            operation: The node whose bindings are read.

        Returns:
            The absent owned target ids to drop, keyed by the relation name
            that holds them; a relation with nothing to drop is absent from
            the mapping.
        """
        unwritable: dict[str, set[str]] = {}
        for name in operation.relations:
            bound = operation.bound(name)
            targets = bound if isinstance(bound, list) else [] if bound is None else [bound]
            absent = {
                target.id for target in targets if target.parent is not None and target.id not in self.operation_map
            }
            if absent:
                unwritable[name] = absent
        return unwritable

    @classmethod
    def from_spec(
        cls,
        spec: DAGSpec,
        catalog: Catalog | None = None,
        *,
        resolve: Callable[[str], Component] | None = None,
    ) -> DAG:
        """Reconstruct a DAG from a spec.

        Args:
            spec: A DAGSpec produced by :meth:`to_spec`.
            catalog: Catalog used to resolve ``key`` references, shared
                across the spec's items. Defaults to the settings-configured
                catalog, built lazily.
            resolve: Called with the id of a reference no item of the spec
                carries; ``None`` makes such a reference an error.

        Returns:
            A new DAG with the same structure.
        """
        return spec.reconstruct(catalog, resolve=resolve)

    @classmethod
    def from_spec_file(
        cls,
        path: str | Path,
        catalog: Catalog | None = None,
        *,
        resolve: Callable[[str], Component] | None = None,
    ) -> DAG:
        """Compile a runnable component spec file into a DAG.

        Loads every :class:`~interloper.serializable.base.Spec` document the
        file holds (with ``${VAR}`` env interpolation), reconstructs them
        against one shared registry so a reference may cross documents, and
        compiles the DAG over all of them: the file-based counterpart of
        targeting a runnable component by id, and what
        :meth:`to_spec` writes back.

        Args:
            path: Path to the YAML spec document(s).
            catalog: Catalog used to resolve ``key`` references. Defaults to
                the settings-configured catalog, built lazily.
            resolve: Called with the id of a reference no document carries;
                ``None`` makes such a reference an error.

        Invalid documents surface as ``SpecError`` from the spec loader.

        Returns:
            The DAG over every root the file declares.

        Raises:
            DAGError: If a root's kind declares no workload.
        """
        from interloper.component.base import Component

        registry: dict[str, Component] = {}
        roots = [spec.reconstruct(catalog, resolve=resolve, registry=registry) for spec in Spec.all_from_file(path)]
        Component._bind_references(registry, resolve)
        for root in roots:
            if not isinstance(root, Workload):
                raise DAGError(f"'{getattr(root, 'kind', type(root).__name__)}' components are not runnable")
        return cls(*cast("list[Workload]", roots))

    # -- Subgraph --------------------------------------------------------------

    def mini_dag(self, operation_id: str) -> DAG:
        """Create a mini-DAG with the target operation and its immediate parents.

        A DAG over the target alone: its bound upstreams join as
        non-materializable copies through the very mechanism any run uses for
        an upstream it does not materialize
        (:meth:`_include_read_only_upstreams`), so the parents are there,
        under their own ids, read instead of executed.

        Args:
            operation_id: Id of the operation the mini-DAG is built around.

        Returns:
            A new DAG containing only the target operation and its parents.

        Raises:
            AssetNotFoundError: If the operation id is not in the DAG.
        """
        if operation_id not in self.operation_map:
            raise AssetNotFoundError(f"Operation '{operation_id}' not found in DAG")

        target = self.operation_map[operation_id]
        # Only an asset has upstreams to pull in, and only an asset can be re-flagged.
        return DAG(target(materializable=True) if isinstance(target, Asset) else target)
