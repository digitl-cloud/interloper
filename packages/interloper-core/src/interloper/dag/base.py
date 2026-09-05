"""Directed Acyclic Graph for operation dependency resolution and execution ordering."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from opentelemetry.trace import StatusCode
from pydantic import BaseModel

from interloper.asset.base import Asset
from interloper.component import Component
from interloper.errors import AssetNotFoundError, CircularDependencyError, DAGError, DependencyNotFoundError
from interloper.operation import Operation, Workload
from interloper.partitioning import Partition, PartitionWindow
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

    def reconstruct(self, catalog: Catalog | None = None) -> DAG:
        """Reconstruct the DAG from its spec.

        Each source spec materialises a live source (with its assets
        pre-bound through ``Source.model_post_init`` → ``_resolve``),
        and each standalone asset spec materialises a bare asset.  All
        reconstructed items are then handed to the :class:`DAG`
        constructor which re-infers the dependency graph from the
        preserved asset ids.

        Args:
            catalog: Catalog used to resolve ``key`` references, shared
                across all items. Defaults to the settings-configured
                catalog, built lazily.

        Returns:
            A new DAG instance with the same structure as the original.
        """
        with tracer().start_as_current_span(
            "interloper.dag_spec.reconstruct",
            attributes={attributes.DAG_SPEC_ITEMS: len(self.items)},
        ):
            reconstructed = [Component.from_spec(spec, catalog) for spec in self.items]
            return DAG(*reconstructed)  # ty: ignore[invalid-argument-type]


# -- DAG -----------------------------------------------------------------------
class DAG:
    """Directed acyclic graph of operations.

    Dependencies are resolved from pre-computed ``upstreams`` on each node
    (mapping parameter names to upstream node ids).  The DAG validates
    the wiring and provides topological ordering for parallel execution.
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
            items: The DAG's constructor arguments — Workload instances or
                classes; classes are instantiated and every workload is
                flattened into the operations it provides.

        Raises:
            DAGError: If the input is empty, contains duplicates, or has invalid types.
            DependencyNotFoundError: If a dependency is not found in the DAG.
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

        for operation in self.operations:
            self.successors[operation.id] = []

        self._resolve_declared()

        for operation in self.operations:
            if not operation.materializable:
                continue

            self.predecessors[operation.id] = []
            declared = operation.declared_upstreams()
            for parameter_name, upstream_ids in operation.upstreams.items():
                dependency = declared.get(parameter_name)
                optional = dependency is not None and dependency.optional
                for upstream_id in upstream_ids:
                    if upstream_id not in self.operation_map:
                        if optional:
                            continue
                        raise DependencyNotFoundError(
                            f"'{operation.key}' upstream '{parameter_name}' points to id '{upstream_id}' "
                            f"which is not in the DAG."
                        )
                    self.predecessors[operation.id].append(upstream_id)
                    self.successors[upstream_id].append(operation.id)

    def _resolve_declared(self) -> None:
        """Wire declared upstream keys that nothing has wired yet.

        For every live asset and every unwired declaration, the candidates
        are the other assets in the DAG whose identity satisfies the key; a
        bare key is further restricted to the asset's own source instance. A
        many-valued slot binds every candidate; a single slot binds exactly
        one. Wiring writes the asset's ``upstreams`` in place, the same way a
        source wires its siblings, so specs and the CLI need no extra step
        for cross-source contracts.

        Raises:
            DAGError: If a single slot has several candidates; the caller
                must wire it explicitly.
        """
        assets = [operation for operation in self.operations if isinstance(operation, Asset)]
        for asset in assets:
            if not asset.materializable:
                continue
            own_source_key = asset.source.key if asset.source is not None else None
            for parameter_name, dependency in asset.declared_upstreams().items():
                if asset.upstreams.get(parameter_name) or not dependency.key:
                    continue
                bare = "." not in dependency.key
                candidates = [
                    candidate
                    for candidate in assets
                    if candidate is not asset
                    and candidate.identity.satisfies(dependency.key, own_source_key=own_source_key)
                    and (not bare or candidate.source is asset.source)
                ]
                if not candidates:
                    continue
                if dependency.many:
                    asset.upstreams[parameter_name] = [candidate.id for candidate in candidates]
                elif len(candidates) > 1:
                    listed = ", ".join(f"{candidate.qualified_key}#{candidate.id[:8]}" for candidate in candidates)
                    raise DAGError(
                        f"'{asset.qualified_key}' parameter '{parameter_name}' depends on '{dependency.key}' and "
                        f"the DAG holds {len(candidates)} matching assets ({listed}); wire "
                        f"upstreams['{parameter_name}'] explicitly."
                    )
                else:
                    asset.upstreams[parameter_name] = [candidates[0].id]

    # -- Validation ------------------------------------------------------------

    def _validate(self) -> None:
        """Validate the DAG structure."""
        self._check_upstreams()
        self._check_circular_dependencies()
        self._check_partition_dependencies()

    def _check_upstreams(self) -> None:
        """Let every live node validate its contract (see ``Asset.validate_upstreams``)."""
        for operation in self.operations:
            if operation.materializable:
                operation.validate_upstreams(self.operation_map)

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
        """Check that no non-partitioned operation depends on a partitioned one.

        Raises:
            DAGError: If a non-partitioned operation depends on a partitioned one.
        """
        for operation_id, preds in self.predecessors.items():
            operation = self.operation_map[operation_id]
            for pred_id in preds:
                upstream = self.operation_map[pred_id]
                if upstream.partitioning is not None and operation.partitioning is None:
                    raise DAGError(
                        f"Invalid dependency: partitioned asset '{upstream.key}' "
                        f"cannot be a dependency of non-partitioned asset '{operation.key}'"
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

        Assets are grouped by their parent source before serialization:
        source-owned assets travel as part of their parent source's
        spec (via the asset-override map), while standalone assets are
        serialised individually.

        The override map is built from the DAG's **actual** asset
        instances (which may differ from the source's originals — e.g.
        in a mini-DAG, parents are marked ``materializable=False``).

        Returns:
            A DAGSpec that can reconstruct an equivalent DAG.
        """
        items: list[Spec] = []

        # Group the DAG's operations by owning source, preserving their state
        source_operations: dict[str, list[Operation]] = {}
        for operation in self.operations:
            source = operation.source
            if source is None:
                items.append(operation.to_spec())
                continue
            source_operations.setdefault(source.id, []).append(operation)

        # For each source, build a spec using the DAG's asset states
        for assets in source_operations.values():
            source = assets[0].source
            assert source is not None

            # Build the source spec but override the assets with THIS
            # DAG's copies (which may have modified materializable, etc.)
            spec = source.to_spec()
            if spec.init is not None:
                overrides: dict[str, Any] = {}
                for asset in assets:
                    asset_spec = asset.to_spec()
                    asset_init = dict(asset_spec.init or {})
                    if asset_spec.id:
                        asset_init["id"] = asset_spec.id
                    overrides[asset.key] = asset_init
                spec.init["assets"] = overrides
            items.append(spec)

        return DAGSpec(items=items)

    @classmethod
    def from_spec(cls, spec: DAGSpec, catalog: Catalog | None = None) -> DAG:
        """Reconstruct a DAG from a spec.

        Args:
            spec: A DAGSpec produced by :meth:`to_spec`.
            catalog: Catalog used to resolve ``key`` references, shared
                across the spec's items. Defaults to the settings-configured
                catalog, built lazily.

        Returns:
            A new DAG with the same structure.
        """
        return spec.reconstruct(catalog)

    @classmethod
    def from_spec_file(cls, path: str | Path, catalog: Catalog | None = None) -> DAG:
        """Compile a runnable component spec file into a DAG.

        Loads a :class:`~interloper.component.base.Spec` document
        (with ``${VAR}`` env interpolation), reconstructs the component, and
        compiles its DAG — the file-based counterpart of targeting a runnable
        component by id.

        Args:
            path: Path to the YAML spec document.
            catalog: Catalog used to resolve ``key`` references. Defaults to
                the settings-configured catalog, built lazily.

        Invalid documents surface as ``SpecError`` from the spec loader.

        Returns:
            The component's DAG.

        Raises:
            DAGError: If the component's kind declares no workload.
        """
        from interloper.component.base import Component

        component = Component.from_spec_file(path, catalog)
        if not isinstance(component, Workload):
            raise DAGError(f"'{component.kind}' components are not runnable")
        return cls(component)

    # -- Subgraph --------------------------------------------------------------

    def mini_dag(self, operation_id: str) -> DAG:
        """Create a mini-DAG with the target operation and its immediate parents.

        Parents are included but marked as non-materializable so only the
        target operation is actually executed.

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
        operations: list[Operation] = []
        for upstream_id in self.get_predecessors(operation_id):
            # Wired upstreams are assets by relation schema.
            parent = cast(Asset, self.operation_map[upstream_id])(materializable=False)
            operations.append(parent)
        operations.append(target)
        return DAG(*operations)
