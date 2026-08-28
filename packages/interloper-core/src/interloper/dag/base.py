"""Directed Acyclic Graph for asset dependency resolution and execution ordering."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from opentelemetry.trace import StatusCode
from pydantic import BaseModel

from interloper.asset.base import Asset, AssetIdentity
from interloper.component import Component
from interloper.errors import AssetNotFoundError, CircularDependencyError, DAGError, DependencyNotFoundError
from interloper.partitioning import Partition, PartitionWindow
from interloper.runner.results import ExecutionStatus, RunResult
from interloper.serializable import Spec
from interloper.source.base import Source
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
    """Directed acyclic graph of assets.

    Dependencies are resolved from pre-computed ``dependencies`` on each asset
    (mapping parameter names to upstream asset ids).  The DAG validates
    the wiring and provides topological ordering for parallel execution.
    """

    def __init__(self, *items: Asset | Source | type[Asset | Source]) -> None:
        """Create a DAG from assets and/or sources.

        Args:
            *items: Asset/Source instances or classes to include.
        """
        self.assets: list[Asset] = []
        self.asset_map: dict[str, Asset] = {}
        self.predecessors: dict[str, list[str]] = {}
        self.successors: dict[str, list[str]] = {}
        self._build_graph(items)
        self._validate()

    def _build_graph(self, items: tuple[Asset | Source | type[Asset | Source], ...]) -> None:
        """Build the dependency graph from assets and sources.

        Args:
            items: The DAG's constructor arguments — Asset/Source instances or
                classes; classes are instantiated and sources flattened into
                their assets.

        Raises:
            DAGError: If the input is empty, contains duplicates, or has invalid types.
            DependencyNotFoundError: If a dependency is not found in the DAG.
        """
        if not items:
            raise DAGError("DAG must contain at least one asset or source")

        for item in items:
            if isinstance(item, type) and issubclass(item, Source):
                item = item()
            if isinstance(item, type) and issubclass(item, Asset):
                item = item()

            if isinstance(item, Source):
                self.assets.extend(item.assets)
            elif isinstance(item, Asset):
                self.assets.append(item)
            else:
                raise DAGError(f"Expected Asset or Source, got {type(item)}")

        self.asset_map = {asset.id: asset for asset in self.assets}

        if len(self.asset_map) != len(self.assets):
            seen: set[str] = set()
            duplicates: list[str] = []
            for asset in self.assets:
                if asset.id in seen:
                    duplicates.append(asset.id)
                seen.add(asset.id)
            raise DAGError(f"Duplicate asset id found: {duplicates}")

        for asset in self.assets:
            self.successors[asset.id] = []

        # Build dependency graph from resolved dependencies
        for asset in self.assets:
            if not asset.materializable:
                continue

            self.predecessors[asset.id] = []

            for parameter_name, upstream_id in asset.dependencies.items():
                if upstream_id not in self.asset_map:
                    if parameter_name in asset.optional_requires:
                        continue
                    raise DependencyNotFoundError(
                        f"Asset '{asset.key}' dep '{parameter_name}' points to id '{upstream_id}' "
                        f"which is not in the DAG."
                    )

                self.predecessors[asset.id].append(upstream_id)
                self.successors[upstream_id].append(asset.id)

    # -- Validation ------------------------------------------------------------

    def _validate(self) -> None:
        """Validate the DAG structure."""
        self._check_requires()
        self._check_circular_dependencies()
        self._check_partition_dependencies()

    def _check_requires(self) -> None:
        """Validate that wired dependencies match the requires contract.

        For each ``(parameter_name, upstream_id)`` in ``asset.dependencies``, if
        ``requires`` or ``optional_requires`` declares an expected key for
        that parameter, the wired upstream's identity must match the declared
        key's resolution (bare keys expect an asset of the declarer's own
        source — see :meth:`~interloper.asset.base.AssetIdentity.resolve`).

        Raises:
            DependencyContractError: If any wired dep violates its contract.
        """
        from interloper.errors import DependencyContractError

        for asset in self.assets:
            if not asset.materializable:
                continue
            own_source_key = asset._source.key if asset._source is not None else None
            for parameter_name, upstream_id in asset.dependencies.items():
                if upstream_id not in self.asset_map:
                    continue  # Missing dependencies are caught in _build_graph

                expected_key = asset.requires.get(parameter_name) or asset.optional_requires.get(parameter_name)
                if not expected_key:
                    continue

                expected = AssetIdentity.resolve(expected_key, own_source_key=own_source_key)
                upstream = self.asset_map[upstream_id]
                if upstream.identity != expected:
                    raise DependencyContractError(
                        f"Asset '{asset.key}' parameter '{parameter_name}' requires "
                        f"'{expected_key}' but is wired to '{upstream.identity}'."
                    )

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

        for asset_id in self.predecessors:
            if asset_id not in visited and has_cycle(asset_id):
                asset = self.asset_map[asset_id]
                raise CircularDependencyError(f"Circular dependency detected involving asset '{asset.key}'")

    def _check_partition_dependencies(self) -> None:
        """Check that no non-partitioned asset depends on a partitioned asset.

        Raises:
            DAGError: If a non-partitioned asset depends on a partitioned asset.
        """
        for asset_id, preds in self.predecessors.items():
            asset = self.asset_map[asset_id]
            for pred_id in preds:
                upstream = self.asset_map[pred_id]
                if upstream.partitioning is not None and asset.partitioning is None:
                    raise DAGError(
                        f"Invalid dependency: partitioned asset '{upstream.key}' "
                        f"cannot be a dependency of non-partitioned asset '{asset.key}'"
                    )

    # -- Traversal -------------------------------------------------------------

    def topological_generations(self) -> list[list[Asset]]:
        """Return assets grouped by parallelizable generations.

        Each inner list contains assets that can be executed in parallel.
        Lists are ordered so that all dependencies of a level appear in
        previous levels (Kahn's algorithm).

        Only materializable assets appear in the generations.  Edges from
        non-materializable assets count as already satisfied — mirroring
        the runners, which mark those assets as skipped (e.g. the parents
        in a :meth:`mini_dag`).

        Returns:
            A list of asset groups ordered by dependency level.

        Raises:
            CircularDependencyError: If a cycle is detected.
        """
        in_degree = {
            key: sum(1 for pred in preds if pred in self.predecessors) for key, preds in self.predecessors.items()
        }
        current_level = sorted(key for key, degree in in_degree.items() if degree == 0)
        levels: list[list[Asset]] = []

        processed = 0
        while current_level:
            levels.append([self.asset_map[key] for key in current_level])

            next_level: list[str] = []
            for asset_id in current_level:
                processed += 1
                for dependent_id, preds in self.predecessors.items():
                    if asset_id in preds:
                        in_degree[dependent_id] -= 1
                        if in_degree[dependent_id] == 0:
                            next_level.append(dependent_id)

            current_level = sorted(next_level)

        if processed != len(self.predecessors):
            raise CircularDependencyError("Circular dependency detected in DAG")

        return levels

    def get_predecessors(self, asset_id: str) -> list[str]:
        """Return upstream dependency ids for the given asset.

        Args:
            asset_id: Id of the asset to look up.

        Raises:
            AssetNotFoundError: If the asset id is not in the DAG.
        """
        if asset_id not in self.asset_map:
            raise AssetNotFoundError(f"Asset '{asset_id}' not found in DAG")
        return self.predecessors.get(asset_id, [])

    def get_successors(self, asset_id: str) -> list[str]:
        """Return downstream dependent ids for the given asset.

        Args:
            asset_id: Id of the asset to look up.

        Raises:
            AssetNotFoundError: If the asset id is not in the DAG.
        """
        if asset_id not in self.asset_map:
            raise AssetNotFoundError(f"Asset '{asset_id}' not found in DAG")
        return self.successors.get(asset_id, [])

    # -- Materialization -------------------------------------------------------

    def materialize(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
    ) -> RunResult:
        """Execute all assets in dependency order using a default ``AsyncRunner``.

        Sync entrypoint for scripts, REPLs, and notebooks — drives
        :meth:`materialize_async` to completion on the bridge loop
        (see :func:`interloper.run`)::

            result = dag.materialize(partition)

        Async code awaits :meth:`materialize_async` instead.

        Args:
            partition_or_window: Partition or PartitionWindow every asset in the
                DAG is run for. ``None`` for an unpartitioned DAG.

        Returns:
            The result of the DAG execution.
        """
        from interloper.utils import concurrency

        return concurrency.run(self.materialize_async(partition_or_window))

    async def materialize_async(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
    ) -> RunResult:
        """Execute all assets in dependency order using a default ``AsyncRunner``.

        Args:
            partition_or_window: Partition or PartitionWindow every asset in the
                DAG is run for. ``None`` for an unpartitioned DAG.

        Returns:
            The result of the DAG execution.
        """
        from interloper.runner.async_runner import AsyncRunner

        span_attrs: dict[str, Any] = {attributes.DAG_ASSET_COUNT: len(self.assets)}
        if partition_or_window is not None:
            span_attrs[attributes.PARTITION] = str(partition_or_window)
        with tracer().start_as_current_span("interloper.dag.materialize", attributes=span_attrs) as span:
            result = await AsyncRunner().run(dag=self, partition_or_window=partition_or_window)
            # A failed run is returned, not raised — without this the trace's
            # root span reads OK while the failure sits on a descendant.
            if result.status is ExecutionStatus.FAILED:
                span.set_status(StatusCode.ERROR, f"{len(result.failed_assets)} asset(s) failed")
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

        # Group DAG assets by source, preserving their current state
        source_assets: dict[str, list[Asset]] = {}
        for asset in self.assets:
            source = asset._source
            if source is None:
                items.append(asset.to_spec())
                continue
            source_assets.setdefault(source.id, []).append(asset)

        # For each source, build a spec using the DAG's asset states
        for assets in source_assets.values():
            source = assets[0]._source
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
            DAGError: If the component's kind is not runnable.
        """
        from interloper.component.base import Component
        from interloper.job.base import Job

        component = Component.from_spec_file(path, catalog)
        if not type(component).runnable:
            raise DAGError(f"'{component.kind}' components are not runnable")
        items = component.targets if isinstance(component, Job) else [component]
        return cls(*items)  # ty: ignore[invalid-argument-type]

    # -- Subgraph --------------------------------------------------------------

    def mini_dag(self, asset_id: str) -> DAG:
        """Create a mini-DAG with the target asset and its immediate parents.

        Parents are included but marked as non-materializable so only the
        target asset is actually executed.

        Args:
            asset_id: Id of the asset the mini-DAG is built around.

        Returns:
            A new DAG containing only the target asset and its parents.

        Raises:
            AssetNotFoundError: If the asset id is not in the DAG.
        """
        if asset_id not in self.asset_map:
            raise AssetNotFoundError(f"Asset '{asset_id}' not found in DAG")

        target = self.asset_map[asset_id]
        assets: list[Asset] = []
        for upstream_id in self.get_predecessors(asset_id):
            parent = self.asset_map[upstream_id](materializable=False)
            assets.append(parent)
        assets.append(target)
        return DAG(*assets)
