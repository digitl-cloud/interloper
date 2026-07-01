"""Directed Acyclic Graph for asset dependency resolution and execution ordering."""

from __future__ import annotations

from typing import Any

from interloper.asset.base import Asset
from interloper.dag.spec import DAGSpec
from interloper.errors import AssetNotFoundError, CircularDependencyError, DAGError, DependencyNotFoundError
from interloper.partitioning import Partition, PartitionWindow
from interloper.runner.results import RunResult
from interloper.source.base import Source


class DAG:
    """Directed acyclic graph of assets.

    Dependencies are resolved from pre-computed ``deps`` on each asset
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

        for asset in self.assets:
            if not asset.materializable:
                continue

            self.predecessors[asset.id] = []

            for param_name, upstream_id in asset.deps.items():
                if upstream_id not in self.asset_map:
                    if param_name in type(asset).optional_requires:
                        continue
                    raise DependencyNotFoundError(
                        f"Asset '{asset.key}' dep '{param_name}' points to id '{upstream_id}' which is not in the DAG."
                    )

                self.predecessors[asset.id].append(upstream_id)
                self.successors[upstream_id].append(asset.id)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate(self) -> None:
        """Validate the DAG structure."""
        self._check_requires()
        self._check_circular_dependencies()
        self._check_partition_dependencies()

    def _check_requires(self) -> None:
        """Validate that wired deps match the requires contract.

        For each ``(param_name, upstream_id)`` in ``asset.deps``, if
        ``requires`` or ``optional_requires`` declares an expected key
        for that param, the upstream must match — either as a qualified
        key (``source_key.asset_key``) or a bare key.

        Raises:
            DependencyContractError: If any wired dep violates its contract.
        """
        from interloper.errors import DependencyContractError

        for asset in self.assets:
            if not asset.materializable:
                continue
            asset_cls = type(asset)
            for param_name, upstream_id in asset.deps.items():
                if upstream_id not in self.asset_map:
                    continue  # Missing deps are caught in _build_graph

                expected_key = asset_cls.requires.get(param_name) or asset_cls.optional_requires.get(param_name)
                if not expected_key:
                    continue

                upstream = self.asset_map[upstream_id]
                upstream_cls = type(upstream)
                upstream_source = upstream._source
                upstream_qk = (
                    f"{type(upstream_source).key}.{upstream_cls.key}"
                    if upstream_source is not None
                    else upstream_cls.key
                )

                if upstream_qk != expected_key and upstream_cls.key != expected_key:
                    raise DependencyContractError(
                        f"Asset '{asset_cls.key}' param '{param_name}' requires "
                        f"'{expected_key}' but is wired to '{upstream_qk}'."
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

    # ------------------------------------------------------------------
    # Traversal
    # ------------------------------------------------------------------

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

        Raises:
            AssetNotFoundError: If the asset id is not in the DAG.
        """
        if asset_id not in self.asset_map:
            raise AssetNotFoundError(f"Asset '{asset_id}' not found in DAG")
        return self.predecessors.get(asset_id, [])

    def get_successors(self, asset_id: str) -> list[str]:
        """Return downstream dependent ids for the given asset.

        Raises:
            AssetNotFoundError: If the asset id is not in the DAG.
        """
        if asset_id not in self.asset_map:
            raise AssetNotFoundError(f"Asset '{asset_id}' not found in DAG")
        return self.successors.get(asset_id, [])

    # ------------------------------------------------------------------
    # Materialization
    # ------------------------------------------------------------------

    async def materialize(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
    ) -> RunResult:
        """Execute all assets in dependency order using a default ``AsyncRunner``.

        Async-native; ``await`` it from async code, or drive it from a sync
        entrypoint with ``asyncio.run``::

            result = await dag.materialize(partition)        # async
            result = asyncio.run(dag.materialize(partition))  # sync edge

        Returns:
            The result of the DAG execution.
        """
        from interloper.runner.async_runner import AsyncRunner

        return await AsyncRunner().run(dag=self, partition_or_window=partition_or_window)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

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
        from interloper.component import ComponentSpec

        items: list[ComponentSpec] = []

        # Group DAG assets by source, preserving their current state
        source_assets: dict[str, list[Asset]] = {}
        for asset in self.assets:
            source = asset._source
            if source is None:
                items.append(asset.to_spec())
                continue
            source_assets.setdefault(source.id, []).append(asset)

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
                    overrides[type(asset).key] = asset_init
                spec.init["assets"] = overrides
            items.append(spec)

        return DAGSpec(items=items)

    @staticmethod
    def from_spec(spec: DAGSpec) -> DAG:
        """Reconstruct a DAG from a spec.

        Args:
            spec: A DAGSpec produced by :meth:`to_spec`.

        Returns:
            A new DAG with the same structure.
        """
        return spec.reconstruct()

    # ------------------------------------------------------------------
    # Subgraph
    # ------------------------------------------------------------------

    def mini_dag(self, asset_id: str) -> DAG:
        """Create a mini-DAG with the target asset and its immediate parents.

        Parents are included but marked as non-materializable so only the
        target asset is actually executed.

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
