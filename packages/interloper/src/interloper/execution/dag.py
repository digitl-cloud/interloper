import functools
import logging
from collections.abc import Sequence

import networkx as nx

import interloper as itlp
from interloper.asset import Asset
from interloper.execution.strategy import ExecutionStategy
from interloper.source import Source

TAssetOrSource = Source | Asset | Sequence[Source | Asset]

itlp.basic_logging(logging.CRITICAL)


# TODO: subgraphs should probably be immutable (DAGView?) since some upstream assets might not be present in the graph
# anymore. The consequence is that subdags cannot be built again (_build_graph). Adding new assets (add_assets) to
# will always fail because it will build the graph again and the upstream assets will then not be present.
class DAG:
    _graph: nx.DiGraph
    _assets: dict[str, Asset]

    def __init__(
        self,
        sources_or_assets: TAssetOrSource | None = None,
    ):
        self._assets = {}
        if sources_or_assets:
            self.add_assets(sources_or_assets)

    @property
    def graph(self) -> nx.DiGraph:
        return self._graph

    def add_assets(self, sources_or_assets: TAssetOrSource) -> None:
        # Convert single source/asset to a list
        if not isinstance(sources_or_assets, Sequence):
            sources_or_assets = [sources_or_assets]

        # Unpack assets or source's assets
        batch: list[Asset] = []
        for source_or_asset in sources_or_assets:
            batch = []
            if isinstance(source_or_asset, Source):
                batch.extend([asset for asset in source_or_asset.assets if asset.materializable])
            elif isinstance(source_or_asset, Asset):
                if source_or_asset.materializable:
                    batch.append(source_or_asset)
            else:
                raise ValueError(f"Expected an instance of Source or Asset, but got {type(source_or_asset)}")

            for asset in batch:
                if asset in self._assets:
                    raise ValueError(f"Duplicate asset name '{asset.id}'")
                self._assets[asset.id] = asset

        self._build_graph()
        self._clear_cache()

    def _build_graph(self) -> None:
        """Builds the asset dependency graph."""

        self._graph = nx.DiGraph()
        for asset in self._assets.values():
            self._graph.add_node(asset)

            for upstream_ref in asset.upstream_assets:
                # Verify that the upstream asset ref is in the asset's deps config
                if upstream_ref.key not in asset.deps.keys():
                    raise ValueError(f"Unable to resolve upstream asset '{upstream_ref.key}' of asset '{asset.name}'")

                upstream_asset_id = asset.deps[upstream_ref.key]

                # Check if the dependency exists in the asset map
                if upstream_asset_id not in self._assets:
                    raise ValueError(
                        f"Upstream asset '{upstream_asset_id}' of asset '{asset.name}' not found in asset graph"
                    )

                upstream_asset = self._assets[upstream_asset_id]
                if not asset.is_partitioned and upstream_asset.is_partitioned:
                    raise ValueError(
                        "Invalid asset graph: a non-partitioned asset cannot have a partitioned upstream asset. "
                        f"Asset '{asset.name}' is not partitioned, but its upstream asset '{upstream_asset.name}' is."
                    )

                # Get the corresponding asset and add to the graph
                self._graph.add_edge(upstream_asset, asset)

        # Detect cycles
        try:
            nx.find_cycle(self._graph, orientation="original")
            raise ValueError("Circular dependency detected in the asset dependency graph")
        except nx.NetworkXNoCycle:
            pass

        assert nx.is_directed_acyclic_graph(self._graph)

    def _clear_cache(self) -> None:
        DAG.assets_by_sources.fget.cache_clear()  # type: ignore
        DAG.assets_by_execution_strategy.fget.cache_clear()  # type: ignore
        DAG.non_partitioned_subdag.fget.cache_clear()  # type: ignore
        DAG.partitioned_subdag.fget.cache_clear()  # type: ignore
        DAG.execution_strategy.fget.cache_clear()  # type: ignore
        DAG.supports_partitioning.fget.cache_clear()  # type: ignore
        DAG.supports_partitioning_window.fget.cache_clear()  # type: ignore
        self.split.cache_clear()

    @property
    def assets(self) -> dict[str, Asset]:
        return self._assets

    @property
    @functools.cache
    def assets_by_sources(self) -> dict[str | None, list[Asset]]:
        assets_by_source: dict[str | None, list[Asset]] = {}
        for asset in self._assets.values():
            source_id = asset.source.name if asset.source else None
            if source_id not in assets_by_source:
                assets_by_source[source_id] = []
            assets_by_source[source_id].append(asset)
        return assets_by_source

    @property
    @functools.cache
    def assets_by_execution_strategy(self) -> dict[ExecutionStategy, list[Asset]]:
        assets_by_execution_strategy: dict[ExecutionStategy, list[Asset]] = {
            ExecutionStategy.NOT_PARTITIONED: [],
            ExecutionStategy.PARTITIONED_MULTI_RUNS: [],
            ExecutionStategy.PARTITIONED_SINGLE_RUN: [],
        }
        for asset in self._assets.values():
            if not asset.partitioning:
                assets_by_execution_strategy[ExecutionStategy.NOT_PARTITIONED].append(asset)
            elif asset.partitioning.allow_window:
                assets_by_execution_strategy[ExecutionStategy.PARTITIONED_SINGLE_RUN].append(asset)
            else:
                assets_by_execution_strategy[ExecutionStategy.PARTITIONED_MULTI_RUNS].append(asset)
        return assets_by_execution_strategy

    @property
    @functools.cache
    def non_partitioned_subdag(self) -> "DAG":
        non_partitioned_assets = [asset for asset in self._assets.values() if not asset.is_partitioned]
        subgraph = nx.subgraph(self._graph, non_partitioned_assets)

        dag = DAG()
        dag._graph = subgraph
        dag._assets = {asset.id: asset for asset in non_partitioned_assets}
        assert dag.execution_strategy == ExecutionStategy.NOT_PARTITIONED
        return dag

    @property
    @functools.cache
    def partitioned_subdag(self) -> "DAG":
        partitioned_assets = [asset for asset in self._assets.values() if asset.is_partitioned]
        subgraph = nx.subgraph(self._graph, partitioned_assets)

        dag = DAG()
        dag._graph = subgraph
        dag._assets = {asset.id: asset for asset in partitioned_assets}
        assert (
            dag.execution_strategy == ExecutionStategy.PARTITIONED_MULTI_RUNS
            or dag.execution_strategy == ExecutionStategy.PARTITIONED_SINGLE_RUN
        )
        return dag

    @property
    @functools.cache
    def execution_strategy(self) -> ExecutionStategy:
        assets_by_execution_strategy = self.assets_by_execution_strategy
        if len(assets_by_execution_strategy[ExecutionStategy.PARTITIONED_SINGLE_RUN]) == len(self._assets):
            return ExecutionStategy.PARTITIONED_SINGLE_RUN
        elif len(assets_by_execution_strategy[ExecutionStategy.PARTITIONED_MULTI_RUNS]) == len(self._assets):
            return ExecutionStategy.PARTITIONED_MULTI_RUNS
        elif len(assets_by_execution_strategy[ExecutionStategy.NOT_PARTITIONED]) == len(self._assets):
            return ExecutionStategy.NOT_PARTITIONED
        elif (
            len(assets_by_execution_strategy[ExecutionStategy.PARTITIONED_SINGLE_RUN])
            + len(assets_by_execution_strategy[ExecutionStategy.PARTITIONED_MULTI_RUNS])
        ) == len(self._assets):
            return ExecutionStategy.PARTITIONED_MULTI_RUNS
        return ExecutionStategy.MIXED

    @property
    @functools.cache
    def supports_partitioning(self) -> bool:
        return all(asset.partitioning for asset in self._assets.values())

    @property
    @functools.cache
    def supports_partitioning_window(self) -> bool:
        return all(asset.partitioning and asset.partitioning.allow_window for asset in self._assets.values())

    @functools.cache
    def split(self) -> tuple["DAG", "DAG"]:
        non_partitioned_subdag = self.non_partitioned_subdag
        partitioned_subdag = self.partitioned_subdag
        return non_partitioned_subdag, partitioned_subdag
