import logging
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any

import networkx as nx

from interloper.asset import Asset
from interloper.partitioning.partitions import Partition
from interloper.partitioning.ranges import PartitionRange
from interloper.source import Source

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExecutionContext:
    assets: dict[str, Asset]
    executed_asset: Asset
    partition: Partition | PartitionRange | None = None


class Pipeline:
    def __init__(
        self,
        sources_or_assets: Source | Asset | list[Source | Asset],
    ):
        self.assets = {}
        self._add_assets(sources_or_assets)
        self._build_execution_graph()

    def materialize(
        self,
        partition: Partition | None = None,
    ) -> Any:
        for generation in self._get_parallel_execution_order():
            with ThreadPoolExecutor() as executor:
                futures = []
                for asset in generation:
                    context = ExecutionContext(
                        assets=self.assets,
                        executed_asset=asset,
                        partition=partition,
                    )
                    futures.append(executor.submit(asset.materialize, context))

                # Wait for all assets in this generation to complete
                wait(futures)

                # Re-raise any exceptions that occurred
                for future in futures:
                    future.result()

    def backfill(
        self,
        partitions: Iterator[Partition] | PartitionRange,
    ) -> None:
        for asset in self._get_sequential_execution_order():
            # Single run per asset
            if isinstance(partitions, PartitionRange) and asset.allows_partition_window:
                context = ExecutionContext(
                    assets=self.assets,
                    executed_asset=asset,
                    partition=partitions,
                )
                asset.materialize(context)

            # Multi runs per asset
            else:
                for partition in partitions:
                    context = ExecutionContext(
                        assets=self.assets,
                        executed_asset=asset,
                        partition=partition,
                    )
                    asset.materialize(context)

    def _add_assets(self, sources_or_assets: Source | Asset | list[Source | Asset]) -> None:
        # Convert single source/asset to a list
        if not isinstance(sources_or_assets, list):
            sources_or_assets = [sources_or_assets]

        # Unpack assets or source's assets
        for source_or_asset in sources_or_assets:
            batch = []
            if isinstance(source_or_asset, Source):
                batch.extend(source_or_asset.assets)
            elif isinstance(source_or_asset, Asset):
                batch.append(source_or_asset)
            else:
                raise ValueError(f"Expected an instance of Source or Asset, but got {type(source_or_asset)}")

            for asset in batch:
                if asset.name in self.assets:
                    raise ValueError(f"Duplicate asset name '{asset.name}'")
                self.assets[asset.name] = asset

    def _build_execution_graph(self) -> None:
        """Builds the asset dependency graph."""

        self.graph = nx.DiGraph()
        for asset in self.assets.values():
            self.graph.add_node(asset)

            for upstream_ref in asset.upstream_assets:
                # Verify that the upstream asset ref is in the asset's deps config
                if upstream_ref.name not in asset.deps.keys():
                    raise ValueError(f"Unable to resolve upstream asset '{upstream_ref.name}' of asset '{asset.name}'")

                upstream_asset_name = asset.deps[upstream_ref.name]

                # Check if the dependency exists in the asset map
                if upstream_asset_name not in self.assets:
                    raise ValueError(
                        f"Upstream asset '{upstream_asset_name}' of asset '{asset.name}' not found in asset graph"
                    )

                # Get the corresponding asset and add to the graph
                upstream_asset = self.assets[upstream_asset_name]
                self.graph.add_edge(upstream_asset, asset)

        # Detect cycles
        try:
            nx.find_cycle(self.graph, orientation="original")
            raise ValueError("Circular dependency detected in the asset dependency graph")
        except nx.NetworkXNoCycle:
            pass

        assert nx.is_directed_acyclic_graph(self.graph)

    def _get_sequential_execution_order(self) -> list[Asset]:
        """Returns the sequential execution order of assets."""

        order = list(nx.topological_sort(self.graph))
        logger.debug(f"Sequential execution order: {order}")
        return order

    def _get_parallel_execution_order(self) -> list[list[Asset]]:
        """Returns the parallel execution order of assets."""

        generations = list(nx.topological_generations(self.graph))
        logger.debug(f"Parallel execution order: {generations}")
        return generations
