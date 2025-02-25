# import datetime as dt
from dataclasses import dataclass
from typing import Any

import networkx as nx

from interloper.core.asset import Asset
from interloper.core.partitioning import Partition
from interloper.core.source import Source


@dataclass(frozen=True)
class ExecutionContext:
    assets: dict[str, Asset]
    executed_asset: Asset
    partition: Partition | None = None
    # date_window: tuple[dt.date, dt.date] | None = None

    # def __post_init__(self):
    #     if self.partition and self.date_window:
    #         raise ValueError("Invalid execution context: only one of 'date' or 'date_window' can be provided")

    #     if self.date_window:
    #         start_date, end_date = self.date_window
    #         if start_date > end_date:
    #             raise ValueError("Invalid execution context:  start date must be less than or equal to end date")


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
        for asset in self._get_execution_order():
            context = ExecutionContext(
                assets=self.assets,
                executed_asset=asset,
                partition=partition,
            )
            asset.materialize(context)

    # def backfill(
    #     self,
    #     start: dt.date,
    #     end: dt.date,
    # ) -> None:
    #     for asset in self._get_execution_order():
    #         context = ExecutionContext(
    #             assets=self.assets,
    #             executed_asset=asset,
    #             date_window=(start, end),
    #         )
    #         asset.materialize(context)

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
                        f"Upstream asset '{upstream_asset_name}' of asset '{{asset.name}}' not found in asset graph"
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

    def _get_execution_order(self) -> list[Asset]:
        """Returns the execution order of assets."""

        return list(nx.topological_sort(self.graph))
