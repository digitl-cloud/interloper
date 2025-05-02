import datetime as dt
import logging
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any

import networkx as nx

from interloper.asset import Asset
from interloper.execution.context import ExecutionContext
from interloper.execution.observable import Event, ExecutionStatus, ExecutionStep, Observable, Observer
from interloper.partitioning.config import PartitionConfig
from interloper.partitioning.partition import Partition
from interloper.partitioning.window import PartitionWindow
from interloper.source import Source

logger = logging.getLogger(__name__)
TAssetOrSource = Source | Asset | list[Source | Asset]


@dataclass
class AssetExecutionState:
    asset: Asset
    step: ExecutionStep | None = None
    status: ExecutionStatus | None = None
    started_at: dt.datetime | None = None
    completed_at: dt.datetime | None = None
    error: Exception | None = None

    @property
    def is_running(self) -> bool:
        return self.started_at is not None and self.completed_at is None

    @property
    def is_completed(self) -> bool:
        return self.completed_at is not None and self.status == ExecutionStatus.SUCCESS

    @property
    def is_failed(self) -> bool:
        return self.status == ExecutionStatus.FAILURE


class Pipeline(Observer, Observable):
    _assets: dict[str, Asset]
    _graph: nx.DiGraph

    def __init__(
        self,
        sources_or_assets: TAssetOrSource,
        on_event: Callable[["Pipeline", Event], None] | None = None,
        async_events: bool = False,
    ):
        Observer.__init__(self, is_async=async_events)
        Observable.__init__(self, observers=[self])

        self.on_event_callback = on_event
        self._assets = {}
        self._add_assets(sources_or_assets)
        self._build_execution_graph()
        self._async_events = async_events

        self.execution_state: dict[Asset, AssetExecutionState] = {
            asset: AssetExecutionState(asset) for asset in self._assets.values()
        }

    @property
    def assets(self) -> dict[str, Asset]:
        return self._assets

    @property
    def graph(self) -> nx.DiGraph:
        return self._graph

    @Observable.event(step=ExecutionStep.PIPELINE_MATERIALIZATION)
    def materialize(
        self,
        partition: Partition | None = None,
    ) -> Any:
        for generation in self._get_parallel_execution_order():
            with ThreadPoolExecutor() as executor:
                futures = []
                for asset in generation:
                    context = ExecutionContext(
                        assets=self._assets,
                        executed_asset=asset,
                        partition=partition,
                    )
                    futures.append(executor.submit(asset.materialize, context))

                # Wait for all assets in this generation to complete
                wait(futures)

                # Re-raise any exceptions that occurred
                for future in futures:
                    future.result()

    @Observable.event(step=ExecutionStep.PIPELINE_BACKFILL)
    def backfill(
        self,
        partitions: Iterator[Partition] | PartitionWindow,
    ) -> None:
        for asset in self._get_sequential_execution_order():
            # Single run per asset
            if isinstance(partitions, PartitionWindow) and asset.allows_partition_window:
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

    def on_event(self, event: Event) -> None:
        if isinstance(event.observable, Asset):
            self.execution_state[event.observable] = AssetExecutionState(
                asset=event.observable,
                step=event.step,
                status=event.status,
            )

            if event.status == ExecutionStatus.RUNNING:
                self.execution_state[event.observable].started_at = dt.datetime.now()
            elif event.status in (ExecutionStatus.SUCCESS, ExecutionStatus.FAILURE):
                self.execution_state[event.observable].completed_at = dt.datetime.now()

            if event.status == ExecutionStatus.FAILURE:
                self.execution_state[event.observable].error = event.error

        # User-defined callback
        if self.on_event_callback:
            self.on_event_callback(self, event)

    def group_assets_by_source(self) -> dict[str | None, list[Asset]]:
        assets_by_source: dict[str | None, list[Asset]] = {}
        for asset in self.assets.values():
            source_id = asset.source.name if asset.source else None
            if source_id not in assets_by_source:
                assets_by_source[source_id] = []
            assets_by_source[source_id].append(asset)
        return assets_by_source

    def group_assets_by_partitioning_config(self) -> dict[PartitionConfig | None, list[Asset]]: ...

    def get_running_assets(self) -> list[tuple[Asset, AssetExecutionState]]:
        return [(asset, state) for asset, state in self.execution_state.items() if state.is_running]

    def get_completed_assets(self) -> list[tuple[Asset, AssetExecutionState]]:
        return [(asset, state) for asset, state in self.execution_state.items() if state.is_completed]

    def get_failed_assets(self) -> list[tuple[Asset, AssetExecutionState]]:
        return [(asset, state) for asset, state in self.execution_state.items() if state.is_failed]

    def get_execution_summary(self) -> dict:
        return {
            "total": len(self._assets),
            "pending": len(self._assets)
            - len(self.get_running_assets())
            - len(self.get_completed_assets())
            - len(self.get_failed_assets()),
            "running": len(self.get_running_assets()),
            "completed": len(self.get_completed_assets()),
            "failed": len(self.get_failed_assets()),
        }

    def _add_assets(self, sources_or_assets: TAssetOrSource) -> None:
        # Convert single source/asset to a list
        if not isinstance(sources_or_assets, list):
            sources_or_assets = [sources_or_assets]

        # Unpack assets or source's assets
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
                asset.add_observer(self)
                self._assets[asset] = asset

    def _build_execution_graph(self) -> None:
        """Builds the asset dependency graph."""

        self._graph = nx.DiGraph()
        for asset in self._assets.values():
            self._graph.add_node(asset)

            for upstream_ref in asset.upstream_assets:
                # Verify that the upstream asset ref is in the asset's deps config
                if upstream_ref.name not in asset.deps.keys():
                    raise ValueError(f"Unable to resolve upstream asset '{upstream_ref.name}' of asset '{asset.name}'")

                upstream_asset_name = asset.deps[upstream_ref.name]

                # Check if the dependency exists in the asset map
                if upstream_asset_name not in self._assets:
                    raise ValueError(
                        f"Upstream asset '{upstream_asset_name}' of asset '{asset.name}' not found in asset graph"
                    )

                # Get the corresponding asset and add to the graph
                upstream_asset = self._assets[upstream_asset_name]
                self._graph.add_edge(upstream_asset, asset)

        # Detect cycles
        try:
            nx.find_cycle(self._graph, orientation="original")
            raise ValueError("Circular dependency detected in the asset dependency graph")
        except nx.NetworkXNoCycle:
            pass

        assert nx.is_directed_acyclic_graph(self._graph)

    def _get_sequential_execution_order(self) -> list[Asset]:
        """Returns the sequential execution order of assets."""

        order = list(nx.topological_sort(self._graph))
        logger.debug(f"Sequential execution order: {order}")
        return order

    def _get_parallel_execution_order(self) -> list[list[Asset]]:
        """Returns the parallel execution order of assets."""

        generations = list(nx.topological_generations(self._graph))
        logger.debug(f"Parallel execution order: {generations}")
        return generations
