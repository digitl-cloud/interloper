import datetime as dt
import logging
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from enum import Enum
from typing import Any

import networkx as nx
from opentelemetry import trace

from interloper.asset import Asset
from interloper.execution.context import ExecutionContext
from interloper.execution.observable import Event, EventStatus, EventType, Observable, Observer
from interloper.partitioning.partition import Partition
from interloper.partitioning.window import PartitionWindow
from interloper.source import Source

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)
TAssetOrSource = Source | Asset | list[Source | Asset]


class AssetExecutionStategy(Enum):
    NOT_PARTITIONED = "not_partitioned"
    MULTI_RUNS = "multi_runs"
    SINGLE_RUN = "single_run"


@dataclass
class AssetExecutionState:
    asset: Asset
    type: EventType | None = None
    status: EventStatus | None = None
    started_at: dt.datetime | None = None
    completed_at: dt.datetime | None = None
    error: Exception | None = None

    @property
    def is_running(self) -> bool:
        return self.status == EventStatus.RUNNING

    @property
    def is_completed(self) -> bool:
        return self.status == EventStatus.SUCCESS

    @property
    def is_failed(self) -> bool:
        return self.status == EventStatus.FAILURE


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

        self._execution_state_lock = threading.Lock()
        self._execution_state: dict[Asset, AssetExecutionState] = {
            asset: AssetExecutionState(asset) for asset in self._assets.values()
        }

    @property
    def assets(self) -> dict[str, Asset]:
        return self._assets

    @property
    def graph(self) -> nx.DiGraph:
        return self._graph

    @property
    def execution_state(self) -> dict[Asset, AssetExecutionState]:
        return self._execution_state

    @property
    def running_assets(self) -> list[tuple[Asset, AssetExecutionState]]:
        with self._execution_state_lock:
            return [(asset, state) for asset, state in self._execution_state.items() if state.is_running]

    @property
    def completed_assets(self) -> list[tuple[Asset, AssetExecutionState]]:
        with self._execution_state_lock:
            return [(asset, state) for asset, state in self._execution_state.items() if state.is_completed]

    @property
    def failed_assets(self) -> list[tuple[Asset, AssetExecutionState]]:
        with self._execution_state_lock:
            return [(asset, state) for asset, state in self._execution_state.items() if state.is_failed]

    @Observable.event(EventType.PIPELINE_MATERIALIZATION)
    def materialize(
        self,
        partition: Partition | None = None,
    ) -> Any:
        attributes = {}
        if partition:
            attributes["partition"] = str(partition)

        with tracer.start_as_current_span("interloper.pipeline.materialize", attributes=attributes):
            for generation in nx.topological_generations(self._graph):
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

    @Observable.event(EventType.PIPELINE_BACKFILL)
    def backfill(
        self,
        partitions: Iterator[Partition] | PartitionWindow,
    ) -> None:
        with tracer.start_as_current_span("interloper.pipeline.backfill"):
            assets_by_execution_strategy = self.group_assets_by_execution_strategy()

            # No partitioned assets
            if (
                len(assets_by_execution_strategy[AssetExecutionStategy.SINGLE_RUN]) == 0
                and len(assets_by_execution_strategy[AssetExecutionStategy.MULTI_RUNS]) == 0
            ):
                logger.warning(
                    "No partitioned assets to backfill, only non-partitioned assets: running simple materialization."
                )
                self.materialize()
                return

            # SINGLE RUN EXECUTION
            # Partition Window + (Non-partitioned assets & Single run partitioned assets)
            elif (
                isinstance(partitions, PartitionWindow)
                and len(assets_by_execution_strategy[AssetExecutionStategy.MULTI_RUNS]) == 0
            ):
                for generation in nx.topological_generations(self._graph):
                    for asset in generation:
                        context = ExecutionContext(
                            assets=self._assets,
                            executed_asset=asset,
                            partition=partitions,
                        )
                        asset.materialize(context)

            # MULTI RUN EXECUTION:
            # Mix of partitioned and non-partitioned assets
            else:
                with ThreadPoolExecutor() as executor:
                    # Non-partitioned assets
                    non_partitioned_assets = [asset for asset in self._assets.values() if not asset.partitioning]
                    non_partitioned_subgraph = nx.subgraph(self._graph, non_partitioned_assets)
                    for generation in nx.topological_generations(non_partitioned_subgraph):
                        futures = []
                        for asset in generation:
                            context = ExecutionContext(assets=self._assets, executed_asset=asset)
                            futures.append(executor.submit(asset.materialize, context))
                        wait(futures)
                        for future in futures:
                            future.result()

                    # Partitioned assets
                    partitioned_assets = [asset for asset in self._assets.values() if asset.partitioning]
                    partitioned_subgraph = nx.subgraph(self._graph, partitioned_assets)
                    for partition in partitions:
                        for generation in nx.topological_generations(partitioned_subgraph):
                            futures = []
                            for asset in generation:
                                context = ExecutionContext(
                                    assets=self._assets, executed_asset=asset, partition=partition
                                )
                                futures.append(executor.submit(asset.materialize, context))
                            wait(futures)
                            for future in futures:
                                future.result()

    def on_event(self, event: Event) -> None:
        if isinstance(event.observable, Asset):
            with self._execution_state_lock:
                self._execution_state[event.observable] = AssetExecutionState(
                    asset=event.observable,
                    type=event.type,
                    status=event.status,
                )

                if event.status == EventStatus.RUNNING:
                    self._execution_state[event.observable].started_at = dt.datetime.now()
                elif event.status in (EventStatus.SUCCESS, EventStatus.FAILURE):
                    self._execution_state[event.observable].completed_at = dt.datetime.now()

                if event.status == EventStatus.FAILURE:
                    self._execution_state[event.observable].error = event.error

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

    def group_assets_by_execution_strategy(self) -> dict[AssetExecutionStategy, list[Asset]]:
        assets_by_execution_strategy: dict[AssetExecutionStategy, list[Asset]] = {
            AssetExecutionStategy.NOT_PARTITIONED: [],
            AssetExecutionStategy.MULTI_RUNS: [],
            AssetExecutionStategy.SINGLE_RUN: [],
        }
        for asset in self.assets.values():
            if not asset.partitioning:
                assets_by_execution_strategy[AssetExecutionStategy.NOT_PARTITIONED].append(asset)
            elif asset.partitioning.allow_window:
                assets_by_execution_strategy[AssetExecutionStategy.SINGLE_RUN].append(asset)
            else:
                assets_by_execution_strategy[AssetExecutionStategy.MULTI_RUNS].append(asset)
        return assets_by_execution_strategy

    def _add_assets(self, sources_or_assets: TAssetOrSource) -> None:
        if not sources_or_assets:
            raise ValueError("No assets or sources provided to the pipeline")

        # Convert single source/asset to a list
        if not isinstance(sources_or_assets, list):
            sources_or_assets = [sources_or_assets]

        # Unpack assets or source's assets
        for source_or_asset in sources_or_assets:
            batch: list[Asset] = []
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
                self._assets[asset.id] = asset

    def _build_execution_graph(self) -> None:
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
                upstream_asset = self._assets[upstream_asset_id]
                self._graph.add_edge(upstream_asset, asset)

        # Detect cycles
        try:
            nx.find_cycle(self._graph, orientation="original")
            raise ValueError("Circular dependency detected in the asset dependency graph")
        except nx.NetworkXNoCycle:
            pass

        assert nx.is_directed_acyclic_graph(self._graph)
