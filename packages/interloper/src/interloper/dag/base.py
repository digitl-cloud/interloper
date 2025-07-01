"""This module contains the DAG and Node classes."""
import functools
from collections.abc import Sequence

from interloper.asset.base import Asset
from interloper.execution.strategy import ExecutionStategy
from interloper.partitioning.partition import Partition
from interloper.partitioning.window import PartitionWindow
from interloper.source.base import Source
from interloper.source.spec import SourceSpec

TAssetOrSource = Source | Asset | Sequence[Source | Asset]


class Node:
    """A node in the DAG."""

    def __init__(self, asset: Asset):
        """Initialize the node.

        Args:
            asset: The asset for the node.
        """
        self.asset = asset
        self.upstream: set[Node] = set()
        self.downstream: set[Node] = set()

    def __repr__(self):
        """Return a string representation of the node."""
        return f"Node({self.asset.id})"


# TODO: subgraphs should probably be immutable (DAGView?) since some upstream assets might not be present in the graph
# after split. The consequence is that subdags might not build again (_build_graph). Adding new assets (add_assets) to
# will then fail because it will build the graph and the upstream assets will then not be present.
class DAG:
    """A Directed Acyclic Graph of assets."""

    _nodes: dict[str, Node]

    def __init__(
        self,
        sources_or_assets: TAssetOrSource | None = None,
        allow_missing_dependencies: bool = False,
    ):
        """Initialize the DAG.

        Args:
            sources_or_assets: The sources or assets to add to the DAG.
            allow_missing_dependencies: Whether to allow missing dependencies.
        """
        self._nodes = {}
        self._allow_missing_dependencies = allow_missing_dependencies
        if sources_or_assets:
            self.add_assets(sources_or_assets)

    def add_assets(self, sources_or_assets: TAssetOrSource) -> None:
        """Add assets to the DAG.

        Args:
            sources_or_assets: The sources or assets to add.

        Raises:
            ValueError: If a duplicate asset is found or if the input is not a Source or Asset.
        """
        if not isinstance(sources_or_assets, Sequence):
            sources_or_assets = [sources_or_assets]

        for source_or_asset in sources_or_assets:
            batch: list[Asset] = []
            if isinstance(source_or_asset, Source):
                batch.extend([a for a in source_or_asset.assets if a.materializable])
            elif isinstance(source_or_asset, Asset):
                if source_or_asset.materializable:
                    batch.append(source_or_asset)
            else:
                raise ValueError(f"Expected Source or Asset, got {type(source_or_asset)}")

            for asset in batch:
                if asset.id in self._nodes:
                    raise ValueError(f"Duplicate asset '{asset.id}'")
                self._nodes[asset.id] = Node(asset)

        self._build_graph()
        self._clear_cache()

    def successors(self, asset: Asset) -> list[Asset]:
        """Get the successors of an asset.

        Args:
            asset: The asset to get the successors of.

        Returns:
            A list of the successors of the asset.
        """
        return [n.asset for n in self._nodes[asset.id].downstream]

    def predecessors(self, asset: Asset) -> list[Asset]:
        """Get the predecessors of an asset.

        Args:
            asset: The asset to get the predecessors of.

        Returns:
            A list of the predecessors of the asset.
        """
        return [n.asset for n in self._nodes[asset.id].upstream]

    def materialize(self, partition: Partition | None = None) -> None:
        """Materialize the DAG.

        Args:
            partition: The partition to materialize.
        """
        from interloper.execution.execution import MultiThreadExecution

        execution = MultiThreadExecution(dag=self, partitions=partition)
        execution()

    def backfill(self, partitions: Sequence[Partition] | PartitionWindow | None = None) -> None:
        """Backfill the DAG.

        Args:
            partitions: The partitions to backfill.
        """
        from interloper.execution.execution import MultiThreadExecution

        execution = MultiThreadExecution(dag=self, partitions=partitions)
        execution()

    def _build_graph(self) -> None:
        for node in self._nodes.values():
            asset = node.asset
            for upstream_ref in asset.upstream_assets:
                if upstream_ref.key not in asset.deps:
                    raise ValueError(f"Missing dep key '{upstream_ref.key}' in asset '{asset.name}'")
                upstream_asset = asset.deps[upstream_ref.key]

                if upstream_asset.id not in self._nodes:
                    if not self._allow_missing_dependencies:
                        raise ValueError(f"Upstream asset '{upstream_asset.id}' not found for '{asset.name}'")
                    continue

                upstream_node = self._nodes[upstream_asset.id]
                if not asset.is_partitioned and upstream_node.asset.is_partitioned:
                    raise ValueError(
                        f"Non-partitioned asset '{asset.name}' cannot depend on "
                        f"partitioned asset '{upstream_node.asset.name}'"
                    )

                node.upstream.add(upstream_node)
                upstream_node.downstream.add(node)

        self._detect_cycles()

    def _detect_cycles(self) -> None:
        visited = set()
        path = set()

        def visit(node: Node) -> None:
            if node in path:
                raise ValueError("Circular dependency detected in the asset graph")
            if node in visited:
                return
            path.add(node)
            for upstream in node.upstream:
                visit(upstream)
            path.remove(node)
            visited.add(node)

        for node in self._nodes.values():
            visit(node)

    def _clear_cache(self) -> None:
        DAG.assets.fget.cache_clear()  # type: ignore
        DAG.assets_by_sources.fget.cache_clear()  # type: ignore
        DAG.assets_by_execution_strategy.fget.cache_clear()  # type: ignore
        DAG.non_partitioned_subdag.fget.cache_clear()  # type: ignore
        DAG.partitioned_subdag.fget.cache_clear()  # type: ignore
        DAG.execution_strategy.fget.cache_clear()  # type: ignore
        DAG.supports_partitioning.fget.cache_clear()  # type: ignore
        DAG.supports_partitioning_window.fget.cache_clear()  # type: ignore
        self.split.cache_clear()

    @property
    def is_empty(self) -> bool:
        """Whether the DAG is empty."""
        return not self._nodes

    @property
    @functools.cache
    def assets(self) -> dict[str, Asset]:
        """The assets in the DAG."""
        return {node.asset.id: node.asset for node in self._nodes.values()}

    @property
    @functools.cache
    def assets_by_sources(self) -> dict[str | None, list[Asset]]:
        """The assets in the DAG grouped by source."""
        result: dict[str | None, list[Asset]] = {}
        for asset in self.assets.values():
            key = asset.source.name if asset.source else None
            result.setdefault(key, []).append(asset)
        return result

    @property
    @functools.cache
    def assets_by_execution_strategy(self) -> dict[ExecutionStategy, list[Asset]]:
        """The assets in the DAG grouped by execution strategy."""
        result = {
            ExecutionStategy.NOT_PARTITIONED: [],
            ExecutionStategy.PARTITIONED_MULTI_RUNS: [],
            ExecutionStategy.PARTITIONED_SINGLE_RUN: [],
        }
        for asset in self.assets.values():
            if not asset.partitioning:
                result[ExecutionStategy.NOT_PARTITIONED].append(asset)
            elif asset.partitioning.allow_window:
                result[ExecutionStategy.PARTITIONED_SINGLE_RUN].append(asset)
            else:
                result[ExecutionStategy.PARTITIONED_MULTI_RUNS].append(asset)
        return result

    @property
    @functools.cache
    def non_partitioned_subdag(self) -> "DAG":
        """The sub-DAG of non-partitioned assets."""
        assets = [a for a in self.assets.values() if not a.is_partitioned]
        return DAG(assets)

    @property
    @functools.cache
    def partitioned_subdag(self) -> "DAG":
        """The sub-DAG of partitioned assets."""
        assets = [a for a in self.assets.values() if a.is_partitioned]
        return DAG(assets, allow_missing_dependencies=True)

    @property
    @functools.cache
    def execution_strategy(self) -> ExecutionStategy:
        """The execution strategy of the DAG."""
        strat = self.assets_by_execution_strategy
        if len(strat[ExecutionStategy.PARTITIONED_SINGLE_RUN]) == len(self.assets):
            return ExecutionStategy.PARTITIONED_SINGLE_RUN
        elif len(strat[ExecutionStategy.PARTITIONED_MULTI_RUNS]) == len(self.assets):
            return ExecutionStategy.PARTITIONED_MULTI_RUNS
        elif len(strat[ExecutionStategy.NOT_PARTITIONED]) == len(self.assets):
            return ExecutionStategy.NOT_PARTITIONED
        elif (
            len(strat[ExecutionStategy.PARTITIONED_SINGLE_RUN]) + len(strat[ExecutionStategy.PARTITIONED_MULTI_RUNS])
        ) == len(self.assets):
            return ExecutionStategy.PARTITIONED_MULTI_RUNS
        return ExecutionStategy.MIXED

    @property
    @functools.cache
    def supports_partitioning(self) -> bool:
        """Whether the DAG supports partitioning."""
        return all(asset.partitioning for asset in self.assets.values())

    @property
    @functools.cache
    def supports_partitioning_window(self) -> bool:
        """Whether the DAG supports partitioning windows."""
        return all(asset.partitioning and asset.partitioning.allow_window for asset in self.assets.values())

    @functools.cache
    def split(self) -> tuple["DAG", "DAG"]:
        """Split the DAG into a non-partitioned and a partitioned sub-DAG.

        Returns:
            A tuple containing the non-partitioned and partitioned sub-DAGs.
        """
        return self.non_partitioned_subdag, self.partitioned_subdag

    @classmethod
    def from_source_specs(cls, specs: list[SourceSpec]) -> "DAG":
        """Create a DAG from a list of source specifications.

        Args:
            specs: The source specifications.

        Returns:
            A new DAG.
        """
        sources = [spec.to_source() for spec in specs]
        return cls(sources, allow_missing_dependencies=True)


if __name__ == "__main__":
    import datetime as dt

    import interloper as itlp

    @itlp.source
    def source() -> tuple[itlp.Asset, ...]:
        """A source for testing.

        Returns:
            A tuple of assets.
        """

        @itlp.asset()
        def root() -> str:
            print("[NOT PARTITIONED] root")
            return "root"

        @itlp.asset()
        def left_1(
            root: str = itlp.UpstreamAsset("root"),
        ) -> str:
            print("[NOT PARTITIONED] left_1")
            return "left_1"

        @itlp.asset(partitioning=itlp.TimePartitionConfig("date", allow_window=True))
        def left_2(
            date: tuple[dt.date, dt.date] = itlp.DateWindow(),
            left_1: str = itlp.UpstreamAsset("left_1"),
        ) -> str:
            print(f"[{date[0].isoformat()}] left_2")
            return "left_2"

        @itlp.asset(partitioning=itlp.TimePartitionConfig("date", allow_window=True))
        def right_1(
            date: tuple[dt.date, dt.date] = itlp.DateWindow(),
            root: str = itlp.UpstreamAsset("root"),
        ) -> str:
            print(f"[{date[0].isoformat()}] right_1")
            return "right_1"

        @itlp.asset(partitioning=itlp.TimePartitionConfig("date", allow_window=True))
        def right_2(
            date: tuple[dt.date, dt.date] = itlp.DateWindow(),
            right_1: str = itlp.UpstreamAsset("right_1"),
        ) -> str:
            print(f"[{date[0].isoformat()}] right_2")
            import random

            if random.random() < 0.5:
                raise Exception("Failed")

            return "right_2"

        return (root, left_1, left_2, right_1, right_2)

    dag = DAG(source)
    # pp(dag.assets)
    # pp(dag.non_partitioned_subdag.assets)
    # pp(dag.partitioned_subdag.assets)

    # dag = DAG.from_source_specs(
    #     [
    #         SourceSpec(
    #             name="adup",
    #             path="interloper_assets:awin",
    #         )
    #     ]
    # )
