"""This module contains tests for the DAG class."""
from unittest.mock import Mock, patch

import pytest

import interloper as itlp
from interloper.dag.base import DAG, Node
from interloper.execution.strategy import ExecutionStategy
from interloper.partitioning.config import TimePartitionConfig


class TestNode:
    """Test the Node class."""

    def test_node_creation(self):
        """Test that a node can be created."""
        # Setup
        asset = Mock(spec=itlp.Asset)
        asset.id = "test_asset"
        node = Node(asset)

        # Assertions
        assert node.asset == asset
        assert node.upstream == set()
        assert node.downstream == set()

    def test_node_repr(self):
        """Test the node representation."""
        # Setup
        asset = Mock(spec=itlp.Asset)
        asset.id = "test_asset"
        node = Node(asset)

        # Assertions
        assert repr(node) == "Node(test_asset)"


class TestDAG:
    """Test the DAG class."""

    def test_dag_creation_empty(self):
        """Test that an empty DAG can be created."""
        # Setup
        dag = DAG()

        # Assertions
        assert dag._nodes == {}
        assert dag._allow_missing_dependencies is False
        assert dag.is_empty is True

    def test_dag_creation_with_allow_missing_dependencies(self):
        """Test that a DAG can be created with allow_missing_dependencies."""
        # Setup
        dag = DAG(allow_missing_dependencies=True)

        # Assertions
        assert dag._allow_missing_dependencies is True

    def test_add_single_asset(self):
        """Test that a single asset can be added to the DAG."""
        # Setup
        asset = Mock(spec=itlp.Asset)
        asset.id = "test_asset"
        asset.materializable = True
        asset.upstream_assets = []
        asset.deps = {}
        asset.name = "test_asset"
        asset.is_partitioned = False

        # Execution
        dag = DAG()
        dag.add_assets(asset)

        # Assertions
        assert len(dag._nodes) == 1
        assert "test_asset" in dag._nodes
        assert dag._nodes["test_asset"].asset == asset

    def test_add_multiple_assets(self):
        """Test that multiple assets can be added to the DAG."""
        # Setup
        asset1 = Mock(spec=itlp.Asset)
        asset1.id = "asset1"
        asset1.materializable = True
        asset1.upstream_assets = []
        asset1.deps = {}
        asset1.name = "asset1"
        asset1.is_partitioned = False

        asset2 = Mock(spec=itlp.Asset)
        asset2.id = "asset2"
        asset2.materializable = True
        asset2.upstream_assets = []
        asset2.deps = {}
        asset2.name = "asset2"
        asset2.is_partitioned = False

        # Execution
        dag = DAG()
        dag.add_assets([asset1, asset2])

        # Assertions
        assert len(dag._nodes) == 2
        assert "asset1" in dag._nodes
        assert "asset2" in dag._nodes

    def test_add_source(self):
        """Test that a source can be added to the DAG."""
        # Setup
        asset = Mock(spec=itlp.Asset)
        asset.id = "test_asset"
        asset.materializable = True
        asset.upstream_assets = []
        asset.deps = {}
        asset.name = "test_asset"
        asset.is_partitioned = False

        source = Mock(spec=itlp.Source)
        source.assets = [asset]

        # Execution
        dag = DAG()
        dag.add_assets(source)

        # Assertions
        assert len(dag._nodes) == 1
        assert "test_asset" in dag._nodes

    def test_add_source_with_non_materializable_assets(self):
        """Test that non-materializable assets are not added to the DAG."""
        # Setup
        asset1 = Mock(spec=itlp.Asset)
        asset1.materializable = False

        asset2 = Mock(spec=itlp.Asset)
        asset2.id = "test_asset"
        asset2.materializable = True
        asset2.upstream_assets = []
        asset2.deps = {}
        asset2.name = "test_asset"
        asset2.is_partitioned = False

        source = Mock(spec=itlp.Source)
        source.assets = [asset1, asset2]

        # Execution
        dag = DAG()
        dag.add_assets(source)

        # Assertions
        assert len(dag._nodes) == 1
        assert "test_asset" in dag._nodes

    def test_add_duplicate_asset_raises_error(self):
        """Test that adding a duplicate asset raises an error."""
        # Setup
        asset = Mock(spec=itlp.Asset)
        asset.id = "test_asset"
        asset.materializable = True
        asset.upstream_assets = []
        asset.deps = {}
        asset.name = "test_asset"
        asset.is_partitioned = False

        # Execution & Assertions
        dag = DAG()
        dag.add_assets(asset)

        with pytest.raises(ValueError, match="Duplicate asset 'test_asset'"):
            dag.add_assets(asset)

    def test_add_invalid_type_raises_error(self):
        """Test that adding an invalid type raises an error."""
        # Setup & Execution & Assertions
        dag = DAG()
        with pytest.raises(ValueError, match="Expected Source or Asset, got <class 'str'>"):
            dag.add_assets("not_an_asset")

    def test_build_graph_with_dependencies(self):
        """Test that the graph is built correctly with dependencies."""
        # Setup
        upstream_asset = Mock(spec=itlp.Asset)
        upstream_asset.id = "upstream"
        upstream_asset.materializable = True
        upstream_asset.upstream_assets = []
        upstream_asset.deps = {}
        upstream_asset.name = "upstream"
        upstream_asset.is_partitioned = False

        downstream_asset = Mock(spec=itlp.Asset)
        downstream_asset.id = "downstream"
        downstream_asset.materializable = True
        downstream_asset.upstream_assets = [Mock(key="upstream_key")]
        downstream_asset.deps = {"upstream_key": upstream_asset}
        downstream_asset.name = "downstream"
        downstream_asset.is_partitioned = False

        # Execution
        dag = DAG([upstream_asset, downstream_asset])

        # Assertions
        assert len(dag._nodes) == 2
        upstream_node = dag._nodes["upstream"]
        downstream_node = dag._nodes["downstream"]

        assert upstream_node in downstream_node.upstream
        assert downstream_node in upstream_node.downstream

    def test_build_graph_missing_dependency_raises_error(self):
        """Test that a missing dependency raises an error."""
        # Setup
        asset = Mock(spec=itlp.Asset)
        asset.id = "test_asset"
        asset.materializable = True
        asset.upstream_assets = [Mock(key="missing_key")]
        asset.deps = {"missing_key": Mock(spec=itlp.Asset, id="missing_asset")}
        asset.name = "test_asset"
        asset.is_partitioned = False

        # Execution & Assertions
        with pytest.raises(ValueError, match="Upstream asset 'missing_asset' not found for 'test_asset'"):
            DAG([asset])

    def test_build_graph_missing_dependency_allowed(self):
        """Test that a missing dependency is allowed when specified."""
        # Setup
        asset = Mock(spec=itlp.Asset)
        asset.id = "test_asset"
        asset.materializable = True
        asset.upstream_assets = [Mock(key="missing_key")]
        asset.deps = {"missing_key": Mock(spec=itlp.Asset)}
        asset.name = "test_asset"
        asset.is_partitioned = False

        # Execution
        dag = DAG([asset], allow_missing_dependencies=True)

        # Assertions
        assert len(dag._nodes) == 1
        assert "test_asset" in dag._nodes

    def test_build_graph_missing_dep_key_raises_error(self):
        """Test that a missing dependency key raises an error."""
        # Setup
        asset = Mock(spec=itlp.Asset)
        asset.id = "test_asset"
        asset.materializable = True
        asset.upstream_assets = [Mock(key="upstream_key")]
        asset.deps = {}  # Missing the key
        asset.name = "test_asset"
        asset.is_partitioned = False

        # Execution & Assertions
        with pytest.raises(ValueError, match="Missing dep key 'upstream_key' in asset 'test_asset'"):
            DAG([asset])

    def test_build_graph_partitioning_constraint_violation(self):
        """Test that a partitioning constraint violation raises an error."""
        # Setup
        partitioned_asset = Mock(spec=itlp.Asset)
        partitioned_asset.id = "partitioned"
        partitioned_asset.materializable = True
        partitioned_asset.upstream_assets = []
        partitioned_asset.deps = {}
        partitioned_asset.name = "partitioned"
        partitioned_asset.is_partitioned = True

        non_partitioned_asset = Mock(spec=itlp.Asset)
        non_partitioned_asset.id = "non_partitioned"
        non_partitioned_asset.materializable = True
        non_partitioned_asset.upstream_assets = [Mock(key="partitioned_key")]
        non_partitioned_asset.deps = {"partitioned_key": partitioned_asset}
        non_partitioned_asset.name = "non_partitioned"
        non_partitioned_asset.is_partitioned = False

        # Execution & Assertions
        with pytest.raises(
            ValueError, match="Non-partitioned asset 'non_partitioned' cannot depend on partitioned asset 'partitioned'"
        ):
            DAG([partitioned_asset, non_partitioned_asset])

    def test_detect_cycles(self):
        """Test that cycles are detected in the graph."""
        # Setup
        asset1 = Mock(spec=itlp.Asset)
        asset1.id = "asset1"
        asset1.materializable = True
        asset1.upstream_assets = [Mock(key="asset2_key")]
        asset1.name = "asset1"
        asset1.is_partitioned = False

        asset2 = Mock(spec=itlp.Asset)
        asset2.id = "asset2"
        asset2.materializable = True
        asset2.upstream_assets = [Mock(key="asset1_key")]
        asset2.name = "asset2"
        asset2.is_partitioned = False

        asset1.deps = {"asset2_key": asset2}
        asset2.deps = {"asset1_key": asset1}

        # Execution & Assertions
        with pytest.raises(ValueError, match="Circular dependency detected in the asset graph"):
            DAG([asset1, asset2])

    def test_successors(self):
        """Test that the successors of a node can be retrieved."""
        # Setup
        upstream_asset = Mock(spec=itlp.Asset)
        upstream_asset.id = "upstream"
        upstream_asset.materializable = True
        upstream_asset.upstream_assets = []
        upstream_asset.deps = {}
        upstream_asset.name = "upstream"
        upstream_asset.is_partitioned = False

        downstream_asset = Mock(spec=itlp.Asset)
        downstream_asset.id = "downstream"
        downstream_asset.materializable = True
        downstream_asset.upstream_assets = [Mock(key="upstream_key")]
        downstream_asset.deps = {"upstream_key": upstream_asset}
        downstream_asset.name = "downstream"
        downstream_asset.is_partitioned = False

        dag = DAG([upstream_asset, downstream_asset])

        # Execution & Assertions
        successors = dag.successors(upstream_asset)
        assert len(successors) == 1
        assert successors[0] == downstream_asset

    def test_predecessors(self):
        """Test that the predecessors of a node can be retrieved."""
        # Setup
        upstream_asset = Mock(spec=itlp.Asset)
        upstream_asset.id = "upstream"
        upstream_asset.materializable = True
        upstream_asset.upstream_assets = []
        upstream_asset.deps = {}
        upstream_asset.name = "upstream"
        upstream_asset.is_partitioned = False

        downstream_asset = Mock(spec=itlp.Asset)
        downstream_asset.id = "downstream"
        downstream_asset.materializable = True
        downstream_asset.upstream_assets = [Mock(key="upstream_key")]
        downstream_asset.deps = {"upstream_key": upstream_asset}
        downstream_asset.name = "downstream"
        downstream_asset.is_partitioned = False

        dag = DAG([upstream_asset, downstream_asset])

        # Execution & Assertions
        predecessors = dag.predecessors(downstream_asset)
        assert len(predecessors) == 1
        assert predecessors[0] == upstream_asset

    def test_assets_property(self):
        """Test the assets property."""
        # Setup
        asset1 = Mock(spec=itlp.Asset)
        asset1.id = "asset1"
        asset1.materializable = True
        asset1.upstream_assets = []
        asset1.deps = {}
        asset1.name = "asset1"
        asset1.is_partitioned = False

        asset2 = Mock(spec=itlp.Asset)
        asset2.id = "asset2"
        asset2.materializable = True
        asset2.upstream_assets = []
        asset2.deps = {}
        asset2.name = "asset2"
        asset2.is_partitioned = False

        dag = DAG([asset1, asset2])

        # Execution & Assertions
        assets = dag.assets
        assert len(assets) == 2
        assert assets["asset1"] == asset1
        assert assets["asset2"] == asset2

    def test_assets_by_sources_property(self):
        """Test the assets_by_sources property."""
        # Setup
        source1 = Mock(spec=itlp.Source)
        source1.name = "source1"

        source2 = Mock(spec=itlp.Source)
        source2.name = "source2"

        asset1 = Mock(spec=itlp.Asset)
        asset1.id = "asset1"
        asset1.materializable = True
        asset1.upstream_assets = []
        asset1.deps = {}
        asset1.name = "asset1"
        asset1.is_partitioned = False
        asset1.source = source1

        asset2 = Mock(spec=itlp.Asset)
        asset2.id = "asset2"
        asset2.materializable = True
        asset2.upstream_assets = []
        asset2.deps = {}
        asset2.name = "asset2"
        asset2.is_partitioned = False
        asset2.source = source2

        asset3 = Mock(spec=itlp.Asset)
        asset3.id = "asset3"
        asset3.materializable = True
        asset3.upstream_assets = []
        asset3.deps = {}
        asset3.name = "asset3"
        asset3.is_partitioned = False
        asset3.source = None

        dag = DAG([asset1, asset2, asset3])

        # Execution & Assertions
        assert len(dag.assets_by_sources) == 3
        assert dag.assets_by_sources["source1"] == [asset1]
        assert dag.assets_by_sources["source2"] == [asset2]
        assert dag.assets_by_sources[None] == [asset3]

    def test_assets_by_execution_strategy_not_partitioned(self):
        """Test the assets_by_execution_strategy property with non-partitioned assets."""
        # Setup
        asset = Mock(spec=itlp.Asset)
        asset.id = "asset"
        asset.materializable = True
        asset.upstream_assets = []
        asset.deps = {}
        asset.name = "asset"
        asset.is_partitioned = False
        asset.partitioning = None

        dag = DAG([asset])

        # Execution & Assertions
        assert len(dag.assets_by_execution_strategy[ExecutionStategy.NOT_PARTITIONED]) == 1
        assert len(dag.assets_by_execution_strategy[ExecutionStategy.PARTITIONED_MULTI_RUNS]) == 0
        assert len(dag.assets_by_execution_strategy[ExecutionStategy.PARTITIONED_SINGLE_RUN]) == 0

    def test_assets_by_execution_strategy_partitioned_single_run(self):
        """Test the assets_by_execution_strategy property with single-run partitioned assets."""
        # Setup
        partitioning = TimePartitionConfig("date", allow_window=True)
        asset = Mock(spec=itlp.Asset)
        asset.id = "asset"
        asset.materializable = True
        asset.upstream_assets = []
        asset.deps = {}
        asset.name = "asset"
        asset.is_partitioned = True
        asset.partitioning = partitioning

        dag = DAG([asset])

        # Execution & Assertions
        assert len(dag.assets_by_execution_strategy[ExecutionStategy.NOT_PARTITIONED]) == 0
        assert len(dag.assets_by_execution_strategy[ExecutionStategy.PARTITIONED_MULTI_RUNS]) == 0
        assert len(dag.assets_by_execution_strategy[ExecutionStategy.PARTITIONED_SINGLE_RUN]) == 1

    def test_assets_by_execution_strategy_partitioned_multi_runs(self):
        """Test the assets_by_execution_strategy property with multi-run partitioned assets."""
        # Setup
        partitioning = TimePartitionConfig("date", allow_window=False)
        asset = Mock(spec=itlp.Asset)
        asset.id = "asset"
        asset.materializable = True
        asset.upstream_assets = []
        asset.deps = {}
        asset.name = "asset"
        asset.is_partitioned = True
        asset.partitioning = partitioning

        dag = DAG([asset])

        # Execution & Assertions
        assert len(dag.assets_by_execution_strategy[ExecutionStategy.NOT_PARTITIONED]) == 0
        assert len(dag.assets_by_execution_strategy[ExecutionStategy.PARTITIONED_MULTI_RUNS]) == 1
        assert len(dag.assets_by_execution_strategy[ExecutionStategy.PARTITIONED_SINGLE_RUN]) == 0

    def test_non_partitioned_subdag(self):
        """Test the non_partitioned_subdag property."""
        # Setup
        non_partitioned_asset = Mock(spec=itlp.Asset)
        non_partitioned_asset.id = "non_partitioned"
        non_partitioned_asset.materializable = True
        non_partitioned_asset.upstream_assets = []
        non_partitioned_asset.deps = {}
        non_partitioned_asset.name = "non_partitioned"
        non_partitioned_asset.is_partitioned = False

        partitioned_asset = Mock(spec=itlp.Asset)
        partitioned_asset.id = "partitioned"
        partitioned_asset.materializable = True
        partitioned_asset.upstream_assets = []
        partitioned_asset.deps = {}
        partitioned_asset.name = "partitioned"
        partitioned_asset.is_partitioned = True

        dag = DAG([non_partitioned_asset, partitioned_asset])

        # Execution & Assertions
        assert len(dag.non_partitioned_subdag.assets) == 1
        assert "non_partitioned" in dag.non_partitioned_subdag.assets

    def test_partitioned_subdag(self):
        """Test the partitioned_subdag property."""
        # Setup
        non_partitioned_asset = Mock(spec=itlp.Asset)
        non_partitioned_asset.id = "non_partitioned"
        non_partitioned_asset.materializable = True
        non_partitioned_asset.upstream_assets = []
        non_partitioned_asset.deps = {}
        non_partitioned_asset.name = "non_partitioned"
        non_partitioned_asset.is_partitioned = False
        non_partitioned_asset.partitioning = None

        partitioned_asset = Mock(spec=itlp.Asset)
        partitioned_asset.id = "partitioned"
        partitioned_asset.materializable = True
        partitioned_asset.upstream_assets = []
        partitioned_asset.deps = {}
        partitioned_asset.name = "partitioned"
        partitioned_asset.is_partitioned = True
        partitioned_asset.partitioning = TimePartitionConfig("date", allow_window=True)

        dag = DAG([non_partitioned_asset, partitioned_asset])

        # Execution & Assertions
        assert len(dag.partitioned_subdag.assets) == 1
        assert "partitioned" in dag.partitioned_subdag.assets
        assert dag.partitioned_subdag._allow_missing_dependencies is True

    def test_execution_strategy_not_partitioned(self):
        """Test the execution_strategy property with a non-partitioned DAG."""
        # Setup
        asset = Mock(spec=itlp.Asset)
        asset.id = "asset"
        asset.materializable = True
        asset.upstream_assets = []
        asset.deps = {}
        asset.name = "asset"
        asset.is_partitioned = False
        asset.partitioning = None

        dag = DAG([asset])

        # Execution & Assertions
        assert dag.execution_strategy == ExecutionStategy.NOT_PARTITIONED

    def test_execution_strategy_partitioned_single_run(self):
        """Test the execution_strategy property with a single-run partitioned DAG."""
        # Setup
        partitioning = TimePartitionConfig("date", allow_window=True)
        asset = Mock(spec=itlp.Asset)
        asset.id = "asset"
        asset.materializable = True
        asset.upstream_assets = []
        asset.deps = {}
        asset.name = "asset"
        asset.is_partitioned = True
        asset.partitioning = partitioning

        dag = DAG([asset])

        # Execution & Assertions
        assert dag.execution_strategy == ExecutionStategy.PARTITIONED_SINGLE_RUN

    def test_execution_strategy_partitioned_multi_runs(self):
        """Test the execution_strategy property with a multi-run partitioned DAG."""
        # Setup
        partitioning = TimePartitionConfig("date", allow_window=False)
        asset = Mock(spec=itlp.Asset)
        asset.id = "asset"
        asset.materializable = True
        asset.upstream_assets = []
        asset.deps = {}
        asset.name = "asset"
        asset.is_partitioned = True
        asset.partitioning = partitioning

        dag = DAG([asset])

        # Execution & Assertions
        assert dag.execution_strategy == ExecutionStategy.PARTITIONED_MULTI_RUNS

    def test_execution_strategy_mixed(self):
        """Test the execution_strategy property with a mixed DAG."""
        # Setup
        non_partitioned_asset = Mock(spec=itlp.Asset)
        non_partitioned_asset.id = "non_partitioned"
        non_partitioned_asset.materializable = True
        non_partitioned_asset.upstream_assets = []
        non_partitioned_asset.deps = {}
        non_partitioned_asset.name = "non_partitioned"
        non_partitioned_asset.is_partitioned = False
        non_partitioned_asset.partitioning = None

        partitioned_asset = Mock(spec=itlp.Asset)
        partitioned_asset.id = "partitioned"
        partitioned_asset.materializable = True
        partitioned_asset.upstream_assets = []
        partitioned_asset.deps = {}
        partitioned_asset.name = "partitioned"
        partitioned_asset.is_partitioned = True
        partitioned_asset.partitioning = TimePartitionConfig("date", allow_window=True)

        dag = DAG([non_partitioned_asset, partitioned_asset])

        # Execution & Assertions
        assert dag.execution_strategy == ExecutionStategy.MIXED

    def test_supports_partitioning_true(self):
        """Test the supports_partitioning property when all assets are partitioned."""
        # Setup
        partitioning = TimePartitionConfig("date", allow_window=True)
        asset = Mock(spec=itlp.Asset)
        asset.id = "asset"
        asset.materializable = True
        asset.upstream_assets = []
        asset.deps = {}
        asset.name = "asset"
        asset.is_partitioned = True
        asset.partitioning = partitioning

        dag = DAG([asset])

        # Execution & Assertions
        assert dag.supports_partitioning is True

    def test_supports_partitioning_false(self):
        """Test the supports_partitioning property when not all assets are partitioned."""
        # Setup
        asset = Mock(spec=itlp.Asset)
        asset.id = "asset"
        asset.materializable = True
        asset.upstream_assets = []
        asset.deps = {}
        asset.name = "asset"
        asset.is_partitioned = False
        asset.partitioning = None

        dag = DAG([asset])

        # Execution & Assertions
        assert dag.supports_partitioning is False

    def test_supports_partitioning_window_true(self):
        """Test the supports_partitioning_window property when all assets support it."""
        # Setup
        partitioning = TimePartitionConfig("date", allow_window=True)
        asset = Mock(spec=itlp.Asset)
        asset.id = "asset"
        asset.materializable = True
        asset.upstream_assets = []
        asset.deps = {}
        asset.name = "asset"
        asset.is_partitioned = True
        asset.partitioning = partitioning

        dag = DAG([asset])

        # Execution & Assertions
        assert dag.supports_partitioning_window is True

    def test_supports_partitioning_window_false(self):
        """Test the supports_partitioning_window property when not all assets support it."""
        # Setup
        partitioning = TimePartitionConfig("date", allow_window=False)
        asset = Mock(spec=itlp.Asset)
        asset.id = "asset"
        asset.materializable = True
        asset.upstream_assets = []
        asset.deps = {}
        asset.name = "asset"
        asset.is_partitioned = True
        asset.partitioning = partitioning

        dag = DAG([asset])

        # Execution & Assertions
        assert dag.supports_partitioning_window is False

    def test_split(self):
        """Test the split method."""
        # Setup
        non_partitioned_asset = Mock(spec=itlp.Asset)
        non_partitioned_asset.id = "non_partitioned"
        non_partitioned_asset.materializable = True
        non_partitioned_asset.upstream_assets = []
        non_partitioned_asset.deps = {}
        non_partitioned_asset.name = "non_partitioned"
        non_partitioned_asset.is_partitioned = False

        partitioned_asset = Mock(spec=itlp.Asset)
        partitioned_asset.id = "partitioned"
        partitioned_asset.materializable = True
        partitioned_asset.upstream_assets = []
        partitioned_asset.deps = {}
        partitioned_asset.name = "partitioned"
        partitioned_asset.is_partitioned = True

        dag = DAG([non_partitioned_asset, partitioned_asset])

        # Execution & Assertions
        non_partitioned_subdag, partitioned_subdag = dag.split()
        assert len(non_partitioned_subdag.assets) == 1
        assert "non_partitioned" in non_partitioned_subdag.assets
        assert len(partitioned_subdag.assets) == 1
        assert "partitioned" in partitioned_subdag.assets

    @patch("interloper.execution.execution.MultiThreadExecution")
    def test_materialize(self, mock_execution_class):
        """Test the materialize method."""
        # Setup
        mock_execution = Mock()
        mock_execution_class.return_value = mock_execution

        asset = Mock(spec=itlp.Asset)
        asset.id = "asset"
        asset.materializable = True
        asset.upstream_assets = []
        asset.deps = {}
        asset.name = "asset"
        asset.is_partitioned = False

        dag = DAG([asset])

        # Execution
        dag.materialize()

        # Assertions
        mock_execution_class.assert_called_once_with(dag=dag, partitions=None)
        mock_execution.assert_called_once()

    @patch("interloper.execution.execution.MultiThreadExecution")
    def test_backfill(self, mock_execution_class):
        """Test the backfill method."""
        # Setup
        mock_execution = Mock()
        mock_execution_class.return_value = mock_execution

        asset = Mock(spec=itlp.Asset)
        asset.id = "asset"
        asset.materializable = True
        asset.upstream_assets = []
        asset.deps = {}
        asset.name = "asset"
        asset.is_partitioned = False

        dag = DAG([asset])

        # Execution
        dag.backfill()

        # Assertions
        mock_execution_class.assert_called_once_with(dag=dag, partitions=None)
        mock_execution.assert_called_once()

    def test_from_source_specs(self):
        """Test creating a DAG from source specs."""
        # Setup
        mock_source = Mock(spec=itlp.Source)
        mock_source.assets = []

        spec = Mock()
        spec.to_source.return_value = mock_source
        specs = [spec]

        # Execution
        dag = DAG.from_source_specs(specs)

        # Assertions
        spec.to_source.assert_called_once_with()
        assert dag._allow_missing_dependencies is True
