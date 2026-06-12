"""Tests for ``interloper.dag.base``."""

# Note: no ``from __future__ import annotations`` — the fixtures below define
# methods whose parameter annotations must be real classes (not lazy strings)
# so that sibling-dep inference in ``Source._infer_all_requires`` can resolve
# them by name.

from typing import Any, ClassVar

import pytest

import interloper as il
from interloper.component.base import ComponentSpec
from interloper.dag.base import DAG
from interloper.dag.spec import DAGSpec
from interloper.errors import (
    AssetNotFoundError,
    CircularDependencyError,
    DAGError,
    DependencyContractError,
    DependencyNotFoundError,
)
from interloper.partitioning.base import PartitionConfig

# ---------------------------------------------------------------------------
# Minimal asset/source fixtures used by small-topology tests
# ---------------------------------------------------------------------------


class FakeAsset(il.Asset):
    """Plain asset fixture."""


class FakeOtherAsset(il.Asset):
    """Second asset class used for subclass-identity and list tests."""


class FakeThirdAsset(il.Asset):
    """Third asset class used for multi-level topology tests."""


class FakePartitionedAsset(il.Asset):
    """Partitioned asset used for partition-dependency validation tests."""

    partitioning: ClassVar[PartitionConfig | None] = PartitionConfig(column="day")


class FakeAssetRequiringFake(il.Asset):
    """Asset whose ``requires`` contract expects an upstream keyed ``fake_asset``."""

    requires: ClassVar[dict[str, str]] = {"upstream": "fake_asset"}


class FakeAssetOptionallyRequiringFake(il.Asset):
    """Asset whose ``optional_requires`` contract expects ``fake_asset`` but may be missing."""

    optional_requires: ClassVar[dict[str, str]] = {"upstream": "fake_asset"}


class FakeSource(il.Source):
    """Small source with two nested assets used for source-flattening tests."""

    class FakeFirst(il.Asset):
        """First nested asset."""

    class FakeSecond(il.Asset):
        """Second nested asset that depends on the first via parameter name."""

        def data(self, fake_first: Any) -> Any:  # pragma: no cover
            return None


# ---------------------------------------------------------------------------
# Larger topology fixtures (mirroring the patterns from the previous framework)
#
# The source classes below must live at module level so that ``import_from_path``
# can resolve the ``"module.Source:asset"`` paths used during serialization
# round-trips.  Inline class definitions inside fixtures break that import
# because inner classes don't exist as module attributes.
# ---------------------------------------------------------------------------


_PART = PartitionConfig(column="date")


@il.source
class FakeComplexSource(il.Source):
    """Seven-asset source covering multiple parallelizable levels.

    Topology:
    - L0: a, b
    - L1: c(a), d(a)
    - L2: e(b, c), f(d)
    - L3: g(e, f)
    """

    @il.asset
    def a(self) -> list[dict]:  # pragma: no cover
        return []

    @il.asset
    def b(self) -> list[dict]:  # pragma: no cover
        return []

    @il.asset
    def c(self, a: list[dict]) -> list[dict]:  # pragma: no cover
        return []

    @il.asset
    def d(self, a: list[dict]) -> list[dict]:  # pragma: no cover
        return []

    @il.asset
    def e(self, b: list[dict], c: list[dict]) -> list[dict]:  # pragma: no cover
        return []

    @il.asset
    def f(self, d: list[dict]) -> list[dict]:  # pragma: no cover
        return []

    @il.asset
    def g(self, e: list[dict], f: list[dict]) -> list[dict]:  # pragma: no cover
        return []


@il.source
class FakePartitionedSource(il.Source):
    """Same topology as :class:`FakeComplexSource` but every asset is partitioned."""

    @il.asset(partitioning=_PART)
    def a(self) -> list[dict]:  # pragma: no cover
        return []

    @il.asset(partitioning=_PART)
    def b(self) -> list[dict]:  # pragma: no cover
        return []

    @il.asset(partitioning=_PART)
    def c(self, a: list[dict]) -> list[dict]:  # pragma: no cover
        return []

    @il.asset(partitioning=_PART)
    def d(self, a: list[dict]) -> list[dict]:  # pragma: no cover
        return []

    @il.asset(partitioning=_PART)
    def e(self, b: list[dict], c: list[dict]) -> list[dict]:  # pragma: no cover
        return []

    @il.asset(partitioning=_PART)
    def f(self, d: list[dict]) -> list[dict]:  # pragma: no cover
        return []

    @il.asset(partitioning=_PART)
    def g(self, e: list[dict], f: list[dict]) -> list[dict]:  # pragma: no cover
        return []


@il.source
class FakeMixedSource(il.Source):
    """Mix of non-partitioned roots and partitioned downstream assets.

    Only partitioned-depends-on-non-partitioned edges are present
    (the opposite direction is rejected by ``_check_partition_dependencies``).

    Topology:
    - L0: a, b                 (non-partitioned)
    - L1: c(a)                 (partitioned)
    - L2: e(b, c)              (partitioned)
    """

    @il.asset
    def a(self) -> list[dict]:  # pragma: no cover
        return []

    @il.asset
    def b(self) -> list[dict]:  # pragma: no cover
        return []

    @il.asset(partitioning=_PART)
    def c(self, a: list[dict]) -> list[dict]:  # pragma: no cover
        return []

    @il.asset(partitioning=_PART)
    def e(self, b: list[dict], c: list[dict]) -> list[dict]:  # pragma: no cover
        return []


@il.source
class FakeSourceOne(il.Source):
    """First source used in double-source DAG tests."""

    @il.asset(partitioning=_PART)
    def a(self) -> list[dict]:  # pragma: no cover
        return []

    @il.asset(partitioning=_PART)
    def b(self) -> list[dict]:  # pragma: no cover
        return []


@il.source
class FakeSourceTwo(il.Source):
    """Second source used in double-source DAG tests."""

    @il.asset(partitioning=_PART)
    def a(self) -> list[dict]:  # pragma: no cover
        return []

    @il.asset(partitioning=_PART)
    def b(self) -> list[dict]:  # pragma: no cover
        return []


@pytest.fixture
def dag() -> il.DAG:
    """A DAG with multiple parallelizable levels.

    Returns:
        A seven-asset DAG built from :class:`FakeComplexSource`.
    """
    return il.DAG(FakeComplexSource())


@pytest.fixture
def dag_partitioned() -> il.DAG:
    """A fully partitioned version of the seven-asset DAG.

    Returns:
        A seven-asset DAG built from :class:`FakePartitionedSource`.
    """
    return il.DAG(FakePartitionedSource())


@pytest.fixture
def dag_mixed() -> il.DAG:
    """A DAG mixing non-partitioned and partitioned assets.

    Returns:
        A four-asset DAG built from :class:`FakeMixedSource`.
    """
    return il.DAG(FakeMixedSource())


@pytest.fixture
def double_source_dag() -> il.DAG:
    """A DAG built from two sources, each exposing its own ``a`` and ``b`` assets.

    Returns:
        A four-asset DAG spanning two independent sources.
    """
    return il.DAG(FakeSourceOne(), FakeSourceTwo())


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_single_asset_instance(self):
        asset = FakeAsset()
        dag = DAG(asset)
        assert dag.assets == [asset]
        assert dag.asset_map == {asset.id: asset}

    def test_multiple_asset_instances(self):
        a = FakeAsset()
        b = FakeOtherAsset()
        dag = DAG(a, b)
        assert len(dag.assets) == 2
        assert set(dag.asset_map) == {a.id, b.id}

    def test_accepts_asset_class_and_instantiates_it(self):
        dag = DAG(FakeAsset)
        assert len(dag.assets) == 1
        assert isinstance(dag.assets[0], FakeAsset)

    def test_accepts_source_instance_and_flattens_its_assets(self):
        source = FakeSource()
        dag = DAG(source)
        assert len(dag.assets) == len(source.assets)
        assert {type(a).key for a in dag.assets} == {"fake_first", "fake_second"}

    def test_accepts_source_class_and_flattens_its_assets(self):
        dag = DAG(FakeSource)
        assert len(dag.assets) == 2

    def test_accepts_mixed_assets_and_sources(self):
        standalone = FakeAsset()
        source = FakeSource()
        dag = DAG(standalone, source)
        assert len(dag.assets) == 1 + len(source.assets)

    def test_complex_dag_has_all_assets(self, dag: il.DAG):
        assert len(dag.assets) == 7
        assert {type(asset).key for asset in dag.assets} == {"a", "b", "c", "d", "e", "f", "g"}

    def test_double_source_dag_flattens_both_sources(self, double_source_dag: il.DAG):
        assert len(double_source_dag.assets) == 4
        keys = [type(asset).key for asset in double_source_dag.assets]
        assert keys.count("a") == 2
        assert keys.count("b") == 2

    def test_empty_input_raises(self):
        with pytest.raises(DAGError):
            DAG()

    def test_invalid_item_type_raises(self):
        with pytest.raises(DAGError):
            DAG("not an asset")  # ty: ignore[invalid-argument-type]

    def test_duplicate_ids_raise(self):
        a = FakeAsset(id="same")
        b = FakeOtherAsset(id="same")
        with pytest.raises(DAGError):
            DAG(a, b)


# ---------------------------------------------------------------------------
# Graph structure
# ---------------------------------------------------------------------------


class TestGraph:
    def test_asset_map_keyed_by_id(self):
        a = FakeAsset()
        b = FakeOtherAsset()
        dag = DAG(a, b)
        assert dag.asset_map[a.id] is a
        assert dag.asset_map[b.id] is b

    def test_successors_initialized_empty_for_all_assets(self):
        a = FakeAsset()
        b = FakeOtherAsset()
        dag = DAG(a, b)
        assert dag.successors[a.id] == []
        assert dag.successors[b.id] == []

    def test_predecessors_wired_from_deps(self):
        upstream = FakeAsset()
        downstream = FakeOtherAsset(deps={"upstream": upstream.id})
        dag = DAG(upstream, downstream)
        assert dag.predecessors[downstream.id] == [upstream.id]
        assert dag.successors[upstream.id] == [downstream.id]

    def test_complex_dag_predecessors_match_topology(self, dag: il.DAG):
        by_key = {type(asset).key: asset for asset in dag.assets}

        # L0 roots
        assert dag.predecessors[by_key["a"].id] == []
        assert dag.predecessors[by_key["b"].id] == []

        # L1
        assert dag.predecessors[by_key["c"].id] == [by_key["a"].id]
        assert dag.predecessors[by_key["d"].id] == [by_key["a"].id]

        # L2
        assert set(dag.predecessors[by_key["e"].id]) == {by_key["b"].id, by_key["c"].id}
        assert dag.predecessors[by_key["f"].id] == [by_key["d"].id]

        # L3
        assert set(dag.predecessors[by_key["g"].id]) == {by_key["e"].id, by_key["f"].id}

    def test_complex_dag_successors_match_topology(self, dag: il.DAG):
        by_key = {type(asset).key: asset for asset in dag.assets}

        assert set(dag.successors[by_key["a"].id]) == {by_key["c"].id, by_key["d"].id}
        assert dag.successors[by_key["b"].id] == [by_key["e"].id]
        assert dag.successors[by_key["c"].id] == [by_key["e"].id]
        assert dag.successors[by_key["d"].id] == [by_key["f"].id]
        assert dag.successors[by_key["e"].id] == [by_key["g"].id]
        assert dag.successors[by_key["f"].id] == [by_key["g"].id]
        assert dag.successors[by_key["g"].id] == []

    def test_non_materializable_asset_skipped_from_predecessors(self):
        # Non-materializable assets are parents, not roots — their predecessors
        # are not computed (they don't need to execute).
        upstream = FakeAsset(materializable=False)
        downstream = FakeOtherAsset(deps={"upstream": upstream.id})
        dag = DAG(upstream, downstream)
        assert upstream.id not in dag.predecessors
        assert dag.predecessors[downstream.id] == [upstream.id]

    def test_missing_required_dep_raises(self):
        downstream = FakeAsset(deps={"upstream": "nonexistent-id"})
        with pytest.raises(DependencyNotFoundError):
            DAG(downstream)

    def test_missing_optional_dep_is_tolerated(self):
        asset = FakeAssetOptionallyRequiringFake(deps={"upstream": "nonexistent-id"})
        dag = DAG(asset)
        assert asset.id in dag.asset_map
        assert dag.predecessors[asset.id] == []


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_valid_requires_contract_passes(self):
        upstream = FakeAsset()
        downstream = FakeAssetRequiringFake(deps={"upstream": upstream.id})
        DAG(upstream, downstream)  # no raise

    def test_requires_contract_mismatch_raises(self):
        upstream = FakeOtherAsset()
        downstream = FakeAssetRequiringFake(deps={"upstream": upstream.id})
        with pytest.raises(DependencyContractError):
            DAG(upstream, downstream)

    def test_circular_dependency_raises(self):
        a = FakeAsset(id="aaaaaaaa")
        b = FakeOtherAsset(id="bbbbbbbb")
        a.deps = {"b": b.id}
        b.deps = {"a": a.id}
        with pytest.raises(CircularDependencyError):
            DAG(a, b)

    def test_non_partitioned_depending_on_partitioned_raises(self):
        upstream = FakePartitionedAsset()
        downstream = FakeAsset(deps={"upstream": upstream.id})
        with pytest.raises(DAGError):
            DAG(upstream, downstream)

    def test_partitioned_depending_on_non_partitioned_is_allowed(self, dag_mixed: il.DAG):
        # ``dag_mixed`` fixture already built the DAG without error; presence
        # of all four assets proves construction + validation passed.
        assert len(dag_mixed.assets) == 4


# ---------------------------------------------------------------------------
# Traversal
# ---------------------------------------------------------------------------


class TestTraversal:
    def test_topological_generations_single_asset(self):
        asset = FakeAsset()
        dag = DAG(asset)
        levels = dag.topological_generations()
        assert levels == [[asset]]

    def test_topological_generations_linear_chain(self):
        a = FakeAsset()
        b = FakeOtherAsset(deps={"a": a.id})
        c = FakeThirdAsset(deps={"b": b.id})
        dag = DAG(a, b, c)
        levels = dag.topological_generations()
        assert len(levels) == 3
        assert levels[0] == [a]
        assert levels[1] == [b]
        assert levels[2] == [c]

    def test_topological_generations_parallel_branches(self):
        a = FakeAsset()
        b = FakeOtherAsset(deps={"a": a.id})
        c = FakeThirdAsset(deps={"a": a.id})
        dag = DAG(a, b, c)
        levels = dag.topological_generations()
        assert levels[0] == [a]
        assert {asset.id for asset in levels[1]} == {b.id, c.id}

    def test_topological_generations_complex_dag(self, dag: il.DAG):
        levels = dag.topological_generations()
        keys_by_level = [{type(asset).key for asset in level} for level in levels]
        assert keys_by_level == [
            {"a", "b"},
            {"c", "d"},
            {"e", "f"},
            {"g"},
        ]

    def test_topological_generations_partitioned_dag(self, dag_partitioned: il.DAG):
        # Same topology as the non-partitioned version.
        levels = dag_partitioned.topological_generations()
        keys_by_level = [{type(asset).key for asset in level} for level in levels]
        assert keys_by_level == [
            {"a", "b"},
            {"c", "d"},
            {"e", "f"},
            {"g"},
        ]

    def test_topological_generations_mixed_dag(self, dag_mixed: il.DAG):
        levels = dag_mixed.topological_generations()
        keys_by_level = [{type(asset).key for asset in level} for level in levels]
        assert keys_by_level == [{"a", "b"}, {"c"}, {"e"}]

    def test_topological_generations_double_source_dag(self, double_source_dag: il.DAG):
        # All four assets are leaves (no inter-source deps) → one level.
        levels = double_source_dag.topological_generations()
        assert len(levels) == 1
        assert len(levels[0]) == 4

    def test_topological_generations_skips_non_materializable_parent(self):
        # Mini-DAG shape: a skipped parent upstream of a live target. The
        # parent's edge counts as satisfied, and only the target appears.
        a = FakeAsset()
        b = FakeOtherAsset(deps={"a": a.id})
        dag = DAG(a(materializable=False), b)
        levels = dag.topological_generations()
        assert [[asset.id for asset in level] for level in levels] == [[b.id]]

    def test_topological_generations_on_mini_dag(self):
        a = FakeAsset()
        b = FakeOtherAsset(deps={"a": a.id})
        mini = DAG(a, b).mini_dag(b.id)
        levels = mini.topological_generations()
        assert [[asset.id for asset in level] for level in levels] == [[b.id]]

    def test_get_predecessors_returns_upstream_ids(self):
        upstream = FakeAsset()
        downstream = FakeOtherAsset(deps={"upstream": upstream.id})
        dag = DAG(upstream, downstream)
        assert dag.get_predecessors(downstream.id) == [upstream.id]

    def test_get_predecessors_raises_for_unknown_id(self):
        dag = DAG(FakeAsset())
        with pytest.raises(AssetNotFoundError):
            dag.get_predecessors("nonexistent")

    def test_get_successors_returns_downstream_ids(self):
        upstream = FakeAsset()
        downstream = FakeOtherAsset(deps={"upstream": upstream.id})
        dag = DAG(upstream, downstream)
        assert dag.get_successors(upstream.id) == [downstream.id]

    def test_get_successors_raises_for_unknown_id(self):
        dag = DAG(FakeAsset())
        with pytest.raises(AssetNotFoundError):
            dag.get_successors("nonexistent")


# ---------------------------------------------------------------------------
# Mini DAG subgraph
# ---------------------------------------------------------------------------


class TestMiniDag:
    def test_mini_dag_contains_target_and_parents(self):
        a = FakeAsset()
        b = FakeOtherAsset(deps={"a": a.id})
        dag = DAG(a, b)
        mini = dag.mini_dag(b.id)

        assert len(mini.assets) == 2
        keys = {type(asset).key for asset in mini.assets}
        assert keys == {"fake_asset", "fake_other_asset"}

    def test_mini_dag_marks_parents_non_materializable(self):
        a = FakeAsset()
        b = FakeOtherAsset(deps={"a": a.id})
        dag = DAG(a, b)
        mini = dag.mini_dag(b.id)

        target = next(asset for asset in mini.assets if type(asset).key == "fake_other_asset")
        parent = next(asset for asset in mini.assets if type(asset).key == "fake_asset")
        assert target.materializable is True
        assert parent.materializable is False

    def test_mini_dag_for_root_asset_has_no_parents(self):
        a = FakeAsset()
        dag = DAG(a)
        mini = dag.mini_dag(a.id)
        assert len(mini.assets) == 1
        assert mini.assets[0].materializable is True

    def test_mini_dag_raises_for_unknown_id(self):
        dag = DAG(FakeAsset())
        with pytest.raises(AssetNotFoundError):
            dag.mini_dag("nonexistent")

    def test_mini_dag_for_deep_target_in_complex_dag(self, dag: il.DAG):
        by_key = {type(asset).key: asset for asset in dag.assets}

        mini = dag.mini_dag(by_key["e"].id)
        # ``e`` has direct parents ``b`` and ``c`` only; ``mini_dag`` is shallow.
        assert {type(asset).key for asset in mini.assets} == {"b", "c", "e"}

        target = next(asset for asset in mini.assets if type(asset).key == "e")
        assert target.materializable is True
        for asset in mini.assets:
            if type(asset).key != "e":
                assert asset.materializable is False


# ---------------------------------------------------------------------------
# Serialization round-trip
# ---------------------------------------------------------------------------


class TestSerialization:
    def test_to_spec_returns_dag_spec(self):
        dag = DAG(FakeAsset())
        assert isinstance(dag.to_spec(), DAGSpec)

    def test_spec_captures_one_entry_per_standalone_asset(self):
        dag = DAG(FakeAsset(), FakeOtherAsset())
        spec = dag.to_spec()
        assert len(spec.items) == 2

    def test_spec_items_are_component_specs(self):
        dag = DAG(FakeAsset(), FakeOtherAsset())
        spec = dag.to_spec()
        for entry in spec.items:
            assert isinstance(entry, ComponentSpec)

    def test_plain_roundtrip_preserves_asset_classes(self):
        dag = DAG(FakeAsset(), FakeOtherAsset())
        restored = DAG.from_spec(dag.to_spec())
        assert len(restored.assets) == 2
        assert {type(asset).__name__ for asset in restored.assets} == {"FakeAsset", "FakeOtherAsset"}

    def test_roundtrip_preserves_instance_ids(self):
        a = FakeAsset()
        b = FakeOtherAsset()
        dag = DAG(a, b)
        restored = DAG.from_spec(dag.to_spec())
        assert {asset.id for asset in restored.assets} == {a.id, b.id}

    def test_roundtrip_preserves_predecessor_wiring(self):
        upstream = FakeAsset()
        downstream = FakeOtherAsset(deps={"upstream": upstream.id})
        dag = DAG(upstream, downstream)
        restored = DAG.from_spec(dag.to_spec())

        # Instance ids survive the round-trip, so predecessor wiring matches exactly.
        assert restored.predecessors[downstream.id] == [upstream.id]
        assert restored.successors[upstream.id] == [downstream.id]

    def test_roundtrip_complex_dag_preserves_topology(self, dag: il.DAG):
        restored = DAG.from_spec(dag.to_spec())

        assert len(restored.assets) == len(dag.assets)
        assert restored.topological_generations() != []

        # Every original predecessor edge survives.
        for asset_id, preds in dag.predecessors.items():
            assert sorted(restored.predecessors[asset_id]) == sorted(preds)
        for asset_id, succs in dag.successors.items():
            assert sorted(restored.successors[asset_id]) == sorted(succs)

    def test_roundtrip_via_json_string(self):
        upstream = FakeAsset()
        downstream = FakeOtherAsset(deps={"upstream": upstream.id})
        dag = DAG(upstream, downstream)

        json_str = dag.to_spec().model_dump_json()
        reloaded = DAGSpec.model_validate_json(json_str).reconstruct()

        assert len(reloaded.assets) == 2
        downstream_restored = next(a for a in reloaded.assets if type(a).key == "fake_other_asset")
        upstream_restored = next(a for a in reloaded.assets if type(a).key == "fake_asset")
        assert downstream_restored.deps["upstream"] == upstream_restored.id

    def test_roundtrip_preserves_source_owned_assets(self):
        dag = DAG(FakeSource())
        restored = DAG.from_spec(dag.to_spec())
        assert len(restored.assets) == len(dag.assets)
        assert {type(a).key for a in restored.assets} == {type(a).key for a in dag.assets}

    def test_roundtrip_preserves_mini_dag_shape(self):
        a = FakeAsset()
        b = FakeOtherAsset(deps={"a": a.id})
        dag = DAG(a, b)
        mini = dag.mini_dag(b.id)

        restored = DAG.from_spec(mini.to_spec())
        assert len(restored.assets) == 2
        assert [asset.materializable for asset in restored.assets] == [asset.materializable for asset in mini.assets]

    def test_roundtrip_source_mini_dag_preserves_materializable(self):
        """Mini-DAG from a source: only the target asset is materializable."""
        source = FakeSource()
        dag = DAG(source)
        by_key = {type(a).key: a for a in dag.assets}
        # fake_second depends on fake_first; mini_dag for fake_second
        # should mark fake_first as non-materializable.
        mini = dag.mini_dag(by_key["fake_second"].id)

        restored = DAG.from_spec(mini.to_spec())
        restored_by_key = {type(a).key: a for a in restored.assets}

        assert len(restored.assets) == 2
        assert restored_by_key["fake_second"].materializable is True
        assert restored_by_key["fake_first"].materializable is False

    def test_roundtrip_source_mini_dag_excludes_unrelated_assets(self):
        """Mini-DAG from a source should not include assets that aren't in the subgraph."""
        dag = DAG(FakeComplexSource())
        by_key = {type(a).key: a for a in dag.assets}
        # ``c`` depends on ``a`` only — the mini-DAG should have ``a`` and ``c``,
        # NOT ``b``, ``d``, ``e``, ``f``, ``g``.
        mini = dag.mini_dag(by_key["c"].id)

        restored = DAG.from_spec(mini.to_spec())
        restored_keys = {type(a).key for a in restored.assets}

        assert restored_keys == {"a", "c"}
        for asset in restored.assets:
            if type(asset).key == "c":
                assert asset.materializable is True
            else:
                assert asset.materializable is False

    def test_roundtrip_source_mini_dag_only_one_materializable(self, dag: il.DAG):
        """Every mini-DAG round-trip should have exactly one materializable asset."""
        for asset in dag.assets:
            mini = dag.mini_dag(asset.id)
            restored = DAG.from_spec(mini.to_spec())
            materializable = [a for a in restored.assets if a.materializable]
            assert len(materializable) == 1, (
                f"mini_dag for '{type(asset).key}' has {len(materializable)} "
                f"materializable assets after round-trip, expected 1"
            )
            assert type(materializable[0]).key == type(asset).key
