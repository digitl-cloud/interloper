"""Tests for ``interloper.source.base``."""

# Note: no ``from __future__ import annotations`` — ``Source._infer_all_requires``
# and ``Asset._infer_resource_types`` read parameter annotations via
# ``inspect.signature`` and need them as real classes, not lazy strings.

from typing import Any

import pytest

import interloper as il
from interloper.asset.base import AssetDefinition
from interloper.component.base import Component, ComponentSpec
from interloper.normalizer import MaterializationStrategy, Normalizer
from interloper.source.base import SourceDefinition

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class FakeResource(il.Resource):
    value: str = ""


class FakeOtherResource(il.Resource):
    other: str = ""


class FakeDestination(il.Destination):
    def read(self, context: Any) -> Any:  # pragma: no cover
        return None

    def write(self, context: Any, data: Any) -> None:  # pragma: no cover
        pass


class FakeOtherDestination(il.Destination):
    def read(self, context: Any) -> Any:  # pragma: no cover
        return None

    def write(self, context: Any, data: Any) -> None:  # pragma: no cover
        pass


class FakeSource(il.Source):
    """Plain source fixture (no nested assets)."""


class FakeOtherSource(il.Source):
    """Second source class used for subclass-identity tests."""


class FakeSourceWithAssets(il.Source):
    """Source with two nested assets; the second references the first as a dependency."""

    class FakeFirst(il.Asset):
        """First asset (no upstream deps)."""

    class FakeSecond(il.Asset):
        """Second asset depending on the first sibling by parameter name."""

        def data(self, fake_first: Any) -> Any:  # pragma: no cover
            return None


# ---------------------------------------------------------------------------
# Identity and class metadata
# ---------------------------------------------------------------------------


class TestIdentity:
    def test_key_auto_derived_from_class_name(self):
        assert FakeSource.key == "fake_source"
        assert FakeOtherSource.key == "fake_other_source"
        assert FakeSourceWithAssets.key == "fake_source_with_assets"

    def test_kind_is_source(self):
        assert il.Source.kind == "source"
        assert FakeSource.kind == "source"

    def test_dataset_default_equals_source_key(self):
        # ``_resolve`` fills an empty ``dataset`` with the source key at
        # instance init — no class-level field mutation involved.
        assert FakeSource().dataset == FakeSource.key

    def test_explicit_dataset_default_is_preserved(self):
        class FakeExplicitDatasetSource(il.Source):
            dataset: str = "custom"

        assert FakeExplicitDatasetSource().dataset == "custom"

    def test_tags_default_empty_list(self):
        assert FakeSource.tags == []

    def test_asset_types_default_empty_list(self):
        assert FakeSource.asset_types == []


# ---------------------------------------------------------------------------
# Definition metadata
# ---------------------------------------------------------------------------


class TestDefinition:
    def test_definition_returns_source_definition(self):
        assert isinstance(FakeSource.definition(), SourceDefinition)

    def test_definition_fields_populated(self):
        defn = FakeSource.definition()
        assert defn.kind == "source"
        assert defn.key == "fake_source"
        assert defn.path.endswith(".FakeSource")
        assert defn.name
        assert defn.assets == []
        assert defn.relations["resource"].slots == {}
        assert defn.relations["destination"].keys == []

    def test_definition_includes_nested_assets(self):
        defn = FakeSourceWithAssets.definition()
        assert len(defn.assets) == 2
        assert all(isinstance(a, AssetDefinition) for a in defn.assets)
        asset_keys = {a.key for a in defn.assets}
        assert asset_keys == {"fake_first", "fake_second"}

    def test_definition_asset_source_keys_are_set(self):
        defn = FakeSourceWithAssets.definition()
        for asset_defn in defn.assets:
            assert asset_defn.source_key == FakeSourceWithAssets.key


# ---------------------------------------------------------------------------
# Asset collection, lookup, and requires inference
# ---------------------------------------------------------------------------


class TestAssets:
    def test_nested_asset_classes_collected_into_asset_types(self):
        assert len(FakeSourceWithAssets.asset_types) == 2
        asset_keys = {cls.key for cls in FakeSourceWithAssets.asset_types}
        assert asset_keys == {"fake_first", "fake_second"}

    def test_nested_asset_classes_replaced_by_asset_ref_descriptors(self):
        # After ``_collect_asset_types``, each nested class attribute is
        # replaced by an ``AssetRef`` descriptor.  Class access returns
        # the asset class, instance access returns the asset instance.
        from interloper.source.base import AssetRef

        assert isinstance(FakeSourceWithAssets.__dict__["FakeFirst"], AssetRef)
        assert isinstance(FakeSourceWithAssets.__dict__["FakeSecond"], AssetRef)
        # Class-level access returns the asset class.
        assert FakeSourceWithAssets.FakeFirst.key == "fake_first"
        assert FakeSourceWithAssets.FakeSecond.key == "fake_second"

    def test_model_post_init_auto_instantiates_assets_from_types(self):
        source = FakeSourceWithAssets()
        assert len(source.assets) == 2
        assert {type(a).key for a in source.assets} == {"fake_first", "fake_second"}

    def test_model_post_init_skips_auto_instantiation_when_assets_supplied(self):
        # Reconstruction / explicit construction must not duplicate the assets.
        asset = FakeSourceWithAssets.asset_types[0]()
        source = FakeSourceWithAssets(assets=[asset])
        assert source.assets == [asset]

    def test_getattr_returns_asset_instance_by_key(self):
        source = FakeSourceWithAssets()
        looked_up = source.fake_first
        assert type(looked_up).key == "fake_first"
        assert looked_up is source.assets[0]

    def test_getattr_raises_for_unknown_key(self):
        source = FakeSourceWithAssets()
        with pytest.raises(AttributeError):
            _ = source.does_not_exist

    def test_asset_def_returns_definition_with_source_key(self):
        defn = FakeSourceWithAssets.asset_def("fake_first")
        assert isinstance(defn, AssetDefinition)
        assert defn.key == "fake_first"
        assert defn.source_key == FakeSourceWithAssets.key
        assert defn.qualified_key == f"{FakeSourceWithAssets.key}.fake_first"

    def test_asset_def_raises_for_unknown_key(self):
        with pytest.raises(KeyError):
            FakeSourceWithAssets.asset_def("does_not_exist")

    def test_infer_all_requires_wires_sibling_dependency(self):
        second_cls = next(
            cls for cls in FakeSourceWithAssets.asset_types if cls.key == "fake_second"
        )
        assert "fake_first" in second_cls.requires
        assert second_cls.requires["fake_first"] == f"{FakeSourceWithAssets.key}.fake_first"

    def test_infer_all_requires_marks_default_none_as_optional(self):
        class FakeOptionalDepsSource(il.Source):
            class FakeA(il.Asset):
                pass

            class FakeB(il.Asset):
                def data(self, fake_a: Any = None) -> Any:  # pragma: no cover
                    return None

        second_cls = next(
            cls for cls in FakeOptionalDepsSource.asset_types if cls.key == "fake_b"
        )
        assert "fake_a" in second_cls.optional_requires
        assert "fake_a" not in second_cls.requires


# ---------------------------------------------------------------------------
# Trickle-down resolution
# ---------------------------------------------------------------------------


class TestResolution:
    def test_trickles_dataset_to_assets_without_one(self):
        class FakeTrickleSource(il.Source):
            class FakeChild(il.Asset):
                pass

        source = FakeTrickleSource(dataset="parent_ds")
        child = source.assets[0]
        assert child.dataset == "parent_ds"

    def test_preserves_asset_own_dataset(self):
        class FakeTrickleSource(il.Source):
            class FakeChild(il.Asset):
                dataset: str = "child_own"

        source = FakeTrickleSource(dataset="parent_ds")
        assert source.assets[0].dataset == "child_own"

    def test_trickles_destination_to_assets(self):
        source_dest = FakeDestination()

        class FakeTrickleSource(il.Source):
            class FakeChild(il.Asset):
                pass

        source = FakeTrickleSource(destinations=[source_dest])
        assert source.assets[0].destinations == [source_dest]

    def test_trickles_normalizer_to_assets(self):
        normalizer = Normalizer()

        class FakeTrickleSource(il.Source):
            class FakeChild(il.Asset):
                pass

        source = FakeTrickleSource(normalizer=normalizer)
        assert source.assets[0].normalizer is normalizer

    def test_trickles_materialization_strategy_when_asset_is_auto(self):
        class FakeTrickleSource(il.Source):
            class FakeChild(il.Asset):
                pass

        source = FakeTrickleSource(materialization_strategy=MaterializationStrategy.STRICT)
        assert source.assets[0].materialization_strategy == MaterializationStrategy.STRICT

    def test_preserves_asset_own_materialization_strategy(self):
        class FakeTrickleSource(il.Source):
            class FakeChild(il.Asset):
                materialization_strategy: MaterializationStrategy = MaterializationStrategy.RECONCILE

        source = FakeTrickleSource(materialization_strategy=MaterializationStrategy.STRICT)
        assert source.assets[0].materialization_strategy == MaterializationStrategy.RECONCILE

    def test_trickles_default_destination_key(self):
        class FakeTrickleSource(il.Source):
            class FakeChild(il.Asset):
                pass

        source = FakeTrickleSource(default_destination_key="primary")
        assert source.assets[0].default_destination_key == "primary"

    def test_trickles_source_resources_to_assets_by_type(self):
        shared = FakeResource(value="shared")

        class FakeTrickleSource(il.Source):
            class FakeChild(il.Asset):
                def data(self, resource: FakeResource) -> Any:  # pragma: no cover
                    return None

        source = FakeTrickleSource(resources={"elsewhere": shared})
        assert source.assets[0].resources["resource"] is shared

    def test_assets_get_source_backref(self):
        class FakeTrickleSource(il.Source):
            class FakeChild(il.Asset):
                pass

        source = FakeTrickleSource()
        assert source.assets[0].source is source

    def test_resolves_sibling_deps_between_assets(self):
        source = FakeSourceWithAssets()
        first = source.fake_first
        second = source.fake_second
        # ``_infer_all_requires`` populated ``second.requires``; ``_resolve_deps``
        # wires those into ``second.deps`` pointing at the sibling's instance id.
        assert second.dependencies["fake_first"] == first.id


# ---------------------------------------------------------------------------
# __call__ reconfiguration
# ---------------------------------------------------------------------------


class TestReconfiguration:
    def test_returns_a_copy(self):
        source = FakeSourceWithAssets()
        reconfigured = source(dataset="new")
        assert reconfigured is not source
        assert type(reconfigured) is type(source)

    def test_override_dataset(self):
        assert FakeSourceWithAssets()(dataset="override").dataset == "override"

    def test_override_destination(self):
        new_dest = FakeDestination()
        reconfigured = FakeSourceWithAssets()(destinations=new_dest)
        assert reconfigured.destinations == [new_dest]

    def test_resources_are_merged_not_replaced(self):
        existing = FakeResource(value="existing")
        extra = FakeOtherResource(other="extra")

        class FakeMergeSource(il.Source):
            pass

        source = FakeMergeSource(resources={"a": existing})
        reconfigured = source(resources={"b": extra})
        assert reconfigured.resources == {"a": existing, "b": extra}

    def test_materializable_override_propagates_to_assets(self):
        source = FakeSourceWithAssets()
        reconfigured = source(materializable=False)
        assert all(a.materializable is False for a in reconfigured.assets)

    def test_copied_assets_have_source_backref_on_copy(self):
        source = FakeSourceWithAssets()
        reconfigured = source(dataset="new")
        assert all(a.source is reconfigured for a in reconfigured.assets)

    def test_omitted_fields_preserved(self):
        source = FakeSourceWithAssets(
            dataset="original",
            destinations=[FakeDestination()],
        )
        reconfigured = source(dataset="updated")
        assert isinstance(reconfigured.destinations[0], FakeDestination)

    def test_override_default_destination_key(self):
        reconfigured = FakeSourceWithAssets()(default_destination_key="primary")
        assert reconfigured.default_destination_key == "primary"

    def test_override_normalizer(self):
        normalizer = Normalizer()
        reconfigured = FakeSourceWithAssets()(normalizer=normalizer)
        assert reconfigured.normalizer is normalizer

    def test_override_materialization_strategy(self):
        reconfigured = FakeSourceWithAssets()(
            materialization_strategy=MaterializationStrategy.STRICT,
        )
        assert reconfigured.materialization_strategy == MaterializationStrategy.STRICT


# ---------------------------------------------------------------------------
# Serialization round-trip
# ---------------------------------------------------------------------------


class TestSerialization:
    def test_plain_source_roundtrip(self):
        source = FakeSource(dataset="ds")
        restored = Component.from_spec(source.to_spec())
        assert isinstance(restored, FakeSource)
        assert restored.dataset == "ds"

    def test_source_with_assets_roundtrip_preserves_asset_count(self):
        source = FakeSourceWithAssets()
        restored = FakeSourceWithAssets.from_spec(source.to_spec())
        assert len(restored.assets) == len(source.assets)

    def test_source_roundtrip_preserves_per_asset_mutation(self):
        source = FakeSourceWithAssets()
        source.assets[0].materializable = False
        source.assets[0].dataset = "custom"

        restored = FakeSourceWithAssets.from_spec(source.to_spec())
        assert restored.assets[0].materializable is False
        assert restored.assets[0].dataset == "custom"
        # Other asset unchanged
        assert restored.assets[1].materializable is True

    def test_source_with_destination_roundtrip(self):
        source = FakeSource(destinations=[FakeDestination()])
        restored = FakeSource.from_spec(source.to_spec())
        assert isinstance(restored.destinations[0], FakeDestination)

    def test_source_with_list_of_destinations_roundtrip(self):
        source = FakeSource(destinations=[FakeDestination(), FakeOtherDestination()])
        restored = FakeSource.from_spec(source.to_spec())
        assert isinstance(restored.destinations[0], FakeDestination)
        assert isinstance(restored.destinations[1], FakeOtherDestination)

    def test_source_with_resources_roundtrip(self):
        source = FakeSource(resources={"config": FakeResource(value="abc")})
        restored = FakeSource.from_spec(source.to_spec())
        config = restored.resources["config"]
        assert isinstance(config, FakeResource)
        assert config.value == "abc"

    def test_source_preserves_instance_id(self):
        source = FakeSource(id="fixed123")
        restored = Component.from_spec(source.to_spec())
        assert restored.id == "fixed123"

    def test_roundtrip_via_json_string(self):
        source = FakeSourceWithAssets(
            dataset="ds",
            destinations=[FakeDestination()],
            resources={"config": FakeResource(value="v")},
        )
        source.assets[0].materializable = False

        spec_json = source.to_spec().model_dump_json()
        restored = ComponentSpec.model_validate_json(spec_json).reconstruct()

        assert isinstance(restored, FakeSourceWithAssets)
        assert restored.dataset == "ds"
        assert isinstance(restored.destinations[0], FakeDestination)
        assert isinstance(restored.resources["config"], FakeResource)
        assert restored.assets[0].materializable is False


class TestSelect:
    """Init-time asset selection via the ``select`` field."""

    def test_unselected_assets_stay_as_non_materializable_deps(self):
        source = FakeSourceWithAssets(select=["fake_second"])
        by_key = {type(a).key: a for a in source.assets}
        assert by_key["fake_second"].materializable
        assert not by_key["fake_first"].materializable
        # The non-materializable sibling stays wired as a dependency.
        assert by_key["fake_first"].id in by_key["fake_second"].dependencies.values()

    def test_selected_assets_keep_their_source(self):
        source = FakeSourceWithAssets(select=["fake_first"])
        assert all(a.source is source for a in source.assets)

    def test_unknown_select_key_raises(self):
        # SourceError is a ValueError, so pydantic wraps it at init time.
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="has no asset"):
            FakeSourceWithAssets(select=["nope"])

    def test_dag_over_selected_source(self):
        dag = FakeSourceWithAssets(select=["fake_second"]).dag()
        generations = dag.topological_generations()
        assert [[type(a).key for a in g] for g in generations] == [["fake_second"]]
