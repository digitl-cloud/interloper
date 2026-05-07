"""Tests for ``interloper.asset.base``."""

# Note: no ``from __future__ import annotations`` — ``Asset._infer_resource_types``
# reads parameter annotations via ``inspect.signature`` and needs them as real
# classes (not lazily-evaluated strings).

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest

import interloper as il
from interloper.asset.base import AssetDefinition
from interloper.component.base import Component, ComponentSpec
from interloper.errors import AssetError, DestinationError, PartitionError
from interloper.partitioning.base import Partition, PartitionConfig, PartitionWindow

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class FakeResource(il.Resource):
    value: str = ""


class FakeOtherResource(il.Resource):
    other: str = ""


class FakeDestination(il.Destination):
    def read(self, context: Any) -> Any:  # pragma: no cover - not exercised
        return None

    def write(self, context: Any, data: Any) -> None:  # pragma: no cover - not exercised
        pass


class FakeOtherDestination(il.Destination):
    def read(self, context: Any) -> Any:  # pragma: no cover
        return None

    def write(self, context: Any, data: Any) -> None:  # pragma: no cover
        pass


class FakeAsset(il.Asset):
    """Plain asset fixture."""


class FakeOtherAsset(il.Asset):
    """Second asset class used for subclass-identity tests."""


class FakeAssetWithResource(il.Asset):
    """Asset whose ``data()`` signature declares a typed resource dependency."""

    def data(self, config: FakeResource) -> Any:  # pragma: no cover
        return None


class FakeParentSource(il.Source):
    """Minimal source fixture used as the parent of source-owned assets."""


class FakeSourceOwnedAsset(il.Asset):
    """Asset registered onto ``FakeParentSource`` post-hoc (bypassing the ``@source`` decorator)."""


# Post-hoc registration installs the AssetRef descriptor on the source so
# that the composite path ``"module:FakeParentSource.FakeSourceOwnedAsset"``
# can be resolved without instantiating the source.
FakeParentSource.register_asset_type(FakeSourceOwnedAsset)


@dataclass(frozen=True)
class FakePartition(Partition):
    pass


@dataclass(frozen=True)
class FakePartitionWindow(PartitionWindow):
    def __iter__(self) -> Iterator[Partition]:  # noqa: D105 - protocol method
        yield FakePartition(self.start)  # pragma: no cover - not exercised


# ---------------------------------------------------------------------------
# Identity and class metadata
# ---------------------------------------------------------------------------


class TestIdentity:
    def test_key_auto_derived_from_class_name(self):
        assert FakeAsset.key == "fake_asset"
        assert FakeOtherAsset.key == "fake_other_asset"

    def test_kind_is_asset(self):
        assert il.Asset.kind == "asset"
        assert FakeAsset.kind == "asset"

    def test_classpath_for_standalone_asset(self):
        assert FakeAsset.classpath().endswith(".FakeAsset")
        assert ":" not in FakeAsset.classpath()

    def test_classpath_for_source_owned_asset_uses_colon_convention(self):
        cp = FakeSourceOwnedAsset.classpath()
        # Format is "module:SourceName.AssetName" — the colon marks the
        # module / attribute boundary explicitly.
        assert ":" in cp
        assert cp.endswith(":FakeParentSource.FakeSourceOwnedAsset")

    def test_path_on_standalone_instance_equals_classpath(self):
        asset = FakeAsset()
        assert asset.path() == FakeAsset.classpath()

    def test_path_on_source_owned_instance_equals_classpath(self):
        source = FakeParentSource()
        asset = FakeSourceOwnedAsset()
        asset._source = source
        assert asset.path() == FakeSourceOwnedAsset.classpath()

    def test_source_property_none_for_standalone(self):
        assert FakeAsset().source is None

    def test_source_property_set_when_source_attached(self):
        source = FakeParentSource()
        asset = FakeSourceOwnedAsset()
        asset._source = source
        assert asset.source is source

    def test_qualified_key_standalone(self):
        assert FakeAsset().qualified_key == "fake_asset"

    def test_qualified_key_when_source_attached(self):
        source = FakeParentSource()
        asset = FakeSourceOwnedAsset()
        asset._source = source
        assert asset.qualified_key == f"{FakeParentSource.key}.{type(asset).key}"

    def test_data_default_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            FakeAsset().data()


# ---------------------------------------------------------------------------
# Definition metadata
# ---------------------------------------------------------------------------


class TestDefinition:
    def test_definition_returns_asset_definition(self):
        assert isinstance(FakeAsset.definition(), AssetDefinition)

    def test_definition_fields_populated(self):
        defn = FakeAsset.definition()
        assert defn.kind == "asset"
        assert defn.key == "fake_asset"
        assert defn.path == FakeAsset.classpath()
        assert defn.name
        assert defn.resources == {}
        assert defn.destinations == []
        assert defn.requires == {}
        assert defn.optional_requires == {}
        assert defn.asset_schema is None
        assert defn.partitioning is None

    def test_definition_uses_classpath_for_source_owned(self):
        defn = FakeSourceOwnedAsset.definition()
        assert defn.path == FakeSourceOwnedAsset.classpath()
        assert ":" in defn.path

    def test_definition_qualified_key_standalone(self):
        defn = FakeAsset.definition()
        assert defn.qualified_key == "fake_asset"

    def test_definition_qualified_key_with_source_key(self):
        defn = FakeAsset.definition().model_copy(update={"source_key": "my_source"})
        assert defn.qualified_key == "my_source.fake_asset"

    def test_definition_includes_inferred_resources(self):
        defn = FakeAssetWithResource.definition()
        assert defn.resources == {"config": FakeResource.key}

    def test_definition_includes_asset_schema_when_set(self):
        from typing import ClassVar

        class FakeSchema(il.Schema):
            value: str

        class FakeAssetWithSchema(il.Asset):
            schema: ClassVar[type[il.Schema] | None] = FakeSchema

        defn = FakeAssetWithSchema.definition()
        assert isinstance(defn.asset_schema, dict)
        assert "properties" in defn.asset_schema

    def test_definition_includes_partitioning_when_set(self):
        from typing import ClassVar

        class FakeAssetPartitioned(il.Asset):
            partitioning: ClassVar[PartitionConfig | None] = PartitionConfig(column="day")

        defn = FakeAssetPartitioned.definition()
        assert defn.partitioning == {"column": "day", "allow_window": False}


# ---------------------------------------------------------------------------
# Resource type inference and runtime resolution
# ---------------------------------------------------------------------------


class TestResources:
    def test_inferred_from_data_annotations(self):
        assert FakeAssetWithResource.resource_types == {"config": FakeResource}

    def test_no_inference_when_data_not_overridden(self):
        assert FakeAsset.resource_types == {}

    def test_skips_self_context_source_kwargs(self):
        class FakeAssetReservedParams(il.Asset):
            def data(self, context: Any, source: Any, **kwargs: Any) -> Any:  # pragma: no cover
                return None

        assert FakeAssetReservedParams.resource_types == {}

    def test_skips_params_without_annotation(self):
        class FakeAssetNoAnnotation(il.Asset):
            def data(self, untyped) -> Any:  # type: ignore[no-untyped-def]  # pragma: no cover
                return None

        assert FakeAssetNoAnnotation.resource_types == {}

    def test_explicit_entries_take_precedence_over_inferred(self):
        from typing import ClassVar

        class FakeAssetExplicit(il.Asset):
            resource_types: ClassVar[dict[str, type[il.Resource]]] = {"config": FakeOtherResource}

            def data(self, config: FakeResource) -> Any:  # pragma: no cover
                return None

        assert FakeAssetExplicit.resource_types["config"] is FakeOtherResource

    def test_resolve_own_resource(self):
        own = FakeResource(value="own")
        asset = FakeAssetWithResource(resources={"config": own})
        assert asset._resolve_resource("config") is own

    def test_resolve_falls_back_to_source_by_name(self):
        asset = FakeAssetWithResource()
        source = FakeParentSource(resources={"config": FakeResource(value="from_source")})
        asset._source = source
        resolved = asset._resolve_resource("config")
        assert isinstance(resolved, FakeResource)
        assert resolved.value == "from_source"

    def test_resolve_falls_back_to_source_by_type(self):
        asset = FakeAssetWithResource()
        source = FakeParentSource(resources={"elsewhere": FakeResource(value="by_type")})
        asset._source = source
        resolved = asset._resolve_resource("config")
        assert isinstance(resolved, FakeResource)
        assert resolved.value == "by_type"

    def test_resolve_auto_instantiates_when_nothing_configured(self):
        asset = FakeAssetWithResource()
        resolved = asset._resolve_resource("config")
        assert isinstance(resolved, FakeResource)

    def test_resolve_raises_on_type_mismatch(self):
        asset = FakeAssetWithResource(resources={"config": FakeOtherResource()})
        with pytest.raises(AssetError):
            asset._resolve_resource("config")


# ---------------------------------------------------------------------------
# Destination resolution and validation
# ---------------------------------------------------------------------------


class TestDestinations:
    def test_resolve_asset_own_destination(self):
        dest = FakeDestination()
        asset = FakeAsset(destination=dest)
        assert asset._resolve_destinations() == [dest]

    def test_resolve_wraps_single_destination_in_list(self):
        asset = FakeAsset(destination=FakeDestination())
        resolved = asset._resolve_destinations()
        assert isinstance(resolved, list)
        assert len(resolved) == 1

    def test_resolve_keeps_list_destination(self):
        dests = [FakeDestination(), FakeOtherDestination()]
        asset = FakeAsset(destination=dests)
        assert asset._resolve_destinations() == dests

    def test_resolve_falls_back_to_source_destination(self):
        source_dest = FakeDestination()
        source = FakeParentSource(destination=source_dest)
        asset = FakeAsset()
        asset._source = source
        assert asset._resolve_destinations() == [source_dest]

    def test_resolve_returns_empty_when_nothing_configured(self):
        assert FakeAsset()._resolve_destinations() == []

    def test_validate_destination_is_noop_when_types_empty(self):
        # FakeAsset has no destination_types → anything is allowed.
        FakeAsset()._validate_destination(FakeDestination())

    def test_validate_destination_accepts_declared_type(self):
        from typing import ClassVar

        class FakeAssetTypedDest(il.Asset):
            destination_types: ClassVar[list[type[il.Destination]]] = [FakeDestination]

        FakeAssetTypedDest()._validate_destination(FakeDestination())

    def test_validate_destination_rejects_undeclared_type(self):
        from typing import ClassVar

        class FakeAssetTypedDest(il.Asset):
            destination_types: ClassVar[list[type[il.Destination]]] = [FakeDestination]

        with pytest.raises(DestinationError):
            FakeAssetTypedDest()._validate_destination(FakeOtherDestination())


# ---------------------------------------------------------------------------
# Partitioning validation
# ---------------------------------------------------------------------------


class FakeAssetPartitioned(il.Asset):
    partitioning = PartitionConfig(column="day", allow_window=False)


class FakeAssetPartitionedWithWindow(il.Asset):
    partitioning = PartitionConfig(column="day", allow_window=True)


class TestPartitioning:
    def test_unpartitioned_asset_with_no_partition_passes(self):
        FakeAsset()._validate_partitioning(None)

    def test_unpartitioned_asset_with_partition_warns(self):
        with pytest.warns(UserWarning):
            FakeAsset()._validate_partitioning(FakePartition(value="x"))

    def test_partitioned_asset_without_partition_raises(self):
        with pytest.raises(PartitionError):
            FakeAssetPartitioned()._validate_partitioning(None)

    def test_partitioned_asset_with_single_partition_passes(self):
        FakeAssetPartitioned()._validate_partitioning(FakePartition(value="x"))

    def test_window_not_allowed_raises(self):
        window = FakePartitionWindow(start="a", end="b")
        with pytest.raises(PartitionError):
            FakeAssetPartitioned()._validate_partitioning(window)

    def test_window_allowed_passes(self):
        window = FakePartitionWindow(start="a", end="b")
        FakeAssetPartitionedWithWindow()._validate_partitioning(window)


# ---------------------------------------------------------------------------
# __call__ reconfiguration
# ---------------------------------------------------------------------------


class TestReconfiguration:
    def test_returns_a_copy(self):
        asset = FakeAsset()
        reconfigured = asset(dataset="new")
        assert reconfigured is not asset
        assert type(reconfigured) is type(asset)

    def test_override_id(self):
        asset = FakeAsset(id="original")
        assert asset(id="updated").id == "updated"

    def test_override_dataset(self):
        assert FakeAsset()(dataset="my_ds").dataset == "my_ds"

    def test_override_materializable(self):
        assert FakeAsset(materializable=True)(materializable=False).materializable is False

    def test_override_destination(self):
        new_dest = FakeOtherDestination()
        reconfigured = FakeAsset(destination=FakeDestination())(destination=new_dest)
        assert reconfigured.destination is new_dest

    def test_override_deps(self):
        reconfigured = FakeAsset()(deps={"upstream": "abc"})
        assert reconfigured.deps == {"upstream": "abc"}

    def test_resources_are_merged_not_replaced(self):
        existing = FakeResource(value="existing")
        extra = FakeOtherResource(other="extra")
        asset = FakeAsset(resources={"a": existing})
        reconfigured = asset(resources={"b": extra})
        assert reconfigured.resources == {"a": existing, "b": extra}

    def test_normalizer_explicit_none_clears_normalizer(self):
        # The ``normalizer`` parameter uses a _UNSET sentinel so that
        # passing ``None`` explicitly means "clear it" (not "unchanged").
        reconfigured = FakeAsset()(normalizer=None)
        assert reconfigured.normalizer is None

    def test_omitted_fields_preserved(self):
        asset = FakeAsset(dataset="original", materializable=False)
        reconfigured = asset(dataset="updated")
        assert reconfigured.materializable is False


# ---------------------------------------------------------------------------
# Serialization round-trip
# ---------------------------------------------------------------------------


class TestSerialization:
    def test_standalone_asset_roundtrip(self):
        asset = FakeAsset(dataset="ds", materializable=False)
        restored = Component.from_spec(asset.to_spec())
        assert isinstance(restored, FakeAsset)
        assert restored.dataset == "ds"
        assert restored.materializable is False

    def test_asset_with_destination_roundtrip(self):
        asset = FakeAsset(destination=FakeDestination())
        restored = Component.from_spec(asset.to_spec())
        assert isinstance(restored, FakeAsset)
        assert isinstance(restored.destination, FakeDestination)

    def test_asset_with_list_of_destinations_roundtrip(self):
        asset = FakeAsset(destination=[FakeDestination(), FakeOtherDestination()])
        restored = FakeAsset.from_spec(asset.to_spec())
        assert isinstance(restored.destination, list)
        assert isinstance(restored.destination[0], FakeDestination)
        assert isinstance(restored.destination[1], FakeOtherDestination)

    def test_asset_with_resources_roundtrip(self):
        asset = FakeAsset(resources={"config": FakeResource(value="abc")})
        restored = FakeAsset.from_spec(asset.to_spec())
        config = restored.resources["config"]
        assert isinstance(config, FakeResource)
        assert config.value == "abc"

    def test_asset_with_deps_roundtrip(self):
        asset = FakeAsset(deps={"upstream": "asset-id-123"})
        restored = FakeAsset.from_spec(asset.to_spec())
        assert restored.deps == {"upstream": "asset-id-123"}

    def test_asset_preserves_instance_id(self):
        asset = FakeAsset(id="fixed123")
        restored = Component.from_spec(asset.to_spec())
        assert restored.id == "fixed123"

    def test_source_owned_asset_roundtrip_preserves_subclass(self):
        source = FakeParentSource()
        asset = FakeSourceOwnedAsset(dataset="override", materializable=False)
        asset._source = source

        restored = FakeSourceOwnedAsset.from_spec(asset.to_spec())
        assert isinstance(restored, FakeSourceOwnedAsset)
        assert restored.dataset == "override"
        assert restored.materializable is False

    def test_roundtrip_via_json_string(self):
        asset = FakeAsset(
            dataset="ds",
            destination=[FakeDestination(), FakeOtherDestination()],
            resources={"config": FakeResource(value="v")},
        )
        spec_json = asset.to_spec().model_dump_json()
        restored = ComponentSpec.model_validate_json(spec_json).reconstruct()

        assert isinstance(restored, FakeAsset)
        assert restored.dataset == "ds"
        assert isinstance(restored.destination, list)
        assert isinstance(restored.resources["config"], FakeResource)
