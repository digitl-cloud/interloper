"""Tests for ``interloper.asset.base``."""

# Note: no ``from __future__ import annotations`` — ``Asset._infer_resource_types``
# reads parameter annotations via ``inspect.signature`` and needs them as real
# classes (not lazily-evaluated strings).

import asyncio
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest

import interloper as il
from interloper.asset.base import AssetDefinition
from interloper.component.base import Component, ComponentSpec
from interloper.errors import AssetError, DestinationError, PartitionError
from interloper.events import Event, EventBus, EventType
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
        assert defn.relations["resource"].slots == {}
        assert defn.relations["destination"].keys == []
        assert defn.relations["dependency"].slots == {}
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
        assert defn.relations["resource"].slots["config"].key == FakeResource.key

    def test_definition_dependency_slots_from_requires(self):
        from typing import ClassVar

        class FakeDependentAsset(il.Asset):
            requires: ClassVar[dict[str, str]] = {"upstream": "other_source.things"}
            optional_requires: ClassVar[dict[str, str]] = {"extra": "other_source.extras"}

        slots = FakeDependentAsset.definition().relations["dependency"].slots
        assert slots["upstream"].key == "other_source.things"
        assert slots["upstream"].required is True
        assert slots["extra"].required is False

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
            def data(self, untyped) -> Any:  # pragma: no cover
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
        asset = FakeAsset(destinations=[dest])
        assert asset._resolve_destinations() == [dest]

    def test_resolve_wraps_single_destination_in_list(self):
        asset = FakeAsset(destinations=[FakeDestination()])
        resolved = asset._resolve_destinations()
        assert isinstance(resolved, list)
        assert len(resolved) == 1

    def test_resolve_keeps_list_destination(self):
        dests = [FakeDestination(), FakeOtherDestination()]
        asset = FakeAsset(destinations=dests)  # ty: ignore[invalid-argument-type]
        assert asset._resolve_destinations() == dests

    def test_resolve_falls_back_to_source_destination(self):
        source_dest = FakeDestination()
        source = FakeParentSource(destinations=[source_dest])
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
        reconfigured = FakeAsset(destinations=[FakeDestination()])(destinations=new_dest)
        assert reconfigured.destinations == [new_dest]

    def test_override_deps(self):
        reconfigured = FakeAsset()(dependencies={"upstream": "abc"})
        assert reconfigured.dependencies == {"upstream": "abc"}

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
        asset = FakeAsset(destinations=[FakeDestination()])
        restored = Component.from_spec(asset.to_spec())
        assert isinstance(restored, FakeAsset)
        assert isinstance(restored.destinations[0], FakeDestination)

    def test_asset_with_list_of_destinations_roundtrip(self):
        asset = FakeAsset(destinations=[FakeDestination(), FakeOtherDestination()])
        restored = FakeAsset.from_spec(asset.to_spec())
        assert isinstance(restored.destinations[0], FakeDestination)
        assert isinstance(restored.destinations[1], FakeOtherDestination)

    def test_asset_with_resources_roundtrip(self):
        asset = FakeAsset(resources={"config": FakeResource(value="abc")})
        restored = FakeAsset.from_spec(asset.to_spec())
        config = restored.resources["config"]
        assert isinstance(config, FakeResource)
        assert config.value == "abc"

    def test_asset_with_deps_roundtrip(self):
        asset = FakeAsset(dependencies={"upstream": "asset-id-123"})
        restored = FakeAsset.from_spec(asset.to_spec())
        assert restored.dependencies == {"upstream": "asset-id-123"}

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
            destinations=[FakeDestination(), FakeOtherDestination()],
            resources={"config": FakeResource(value="v")},
        )
        spec_json = asset.to_spec().model_dump_json()
        restored = ComponentSpec.model_validate_json(spec_json).reconstruct()

        assert isinstance(restored, FakeAsset)
        assert restored.dataset == "ds"
        assert isinstance(restored.destinations, list)
        assert isinstance(restored.resources["config"], FakeResource)


# ---------------------------------------------------------------------------
# Destination write — empty-result handling
# ---------------------------------------------------------------------------


async def _capture_log_events(coro: Any) -> list[Event]:
    captured: list[Event] = []

    def handler(event: Event) -> None:
        captured.append(event)

    EventBus.subscribe(handler)
    try:
        await coro
        EventBus.flush(timeout=5.0)
    finally:
        EventBus.unsubscribe(handler)
    return [e for e in captured if e.type == EventType.LOG]


class TestDestinationWrite:
    async def test_empty_result_skips_write_and_warns(self):
        il.MemoryDestination.clear()
        mem = il.MemoryDestination()

        @il.asset()
        def empty() -> list[dict[str, Any]]:
            return []

        asset = empty(id="empty", destinations=[mem])
        logs = await _capture_log_events(asset.materialize_async())

        # Nothing was written.
        assert mem._storage == {}

        warnings = [
            e
            for e in logs
            if e.metadata.get("level") == "WARNING" and "produced no data" in (e.metadata.get("message") or "")
        ]
        assert len(warnings) == 1
        # The warning is attributed to the asset so it filters/labels in the UI.
        assert warnings[0].metadata.get("asset_id") == asset.id

    async def test_non_empty_result_is_written(self):
        il.MemoryDestination.clear()
        mem = il.MemoryDestination()

        @il.asset()
        def full() -> list[dict[str, Any]]:
            return [{"a": 1}]

        asset = full(id="full", destinations=[mem])
        logs = await _capture_log_events(asset.materialize_async())

        # Data was written and no "no data" warning was emitted.
        assert mem._storage
        assert not [e for e in logs if "produced no data" in (e.metadata.get("message") or "")]


# ---------------------------------------------------------------------------
# Conform (schema enforcement decoupled from normalizer)
# ---------------------------------------------------------------------------


class ConformSchema(il.Schema):
    user_id: int | None = None
    name: str | None = None


class StrictConformSchema(il.Schema):
    user_id: int | None
    name: str | None


class TestAsyncAndSyncData:
    """``@asset`` accepts both sync and ``async`` data functions."""

    async def test_sync_data_function(self):
        @il.asset
        def users() -> list[dict[str, Any]]:
            return [{"id": 1}]

        assert not asyncio.iscoroutinefunction(users().data)
        assert await users().run_async() == [{"id": 1}]

    async def test_async_data_function_is_awaited_natively(self):
        @il.asset
        async def users() -> list[dict[str, Any]]:
            return [{"id": 1}]

        # The decorator must preserve coroutine-ness, otherwise the engine
        # would offload a sync wrapper to a thread and return an un-awaited
        # coroutine instead of the data.
        assert asyncio.iscoroutinefunction(users().data)
        assert await users().run_async() == [{"id": 1}]

    def test_run_is_callable_directly_from_sync_code(self):
        # The manual script/REPL/notebook path: run() drives the async
        # engine on the bridge loop, no asyncio.run required.
        @il.asset
        def users() -> list[dict[str, Any]]:
            return [{"id": 1}]

        assert users().run() == [{"id": 1}]

    def test_materialize_is_callable_directly_from_sync_code(self):
        captured: dict[str, Any] = {}

        class CapturingDestination(il.Destination):
            def read(self, context: Any) -> Any:  # pragma: no cover - not exercised
                return None

            def write(self, context: Any, data: Any) -> None:
                captured["data"] = data

        @il.asset
        def users() -> list[dict[str, Any]]:
            return [{"id": 1}]

        users(destinations=[CapturingDestination(id="sync-dest")]).materialize()
        assert captured["data"] == [{"id": 1}]

    async def test_async_destination_write_is_awaited(self):
        # A destination may implement ``write`` as ``async def``; materialize
        # must await it natively rather than hand it a coroutine to a thread.
        captured: dict[str, Any] = {}

        class AsyncDestination(il.Destination):
            def read(self, context: Any) -> Any:  # pragma: no cover - not exercised
                return None

            async def write(self, context: Any, data: Any) -> None:
                captured["data"] = data

        @il.asset
        def users() -> list[dict[str, Any]]:
            return [{"id": 1}]

        asset = users(destinations=[AsyncDestination(id="async-dest")])
        await asset.materialize_async()
        assert captured["data"] == [{"id": 1}]


class TestConform:
    """Schema enforcement runs whether or not a normalizer is configured."""

    async def test_schema_validates_without_normalizer(self):
        @il.asset(schema=ConformSchema)
        def users() -> list[dict[str, Any]]:
            return [{"user_id": 1, "name": "a"}]

        assert await users().run_async() == [{"user_id": 1, "name": "a"}]

    async def test_mismatched_data_fails_fast(self):
        from interloper.errors import SchemaError

        @il.asset(schema=ConformSchema)
        def users() -> list[dict[str, Any]]:
            return [{"user_id": "not-an-int", "name": "a"}]

        with pytest.raises(SchemaError):
            await users().run_async()

    async def test_dataframe_validated_without_normalizer(self):
        pd = pytest.importorskip("pandas")
        from interloper.errors import SchemaError

        @il.asset(schema=StrictConformSchema)
        def users() -> Any:
            return pd.DataFrame([{"userId": 1, "Name": "a"}])  # wrong casing -> required fields missing

        with pytest.raises(SchemaError):
            await users().run_async()

    async def test_dataframe_with_nan_validates_against_nullable_fields(self):
        pd = pytest.importorskip("pandas")
        import numpy as np

        @il.asset(schema=ConformSchema)
        def users() -> Any:
            return pd.DataFrame([{"user_id": np.nan, "name": "a"}])

        result = await users().run_async()
        assert isinstance(result, pd.DataFrame)

    async def test_strategy_requires_schema(self):
        from interloper.normalizer import MaterializationStrategy

        @il.asset(materialization_strategy=MaterializationStrategy.RECONCILE)
        def users() -> list[dict[str, Any]]:
            return [{"a": 1}]

        with pytest.raises(AssetError, match="requires a schema"):
            await users().run_async()

    async def test_reconcile_without_normalizer(self):
        from interloper.normalizer import MaterializationStrategy

        @il.asset(schema=ConformSchema, materialization_strategy=MaterializationStrategy.RECONCILE)
        def users() -> list[dict[str, Any]]:
            return [{"user_id": "1", "name": "a", "extra": True}]

        assert await users().run_async() == [{"user_id": 1, "name": "a"}]

    async def test_generator_with_schema_is_coerced(self):
        @il.asset(schema=ConformSchema)
        def users() -> Any:
            yield {"user_id": 1, "name": "a"}

        assert await users().run_async() == [{"user_id": 1, "name": "a"}]

    async def test_non_tabular_data_with_schema_fails(self):
        @il.asset(schema=ConformSchema)
        def users() -> Any:
            return "not tabular"

        with pytest.raises(AssetError, match="cannot[\\s\\S]*be checked"):
            await users().run_async()

    async def test_auto_without_schema_infers_effective_schema(self):
        @il.asset
        def users() -> list[dict[str, Any]]:
            return [{"user_id": 1, "name": "a"}]

        asset = users()
        await asset.run_async()
        assert asset._effective_schema is not None
        names = [s.name for s in asset._effective_schema.field_specs()]
        assert names == ["user_id", "name"]

    async def test_iocontext_carries_schema_to_destination(self):
        captured: dict[str, Any] = {}

        class CapturingDestination(il.Destination):
            def read(self, context: Any) -> Any:
                return None

            def write(self, context: Any, data: Any) -> None:
                captured["schema"] = context.schema

        @il.asset(schema=ConformSchema)
        def users() -> list[dict[str, Any]]:
            return [{"user_id": 1, "name": "a"}]

        asset = users(destinations=[CapturingDestination(id="cap")])
        await asset.materialize_async()
        assert captured["schema"] is ConformSchema

    async def test_iocontext_carries_inferred_schema_when_undeclared(self):
        captured: dict[str, Any] = {}

        class CapturingDestination(il.Destination):
            def read(self, context: Any) -> Any:
                return None

            def write(self, context: Any, data: Any) -> None:
                captured["schema"] = context.schema

        @il.asset
        def users() -> list[dict[str, Any]]:
            return [{"user_id": 1}]

        asset = users(destinations=[CapturingDestination(id="cap")])
        await asset.materialize_async()
        assert captured["schema"] is not None
        assert [s.name for s in captured["schema"].field_specs()] == ["user_id"]
