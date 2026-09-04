"""Tests for ``interloper.asset.base``."""

# Note: no ``from __future__ import annotations`` — ``Asset._infer_resource_types``
# reads parameter annotations via ``inspect.signature`` and needs them as real
# classes (not lazily-evaluated strings).

import asyncio
import datetime as dt
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, ClassVar

import pytest

import interloper as il
from interloper.asset.base import AssetDefinition
from interloper.component.base import Component
from interloper.errors import AssetError, DestinationError, PartitionError
from interloper.events import Event, EventBus, EventType
from interloper.partitioning.base import Partition, PartitionConfig, PartitionWindow
from interloper.partitioning.time import TimeGranularity, TimePartition, TimePartitionConfig, TimePartitionWindow
from interloper.runner.results import ExecutionStatus
from interloper.serializable import Spec

# -- Fixtures ------------------------------------------------------------------


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


# -- Identity and class metadata -----------------------------------------------


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

    def test_table_standalone_equals_key(self):
        assert FakeAsset().table == "fake_asset"

    def test_table_when_source_attached_uses_asset_table(self):
        source = FakeParentSource()
        asset = FakeSourceOwnedAsset()
        asset._source = source
        assert asset.table == source.asset_table(asset) == type(asset).key

    def test_data_default_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            FakeAsset().data()


# -- Definition metadata -------------------------------------------------------


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


# -- Resource type inference and runtime resolution ----------------------------


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


# -- Destination resolution and validation -------------------------------------


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


# -- Partitioning validation ---------------------------------------------------


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


class FakeAssetDaily(il.Asset):
    partitioning = TimePartitionConfig(column="date", allow_window=True)


class FakeAssetBounded(il.Asset):
    partitioning = TimePartitionConfig(column="date", allow_window=True, start=dt.date(2026, 1, 10))


class TestTimePartitioning:
    def test_matching_granularity_passes(self):
        FakeAssetDaily()._validate_partitioning(TimePartition(dt.date(2026, 1, 1)))

    def test_mismatched_partition_granularity_raises(self):
        partition = TimePartition(dt.date(2026, 1, 1), TimeGranularity.MONTH)
        with pytest.raises(PartitionError, match="partitioned by day"):
            FakeAssetDaily()._validate_partitioning(partition)

    def test_mismatched_window_granularity_raises(self):
        window = TimePartitionWindow(
            start=dt.date(2026, 1, 1), end=dt.date(2026, 3, 1), granularity=TimeGranularity.MONTH
        )
        with pytest.raises(PartitionError, match="partitioned by day"):
            FakeAssetDaily()._validate_partitioning(window)

    def test_partition_on_the_start_bound_passes(self):
        FakeAssetBounded()._validate_partitioning(TimePartition(dt.date(2026, 1, 10)))

    def test_partition_before_the_start_bound_raises(self):
        with pytest.raises(PartitionError, match="no data before 2026-01-10"):
            FakeAssetBounded()._validate_partitioning(TimePartition(dt.date(2026, 1, 9)))

    def test_window_reaching_before_the_start_bound_raises(self):
        window = TimePartitionWindow(start=dt.date(2026, 1, 5), end=dt.date(2026, 1, 20))
        with pytest.raises(PartitionError, match="no data before 2026-01-10"):
            FakeAssetBounded()._validate_partitioning(window)

    def test_window_within_the_start_bound_passes(self):
        window = TimePartitionWindow(start=dt.date(2026, 1, 10), end=dt.date(2026, 1, 20))
        FakeAssetBounded()._validate_partitioning(window)

    def test_unbounded_asset_accepts_any_partition(self):
        FakeAssetDaily()._validate_partitioning(TimePartition(dt.date(1999, 1, 1)))

    def test_a_non_time_partition_is_rejected(self):
        # Only time partitions carry a granularity, so anything else would
        # reach the asset as a scope that cannot answer `granularity`/`bounds`.
        with pytest.raises(PartitionError, match="is time-partitioned, but the run was given a FakePartition"):
            FakeAssetDaily()._validate_partitioning(FakePartition(value="2026-01-01"))

    def test_a_non_time_window_is_rejected(self):
        window = FakePartitionWindow(start="2026-01-01", end="2026-01-03")
        with pytest.raises(PartitionError, match="is time-partitioned"):
            FakeAssetDaily()._validate_partitioning(window)


# -- __call__ reconfiguration --------------------------------------------------


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


# -- Serialization round-trip --------------------------------------------------


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
        restored = Spec.model_validate_json(spec_json).reconstruct()

        assert isinstance(restored, FakeAsset)
        assert restored.dataset == "ds"
        assert isinstance(restored.destinations, list)
        assert isinstance(restored.resources["config"], FakeResource)


# -- Destination write — empty-result handling ---------------------------------


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
        assert warnings[0].metadata.get("component_id") == asset.id

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


# -- Conform (schema enforcement decoupled from normalizer) --------------------


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

    async def test_schema_conforms_without_normalizer(self):
        @il.asset(schema=ConformSchema)
        def users() -> list[dict[str, Any]]:
            return [{"user_id": 1, "name": "a"}]

        assert await users().run_async() == [{"user_id": 1, "name": "a"}]

    async def test_auto_with_schema_coerces_types(self):
        # AUTO reconciles by default: an int id against a str field is cast,
        # not rejected.
        @il.asset(schema=ConformSchema)
        def users() -> list[dict[str, Any]]:
            return [{"user_id": "1", "name": 42}]

        assert await users().run_async() == [{"user_id": 1, "name": "42"}]

    async def test_uncoercible_data_fails_fast(self):
        from interloper.errors import SchemaError

        @il.asset(schema=ConformSchema)
        def users() -> list[dict[str, Any]]:
            return [{"user_id": "not-an-int", "name": "a"}]

        with pytest.raises(SchemaError):
            await users().run_async()

    async def test_dataframe_reconciled_without_normalizer(self):
        pd = pytest.importorskip("pandas")

        @il.asset(schema=StrictConformSchema)
        def users() -> Any:
            return pd.DataFrame([{"userId": 1, "Name": "a"}])  # wrong casing -> extras dropped, nullables filled

        result = await users().run_async()
        assert list(result.columns) == ["user_id", "name"]
        assert result["user_id"].isna().all()

    async def test_strict_rejects_mismatched_dataframe(self):
        pd = pytest.importorskip("pandas")
        from interloper.errors import SchemaError
        from interloper.normalizer import MaterializationStrategy

        @il.asset(schema=StrictConformSchema, materialization_strategy=MaterializationStrategy.STRICT)
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


class TestAssetIdentity:
    """The canonical reading of bare/qualified dependency keys."""

    def test_bare_key_scopes_to_own_source(self):
        assert il.AssetIdentity.resolve("a", own_source_key="s") == il.AssetIdentity("s", "a")

    def test_bare_key_on_standalone_declarer_has_no_source(self):
        assert il.AssetIdentity.resolve("a") == il.AssetIdentity(None, "a")

    def test_qualified_key_names_the_source_explicitly(self):
        assert il.AssetIdentity.resolve("other.a", own_source_key="s") == il.AssetIdentity("other", "a")

    def test_qualified_key_splits_on_the_first_dot(self):
        assert il.AssetIdentity.resolve("s.a.b") == il.AssetIdentity("s", "a.b")

    def test_str_renders_the_qualified_form(self):
        assert str(il.AssetIdentity("s", "a")) == "s.a"
        assert str(il.AssetIdentity(None, "a")) == "a"

    def test_asset_identity_property(self):
        @il.asset
        def standalone() -> str:
            return ""

        assert standalone().identity == il.AssetIdentity(None, "standalone")


# -- Partition row counts ------------------------------------------------------


class TestPartitionRowCounts:
    """Row counts delegated to the asset's first resolved destination."""

    def test_delegates_to_the_destination(self):
        il.MemoryDestination.clear()
        mem = il.MemoryDestination()
        asset = FakeAssetDaily(id="daily", destinations=[mem])
        mem.write(
            il.IOContext(asset=asset, partition_or_window=TimePartition(dt.date(2026, 1, 1))),
            [{"date": "2026-01-01"}, {"date": "2026-01-01"}],
        )

        assert asset.partition_row_counts() == {"2026-01-01": 2}

    def test_an_unpartitioned_asset_is_rejected(self):
        asset = FakeAsset(destinations=[FakeDestination()])

        with pytest.raises(PartitionError, match="is not partitioned"):
            asset.partition_row_counts()

    def test_no_destination_is_rejected(self):
        with pytest.raises(AssetError, match="No destinations found"):
            FakeAssetDaily().partition_row_counts()


# -- Dependency resolution -----------------------------------------------------


class DependentSource(il.Source):
    """``consumer`` requires ``producer``; ``tolerant`` only prefers it."""

    class Producer(il.Asset):
        """Returns one row."""

        def data(self) -> Any:
            return [{"x": 1}]

    class Consumer(il.Asset):
        """Requires the upstream's rows."""

        requires: ClassVar[dict[str, str]] = {"producer": "producer"}

        def data(self, producer: Any) -> Any:
            return producer

    class Tolerant(il.Asset):
        """Runs with or without the upstream's rows."""

        optional_requires: ClassVar[dict[str, str]] = {"producer": "producer"}

        def data(self, producer: Any) -> Any:
            return producer or [{"fallback": True}]


class TestDependencyResolution:
    """What ``_build_kwargs`` does about upstream data."""

    async def test_a_required_dependency_without_a_dag_is_an_actionable_error(self):
        il.MemoryDestination.clear()
        source = DependentSource(destinations=[il.MemoryDestination()])
        consumer = next(asset for asset in source.assets if asset.key == "consumer")

        with pytest.raises(AssetError, match="has dependencies but no DAG provided"):
            await consumer.run_async()

    async def test_an_optional_dependency_without_a_dag_resolves_to_none(self):
        il.MemoryDestination.clear()
        source = DependentSource(destinations=[il.MemoryDestination()])
        tolerant = next(asset for asset in source.assets if asset.key == "tolerant")

        assert await tolerant.run_async() == [{"fallback": True}]

    async def test_a_required_dependency_is_read_from_the_upstream_destination(self):
        il.MemoryDestination.clear()
        dag = il.DAG(DependentSource(select=["producer", "consumer"], destinations=[il.MemoryDestination()]))

        result = await il.AsyncRunner(max_workers=1).run(dag)

        assert result.status is ExecutionStatus.COMPLETED

    async def test_an_optional_dependency_that_cannot_be_read_resolves_to_none(self):
        # The upstream never ran, so its destination has nothing to return.
        il.MemoryDestination.clear()
        source = DependentSource(select=["tolerant"], destinations=[il.MemoryDestination()])
        dag = il.DAG(source)
        tolerant = next(o for o in dag.operations if o.key == "tolerant")

        assert await tolerant.run_async(dag=dag) == [{"fallback": True}]  # ty: ignore[unresolved-attribute]


class TestUpstreamReadFailures:
    """Reading an upstream's data is where a run most often breaks."""

    async def test_a_missing_upstream_destination_is_named(self):
        il.MemoryDestination.clear()
        source = DependentSource(select=["producer", "consumer"])
        dag = il.DAG(source)
        consumer = next(o for o in dag.operations if o.key == "consumer")

        with pytest.raises(AssetError, match="No destination found for upstream asset 'producer'"):
            await consumer.run_async(dag=dag)  # ty: ignore[unresolved-attribute]

    async def test_a_failing_read_is_wrapped_and_reported(self):
        il.MemoryDestination.clear()

        class BrokenReadDestination(il.Destination):
            """Destination whose reads always fail."""

            def read(self, context: Any) -> Any:
                """Fail every read.

                Args:
                    context: Ignored IO context.

                Raises:
                    RuntimeError: Always.
                """
                raise RuntimeError("backend down")

            def write(self, context: Any, data: Any) -> None:
                """Accept and drop the data.

                Args:
                    context: Ignored IO context.
                    data: Ignored payload.
                """

        source = DependentSource(select=["producer", "consumer"], destinations=[BrokenReadDestination()])
        dag = il.DAG(source)
        consumer = next(o for o in dag.operations if o.key == "consumer")
        captured: list[Event] = []
        EventBus.subscribe(captured.append)
        try:
            with pytest.raises(AssetError, match="Failed to load data from upstream asset 'producer'"):
                await consumer.run_async(dag=dag)  # ty: ignore[unresolved-attribute]
            EventBus.flush(timeout=5.0)
        finally:
            EventBus.unsubscribe(captured.append)

        failures = [e for e in captured if e.type is EventType.DEST_READ_FAILED]
        assert len(failures) == 1
        assert "backend down" in failures[0].metadata["error"]
        assert failures[0].metadata["traceback"]


class TestDestinationWriteFailures:
    """A write failure is reported before it propagates."""

    async def test_a_failing_write_emits_the_failure_event(self):
        il.MemoryDestination.clear()

        class BrokenWriteDestination(il.Destination):
            """Destination whose writes always fail."""

            def read(self, context: Any) -> Any:  # pragma: no cover - not exercised
                """Unused.

                Args:
                    context: Ignored IO context.

                Returns:
                    Nothing.
                """
                return None

            def write(self, context: Any, data: Any) -> None:
                """Fail every write.

                Args:
                    context: Ignored IO context.
                    data: Ignored payload.

                Raises:
                    RuntimeError: Always.
                """
                raise RuntimeError("disk full")

        @il.asset()
        def rows() -> list[dict[str, Any]]:
            return [{"a": 1}]

        asset = rows(id="rows", destinations=[BrokenWriteDestination()])
        captured: list[Event] = []
        EventBus.subscribe(captured.append)
        try:
            with pytest.raises(RuntimeError, match="disk full"):
                await asset.materialize_async()
            EventBus.flush(timeout=5.0)
        finally:
            EventBus.unsubscribe(captured.append)

        failures = [e for e in captured if e.type is EventType.DEST_WRITE_FAILED]
        assert len(failures) == 1
        assert "disk full" in failures[0].metadata["error"]


class TestNonMaterializableAssets:
    """Read-only hydration of an upstream dependency."""

    async def test_materialize_returns_nothing(self):
        il.MemoryDestination.clear()
        asset = FakeAsset(destinations=[il.MemoryDestination()], materializable=False)

        assert await asset.materialize_async() is None


# -- Conform edge cases --------------------------------------------------------


class TestConformEdgeCases:
    """What ``_normalize_and_conform`` does with data the conformer cannot shape."""

    async def test_non_tabular_data_without_a_schema_passes_through(self):
        # Arbitrary objects bound for a FileDestination have no contract to
        # check against, so they must reach the destination untouched.
        payload = object()

        @il.asset()
        def opaque() -> Any:
            return payload

        asset = opaque(id="opaque")

        assert asset._normalize_and_conform(payload) is payload
        assert asset._effective_schema is None

    def test_non_tabular_data_with_a_schema_is_an_actionable_error(self):
        @il.asset(schema=ConformSchema)
        def opaque() -> Any:
            return object()

        asset = opaque(id="opaque")

        with pytest.raises(AssetError, match="declares a schema but returned data that cannot be checked"):
            asset._normalize_and_conform(object())

    def test_strict_returns_the_validated_data_unchanged(self):
        from interloper.normalizer import MaterializationStrategy

        @il.asset(schema=StrictConformSchema, materialization_strategy=MaterializationStrategy.STRICT)
        def rows() -> list[dict[str, Any]]:
            return [{"user_id": 1, "name": "x"}]

        asset = rows(id="rows")
        data = [{"user_id": 1, "name": "x"}]

        assert asset._normalize_and_conform(data) == data
        assert asset._effective_schema is StrictConformSchema

    def test_failed_inference_leaves_no_effective_schema(self, monkeypatch: pytest.MonkeyPatch):
        # Inference is best-effort metadata; a conformer that cannot infer
        # must not fail the materialization.
        from interloper.conformer.base import RowsConformer

        @il.asset()
        def rows() -> list[dict[str, Any]]:
            return [{"a": 1}]

        asset = rows(id="rows")
        monkeypatch.setattr(
            RowsConformer, "infer", lambda self, data: (_ for _ in ()).throw(RuntimeError("cannot infer"))
        )

        assert asset._normalize_and_conform([{"a": 1}]) == [{"a": 1}]
        assert asset._effective_schema is None


# -- Reconfiguration coverage --------------------------------------------------


class TestReconfigurationFields:
    """Every ``__call__`` override lands on the copy."""

    def test_dataset_and_strategy_are_overridable(self):
        asset = FakeAsset()

        from interloper.normalizer import MaterializationStrategy

        reconfigured = asset(
            dataset="analytics",
            materialization_strategy=MaterializationStrategy.STRICT,
        )

        assert reconfigured.dataset == "analytics"
        assert reconfigured.materialization_strategy is MaterializationStrategy.STRICT
        assert asset.dataset != "analytics"

    def test_a_single_destination_is_wrapped_in_a_list(self):
        destination = FakeDestination()

        reconfigured = FakeAsset()(destinations=destination)

        assert reconfigured.destinations == [destination]

    def test_destinations_none_becomes_an_empty_list(self):
        assert FakeAsset(destinations=None).destinations == []  # ty: ignore[invalid-argument-type]


# -- Time-partition scope validation -------------------------------------------


class TestTimePartitionScope:
    """``_validate_time_partitioning`` guards the scope's shape."""

    def test_no_scope_is_accepted(self):
        asset = FakeAssetDaily()

        asset._validate_time_partitioning(asset.partitioning, None)

    def test_a_non_time_partition_is_rejected(self):
        asset = FakeAssetDaily()

        with pytest.raises(PartitionError, match="is time-partitioned, but the run was given a FakePartition"):
            asset._validate_time_partitioning(asset.partitioning, FakePartition("x"))
