"""Tests for ``interloper.asset.base``."""

# Note: no ``from __future__ import annotations``. ``Asset._collect`` reads the
# ``data()`` parameter annotations to infer relations and needs them as real
# classes, not lazily-evaluated strings a locally-defined fixture would leave
# unresolvable.

import asyncio
import datetime as dt
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, ClassVar

import pytest
from pydantic import ValidationError

import interloper as il
from interloper.asset.base import AssetDefinition
from interloper.component.base import Component
from interloper.component.relation import ComponentIdentity
from interloper.dag import DAG
from interloper.errors import AssetError, ConfigError, DestinationError, PartitionError
from interloper.events import Event, EventBus, EventType
from interloper.partitioning.base import Partition, PartitionConfig, PartitionWindow
from interloper.partitioning.time import TimeGranularity, TimePartition, TimePartitionConfig, TimePartitionWindow
from interloper.runner.results import ExecutionStatus
from interloper.serializable import Spec

# -- Fixtures ------------------------------------------------------------------

PARTITION = TimePartitionConfig(column="date")


class Conn(il.Connection):
    """Connection fixture; the required secret is read from the environment, so the read is what fails."""

    api_secret: str = il.SecretField()


class Cfg(il.Config):
    """Config fixture; every field is defaulted, so its relation fills itself."""

    threshold: int = il.InputField(default=1)


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


class NeedyDestination(il.Destination):
    """Destination fixture with a required field, so its relation never fills itself."""

    bucket: str

    def read(self, context: Any) -> Any:  # pragma: no cover - not exercised
        return None

    def write(self, context: Any, data: Any) -> None:  # pragma: no cover - not exercised
        pass


class FakeAsset(il.Asset):
    """Plain asset fixture."""


class FakeOtherAsset(il.Asset):
    """Second asset class used for subclass-identity tests."""


class FakeAssetWithConfig(il.Asset):
    """Asset whose ``data()`` signature declares a config that fills itself."""

    def data(self, config: Cfg) -> Any:  # pragma: no cover
        return None


class FakeParentSource(il.Source):
    """Minimal source fixture used as the parent of source-owned assets."""

    class FakeSourceOwnedAsset(il.Asset):
        """Asset owned through the class body, bypassing the ``@source`` decorator."""


FakeSourceOwnedAsset = FakeParentSource.FakeSourceOwnedAsset


@il.source
class FbLike(il.Source):
    """Provider fixture: one stamped ``campaigns`` asset."""

    @il.asset(partitioning=PARTITION)
    def campaigns(self, context: il.ExecutionContext) -> Any:
        """One row per partition.

        Args:
            context: The execution context carrying the partition.

        Returns:
            The single stamped row.
        """
        return [{"date": context.partition_date, "id": "fb"}]


@il.source
class TtLike(il.Source):
    """Second provider fixture: one stamped ``campaigns`` asset."""

    @il.asset(partitioning=PARTITION)
    def campaigns(self, context: il.ExecutionContext) -> Any:
        """One row per partition.

        Args:
            context: The execution context carrying the partition.

        Returns:
            The single stamped row.
        """
        return [{"date": context.partition_date, "id": "tt"}]


@dataclass(frozen=True)
class FakePartition(Partition):
    pass


@dataclass(frozen=True)
class FakePartitionWindow(PartitionWindow):
    def __iter__(self) -> Iterator[Partition]:  # noqa: D105 - protocol method
        yield FakePartition(self.start)  # pragma: no cover - not exercised


@pytest.fixture(autouse=True)
def _clear_memory_destination() -> None:
    """Empty the class-wide ``MemoryDestination`` storage before every test."""
    il.MemoryDestination.clear()


async def _capture_log_events(coro: Any) -> list[Event]:
    """Await *coro* and return the ``LOG`` events it emitted.

    Args:
        coro: The coroutine to await with a subscriber attached.

    Returns:
        The captured ``LOG`` events, in emission order.
    """
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
        # Format is "module:SourceName.AssetName": the colon marks the
        # module / attribute boundary explicitly.
        assert ":" in cp
        assert cp.endswith(":FakeParentSource.FakeSourceOwnedAsset")

    def test_path_on_standalone_instance_equals_classpath(self):
        asset = FakeAsset()
        assert asset.path() == FakeAsset.classpath()

    def test_path_on_source_owned_instance_equals_classpath(self):
        source = FakeParentSource()
        asset = FakeSourceOwnedAsset()
        asset.parent = source
        assert asset.path() == FakeSourceOwnedAsset.classpath()

    def test_source_property_none_for_standalone(self):
        assert FakeAsset().source is None

    def test_source_property_set_when_source_attached(self):
        source = FakeParentSource()
        asset = FakeSourceOwnedAsset()
        asset.parent = source
        assert asset.source is source

    def test_identity_is_the_component_identity(self):
        source = FakeParentSource()
        asset = FakeSourceOwnedAsset()
        asset.parent = source
        assert asset.identity == ComponentIdentity(FakeParentSource.key, FakeSourceOwnedAsset.key)
        assert FakeAsset().identity == ComponentIdentity(None, "fake_asset")

    def test_qualified_key_standalone(self):
        assert FakeAsset().qualified_key == "fake_asset"

    def test_qualified_key_when_source_attached(self):
        source = FakeParentSource()
        asset = FakeSourceOwnedAsset()
        asset.parent = source
        assert asset.qualified_key == f"{FakeParentSource.key}.{type(asset).key}"

    def test_table_standalone_equals_key(self):
        assert FakeAsset().table == "fake_asset"

    def test_table_when_source_attached_uses_asset_table(self):
        source = FakeParentSource()
        asset = FakeSourceOwnedAsset()
        asset.parent = source
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
        assert defn.relations["destinations"].many is True
        assert defn.relations["destinations"].keys == []
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

    def test_definition_publishes_the_inferred_relations(self):
        defn = FakeAssetWithConfig.definition()
        assert defn.relations["config"].key == Cfg.key
        assert defn.relations["config"].kind == "config"

    def test_definition_includes_asset_schema_when_set(self):
        class FakeSchema(il.Schema):
            value: str

        class FakeAssetWithSchema(il.Asset):
            schema: ClassVar[type[il.Schema] | None] = FakeSchema

        defn = FakeAssetWithSchema.definition()
        assert isinstance(defn.asset_schema, dict)
        assert "properties" in defn.asset_schema

    def test_definition_includes_partitioning_when_set(self):
        class FakeAssetPartitioned(il.Asset):
            partitioning: ClassVar[PartitionConfig | None] = PartitionConfig(column="day")

        defn = FakeAssetPartitioned.definition()
        assert defn.partitioning == {"column": "day", "allow_window": False}


# -- Relation inference from data() --------------------------------------------


class TestInference:
    def test_component_annotation_becomes_relation(self):
        class A(il.Asset):
            def data(self, context: il.ExecutionContext, connection: Conn) -> Any:  # pragma: no cover
                return []

        assert A.relations["connection"].key == "conn"
        assert A.relations["connection"].kind == "connection"
        assert A.relations["connection"].optional is False

    def test_none_default_makes_optional(self):
        class A(il.Asset):
            def data(self, context: il.ExecutionContext, config: Cfg | None = None) -> Any:  # pragma: no cover
                return []

        assert A.relations["config"].optional is True

    def test_optional_annotation_makes_optional(self):
        # Written as a string on purpose: a lazily-evaluated annotation must
        # resolve the same way as a real class.
        class A(il.Asset):
            def data(self, context: il.ExecutionContext, config: "Cfg | None" = None) -> Any:  # pragma: no cover
                return []

        assert A.relations["config"].optional is True

    def test_upstream_annotation_is_a_bare_asset_key(self):
        class A(il.Asset):
            def data(self, context: il.ExecutionContext, orders: il.Upstream) -> Any:  # pragma: no cover
                return []

        relation = A.relations["orders"]
        assert (relation.kind, relation.key, relation.many) == ("asset", "orders", False)

    def test_list_upstream_is_many(self):
        class A(il.Asset):
            def data(self, context: il.ExecutionContext, campaigns: list[il.Upstream]) -> Any:  # pragma: no cover
                return []

        assert A.relations["campaigns"].many is True

    def test_reserved_parameters_declare_nothing(self):
        class A(il.Asset):
            def data(self, context: il.ExecutionContext, source: Any, **kwargs: Any) -> Any:  # pragma: no cover
                return []

        assert set(A.relations) == {"destinations"}

    def test_variadic_parameters_declare_nothing(self):
        class A(il.Asset):
            def data(self, context: il.ExecutionContext, *args: Any, **extra: Any) -> Any:  # pragma: no cover
                return []

        assert set(A.relations) == {"destinations"}

    def test_unresolvable_annotation_is_a_definition_error(self):
        with pytest.raises(TypeError, match="could not be resolved"):

            class A(il.Asset):
                def data(self, x: "Nope") -> Any:  # noqa: F821  # ty: ignore[unresolved-reference]
                    return []

    def test_unknown_parameter_is_a_definition_error(self):
        with pytest.raises(TypeError, match="nothing can fill it"):

            class A(il.Asset):
                def data(self, context: il.ExecutionContext, x: str) -> Any:  # pragma: no cover
                    return []

    def test_unannotated_parameter_is_a_definition_error(self):
        with pytest.raises(TypeError, match="nothing can fill it"):

            class A(il.Asset):
                def data(self, context: il.ExecutionContext, x) -> Any:  # pragma: no cover
                    return []

    def test_explicit_relations_win(self):
        class A(il.Asset):
            campaigns: list[il.Asset] = il.Relation("asset", "*.campaigns", many=True)

            def data(self, context: il.ExecutionContext, campaigns: list[il.Upstream]) -> Any:  # pragma: no cover
                return []

        assert A.relations["campaigns"].key == "*.campaigns"

    def test_the_decorator_infers_from_the_decorated_function(self):
        @il.asset
        def rows(context: il.ExecutionContext, connection: Conn) -> Any:  # pragma: no cover
            return []

        assert rows.relations["connection"].key == "conn"

    def test_nothing_is_inferred_without_a_data_override(self):
        assert set(FakeAsset.relations) == {"destinations"}


# -- Injection into data() -----------------------------------------------------


class TestInjection:
    def test_resource_and_upstream_injected(self):
        seen: dict[str, Any] = {}

        @il.source
        class Shop(il.Source):
            """Source whose ``revenue`` asset consumes its connection and its sibling."""

            connection: Conn

            @il.asset(partitioning=PARTITION)
            def orders(self, context: il.ExecutionContext) -> Any:
                """One order row per partition.

                Args:
                    context: The execution context carrying the partition.

                Returns:
                    The single stamped row.
                """
                return [{"date": context.partition_date, "id": "o1"}]

            @il.asset(partitioning=PARTITION)
            def revenue(self, context: il.ExecutionContext, connection: Conn, orders: il.Upstream) -> Any:
                """One row counting the upstream's rows.

                Args:
                    context: The execution context carrying the partition.
                    connection: The connection bound on the source.
                    orders: The upstream leg.

                Returns:
                    The single stamped row.
                """
                seen.update(connection=connection, orders=orders)
                return [{"date": context.partition_date, "n": len(orders.data or [])}]

        connection, memory = Conn(api_secret="s"), il.MemoryDestination()
        shop = Shop(connection=connection, destinations=[memory])  # ty: ignore[unknown-argument]

        result = DAG(shop).materialize(TimePartition(dt.date(2026, 9, 1)))

        assert result.status is ExecutionStatus.COMPLETED
        assert seen["connection"] is connection
        assert seen["orders"].asset is shop.orders
        assert [row["id"] for row in seen["orders"].data] == ["o1"]

    def test_many_receives_one_leg_per_bound_upstream(self):
        seen: dict[str, Any] = {}

        # The relation stays optional: nothing its own source holds can fill a
        # cross-source key, so it is bound once both providers exist.
        @il.source
        class Matcher(il.Source):
            """Source whose asset fans in every ``campaigns`` asset bound to it."""

            @il.asset(
                partitioning=PARTITION,
                relations={"campaigns": il.Relation("asset", "*.campaigns", many=True, optional=True)},
            )
            def matches(self, context: il.ExecutionContext, campaigns: list[il.Upstream]) -> Any:
                """No rows; the legs are what the test reads.

                Args:
                    context: The execution context carrying the partition.
                    campaigns: One leg per bound upstream.

                Returns:
                    No rows.
                """
                seen["legs"] = campaigns
                return []

        memory = il.MemoryDestination()
        fb, tt = FbLike(destinations=[memory]), TtLike(destinations=[memory])
        matcher = Matcher(destinations=[memory])
        matcher.matches.bind("campaigns", fb.campaigns, tt.campaigns)

        result = DAG(fb, tt, matcher).materialize(TimePartition(dt.date(2026, 9, 1)))

        assert result.status is ExecutionStatus.COMPLETED
        assert {leg.asset.id for leg in seen["legs"]} == {fb.campaigns.id, tt.campaigns.id}

    def test_missing_partition_gives_none_data(self):
        # The upstream is bound but never materialised, so its leg arrives as
        # Upstream(asset, data=None) and a LOG warning names it; the asset still runs.
        seen: dict[str, Any] = {}

        @il.asset(partitioning=PARTITION, relations={"campaigns": il.Relation("asset", "*.campaigns")})
        def lonely(context: il.ExecutionContext, campaigns: il.Upstream) -> Any:
            """No rows; the leg is what the test reads.

            Args:
                context: The execution context carrying the partition.
                campaigns: The upstream leg.

            Returns:
                No rows.
            """
            seen["leg"] = campaigns
            return []

        memory = il.MemoryDestination()
        fb = FbLike(destinations=[memory])
        asset = lonely(destinations=[memory], campaigns=fb.campaigns)  # ty: ignore[unknown-argument]
        warnings_seen: list[Event] = []

        def handler(event: Event) -> None:
            if event.metadata.get("level") == "WARNING":
                warnings_seen.append(event)

        EventBus.subscribe(handler)
        try:
            result = DAG(fb(materializable=False), asset).materialize(TimePartition(dt.date(2026, 9, 1)))
            EventBus.flush(timeout=5.0)
        finally:
            EventBus.unsubscribe(handler)

        assert result.status is ExecutionStatus.COMPLETED
        # The leg carries the DAG's own node, the read-only copy of the binding.
        assert seen["leg"].asset.id == fb.campaigns.id
        assert seen["leg"].asset.materializable is False
        assert seen["leg"].data is None
        assert any("found no data in upstream" in e.metadata.get("message", "") for e in warnings_seen)

    def test_a_fallback_is_instantiated_at_read_time(self):
        seen: dict[str, Any] = {}

        @il.asset
        def rows(config: Cfg) -> Any:
            """No rows; the injected config is what the test reads.

            Args:
                config: The config that fills itself.

            Returns:
                No rows.
            """
            seen["config"] = config
            return []

        rows(id="rows").run()

        assert isinstance(seen["config"], Cfg)

    def test_an_optional_relation_nothing_can_fill_is_none(self):
        seen: dict[str, Any] = {}

        @il.asset
        def rows(store: NeedyDestination | None = None) -> Any:
            """No rows; the injected destination is what the test reads.

            Args:
                store: The optional destination, unfillable without a binding.

            Returns:
                No rows.
            """
            seen["store"] = store
            return []

        rows(id="rows").run()

        assert seen["store"] is None

    def test_an_unbound_connection_fails_on_the_read(self, monkeypatch):
        # A connection's required fields come from the environment, so the
        # relation fills itself and the missing credential surfaces here.
        monkeypatch.delenv("api_secret", raising=False)
        monkeypatch.delenv("API_SECRET", raising=False)

        @il.asset
        def rows(connection: Conn) -> Any:
            """No rows; the read never gets this far.

            Args:
                connection: The connection the relation fills itself with.

            Returns:
                No rows.
            """
            return []

        asset = rows(id="rows")
        with pytest.raises(ValidationError):
            asset.run()

    def test_the_source_is_injected(self):
        seen: dict[str, Any] = {}

        @il.source
        class Holder(il.Source):
            """Source whose asset reads its parent through the ``source`` parameter."""

            @il.asset
            def rows(self, source: Any) -> Any:
                """No rows; the injected source is what the test reads.

                Args:
                    source: The owning source.

                Returns:
                    No rows.
                """
                seen["source"] = source
                return []

        holder = Holder()
        holder.rows.run()

        assert seen["source"] is holder

    async def test_upstreams_without_a_dag_are_an_actionable_error(self):
        @il.asset(relations={"campaigns": il.Relation("asset", "*.campaigns")})
        def consumer(campaigns: il.Upstream) -> Any:  # pragma: no cover - never reached
            return []

        fb = FbLike(destinations=[il.MemoryDestination()])
        asset = consumer(destinations=[il.MemoryDestination()], campaigns=fb.campaigns)  # ty: ignore[unknown-argument]

        with pytest.raises(AssetError, match="has upstreams but no DAG provided"):
            await asset.run_async()


# -- Destination resolution and validation -------------------------------------


class TestDestinations:
    def test_the_asset_binds_its_own_destinations(self):
        destination = FakeDestination()
        assert FakeAsset(destinations=[destination]).destinations == [destination]

    def test_a_single_destination_is_wrapped_in_a_list(self):
        destination = FakeDestination()
        assert FakeAsset(destinations=destination).destinations == [destination]  # ty: ignore[invalid-argument-type]

    def test_several_destinations_are_kept_in_order(self):
        destinations: list[il.Destination] = [FakeDestination(), FakeOtherDestination()]
        assert FakeAsset(destinations=destinations).destinations == destinations

    def test_the_source_trickles_its_destinations_down(self):
        destination = FakeDestination()
        source = FakeParentSource(destinations=[destination])
        assert source.assets[0].destinations == [destination]

    def test_nothing_configured_leaves_the_relation_empty(self):
        assert FakeAsset().destinations == []

    def test_validate_destination_is_a_noop_when_no_key_is_declared(self):
        FakeAsset()._validate_destination(FakeDestination())

    def test_validate_destination_accepts_a_declared_key(self):
        @il.asset(relations={"destinations": [FakeDestination]})
        def narrowed() -> Any:  # pragma: no cover - not exercised
            return []

        narrowed()._validate_destination(FakeDestination())

    def test_validate_destination_rejects_an_undeclared_key(self):
        @il.asset(relations={"destinations": [FakeDestination]})
        def narrowed() -> Any:  # pragma: no cover - not exercised
            return []

        with pytest.raises(DestinationError):
            narrowed()._validate_destination(FakeOtherDestination())

    def test_binding_an_undeclared_destination_is_rejected(self):
        @il.asset(relations={"destinations": [FakeDestination]})
        def narrowed() -> Any:  # pragma: no cover - not exercised
            return []

        with pytest.raises(ConfigError):
            narrowed(destinations=[FakeOtherDestination()])


class TestReadDestination:
    def test_default_destination_key_selects_the_read_destination(self):
        first, second = il.MemoryDestination(), il.CSVDestination(base_path="/tmp/unused")
        asset = FakeAsset(destinations=[first, second], default_destination_key=second.key)
        assert asset._read_destination() is second

    def test_falls_back_to_the_first_destination(self):
        first, second = il.MemoryDestination(), il.CSVDestination(base_path="/tmp/unused")
        asset = FakeAsset(destinations=[first, second])
        assert asset._read_destination() is first

    def test_raises_without_destinations(self):
        with pytest.raises(AssetError, match="No destination found"):
            FakeAsset()._read_destination()


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

    def test_dataset_and_strategy_are_overridable(self):
        from interloper.normalizer import MaterializationStrategy

        asset = FakeAsset()
        reconfigured = asset(dataset="analytics", materialization_strategy=MaterializationStrategy.STRICT)

        assert reconfigured.dataset == "analytics"
        assert reconfigured.materialization_strategy is MaterializationStrategy.STRICT
        assert asset.dataset != "analytics"

    def test_override_default_destination_key(self):
        assert FakeAsset()(default_destination_key="memory").default_destination_key == "memory"

    def test_normalizer_explicit_none_clears_normalizer(self):
        # The ``normalizer`` parameter uses a _UNSET sentinel so that
        # passing ``None`` explicitly means "clear it" (not "unchanged").
        reconfigured = FakeAsset()(normalizer=None)
        assert reconfigured.normalizer is None

    def test_omitted_fields_preserved(self):
        asset = FakeAsset(dataset="original", materializable=False)
        reconfigured = asset(dataset="updated")
        assert reconfigured.materializable is False

    def test_the_copy_keeps_the_originals_bindings(self):
        destination = FakeDestination()
        source = FakeParentSource()
        asset = FakeSourceOwnedAsset(destinations=[destination])
        asset.parent = source

        reconfigured = asset(materializable=False)

        assert reconfigured.destinations == [destination]
        assert reconfigured.parent is source

    def test_rebinding_a_relation_leaves_the_original_untouched(self):
        first, second = FakeDestination(), FakeOtherDestination()
        asset = FakeAsset(destinations=[first])

        reconfigured = asset(destinations=second)

        assert reconfigured.destinations == [second]
        assert asset.destinations == [first]

    def test_a_relation_set_to_none_is_cleared(self):
        asset = FakeAsset(destinations=[FakeDestination()])
        assert asset(destinations=None).destinations == []

    def test_an_unknown_keyword_is_rejected(self):
        with pytest.raises(TypeError, match="declares no relation"):
            FakeAsset()(nonsense=1)


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

    def test_asset_preserves_instance_id(self):
        asset = FakeAsset(id="fixed123")
        restored = Component.from_spec(asset.to_spec())
        assert restored.id == "fixed123"

    def test_source_owned_asset_roundtrip_preserves_subclass(self):
        source = FakeParentSource()
        asset = FakeSourceOwnedAsset(dataset="override", materializable=False)
        asset.parent = source

        restored = FakeSourceOwnedAsset.from_spec(asset.to_spec())
        assert isinstance(restored, FakeSourceOwnedAsset)
        assert restored.dataset == "override"
        assert restored.materializable is False

    def test_roundtrip_via_json_string(self):
        asset = FakeAsset(dataset="ds", default_destination_key="memory")
        spec_json = asset.to_spec().model_dump_json()
        restored = Spec.model_validate_json(spec_json).reconstruct()

        assert isinstance(restored, FakeAsset)
        assert restored.dataset == "ds"
        assert restored.default_destination_key == "memory"


# -- Destination write, empty-result handling ---------------------------------


class TestDestinationWrite:
    async def test_empty_result_skips_write_and_warns(self):
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


# -- Partition row counts ------------------------------------------------------


class TestPartitionRowCounts:
    """Row counts delegated to the asset's first resolved destination."""

    def test_delegates_to_the_destination(self):
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
        with pytest.raises(AssetError, match="No destination found"):
            FakeAssetDaily().partition_row_counts()


# -- Upstream reads ------------------------------------------------------------


class TestUpstreamReads:
    @staticmethod
    def _matcher() -> type[il.Asset]:
        """Build a matcher asset fanning in every ``campaigns`` asset bound to it.

        Returns:
            The asset class.
        """

        @il.asset(
            relations={"campaigns": il.Relation("asset", "*.campaigns", many=True)},
            partitioning=PARTITION,
        )
        def matches(context: il.ExecutionContext, campaigns: list[il.Upstream]) -> Any:
            rows = []
            for leg in campaigns:
                assert leg.asset.source is not None
                rows.append(
                    {
                        "date": context.partition_date,
                        "source": leg.asset.source.key,
                        "rows": len(leg.data) if leg.data is not None else None,
                    }
                )
            return rows

        return matches

    def test_many_slot_receives_one_upstream_per_leg(self):
        mem = il.MemoryDestination()
        one, two = FbLike(destinations=[mem]), TtLike(destinations=[mem])
        matcher = self._matcher()(destinations=[mem], campaigns=[one.campaigns, two.campaigns])  # ty: ignore[unknown-argument]
        partition = TimePartition(dt.date(2026, 1, 1))

        result = DAG(one, two, matcher).materialize(partition)

        assert result.status is ExecutionStatus.COMPLETED
        rows = mem.read(il.IOContext(asset=matcher, partition_or_window=partition))
        assert sorted(row["source"] for row in rows) == ["fb_like", "tt_like"]
        assert all(row["rows"] == 1 for row in rows)

    def test_missing_leg_arrives_as_none_with_a_warning(self):
        mem = il.MemoryDestination()
        one, two = FbLike(destinations=[mem]), TtLike(destinations=[mem])
        partition = TimePartition(dt.date(2026, 1, 1))
        DAG(one).materialize(partition)  # only provider one has data
        matcher = self._matcher()(destinations=[mem], campaigns=[one.campaigns, two.campaigns])  # ty: ignore[unknown-argument]
        warnings_seen: list[Event] = []

        def handler(event: Event) -> None:
            if event.metadata.get("level") == "WARNING":
                warnings_seen.append(event)

        EventBus.subscribe(handler)
        try:
            result = DAG(one(materializable=False), two(materializable=False), matcher).materialize(partition)
            EventBus.flush(timeout=5.0)
        finally:
            EventBus.unsubscribe(handler)

        assert result.status is ExecutionStatus.COMPLETED
        read = mem.read(il.IOContext(asset=matcher, partition_or_window=partition))
        rows = {row["source"]: row["rows"] for row in read}
        assert rows == {"fb_like": 1, "tt_like": None}
        assert any("found no data in upstream" in e.metadata.get("message", "") for e in warnings_seen)

    def test_other_read_errors_fail_the_asset(self):
        class Broken(il.Destination):
            """Destination whose reads always fail for a reason other than missing data."""

            def read(self, context: il.IOContext) -> Any:
                raise RuntimeError("boom")

            def write(self, context: il.IOContext, data: Any) -> None:
                return None

        mem = il.MemoryDestination()
        one = FbLike(destinations=[Broken()])
        matcher = self._matcher()(destinations=[mem], campaigns=[one.campaigns])  # ty: ignore[unknown-argument]
        partition = TimePartition(dt.date(2026, 1, 1))

        result = DAG(one(materializable=False), matcher).materialize(partition)

        assert result.status is ExecutionStatus.FAILED

    def test_optional_many_slot_with_nothing_bound_receives_an_empty_list(self):
        @il.asset(relations={"campaigns": il.Relation("asset", "*.campaigns", optional=True, many=True)})
        def lonely(campaigns: list[il.Upstream]) -> Any:
            return [{"n": len(campaigns)}]

        asset = lonely(destinations=[il.MemoryDestination()])
        assert asset.run(dag=DAG(asset)) == [{"n": 0}]

    def test_single_slot_receives_one_leg(self):
        mem = il.MemoryDestination()
        one = FbLike(destinations=[mem])
        partition = TimePartition(dt.date(2026, 1, 1))
        DAG(one).materialize(partition)

        @il.asset(relations={"c": il.Relation("asset", "fb_like.campaigns")}, partitioning=PARTITION)
        def single(context: il.ExecutionContext, c: il.Upstream) -> Any:
            return [{"date": context.partition_date, "ids": [row["id"] for row in c.data]}]

        asset = single(destinations=[mem], c=one.campaigns)  # ty: ignore[unknown-argument]
        DAG(one(materializable=False), asset).materialize(partition)

        assert mem.read(il.IOContext(asset=asset, partition_or_window=partition)) == [
            {"date": dt.date(2026, 1, 1), "ids": ["fb"]}
        ]

    def test_optional_single_slot_is_none_on_missing_data_and_fails_otherwise(self):
        mem = il.MemoryDestination()
        one = FbLike(destinations=[mem])

        @il.asset(
            relations={"c": il.Relation("asset", "fb_like.campaigns", optional=True)},
            partitioning=PARTITION,
        )
        def lenient(context: il.ExecutionContext, c: il.Upstream | None = None) -> Any:
            return [{"date": context.partition_date, "got": c is not None and c.data is not None}]

        asset = lenient(destinations=[mem], c=one.campaigns)  # ty: ignore[unknown-argument]
        partition = TimePartition(dt.date(2030, 5, 5))  # provider never ran for this day

        result = DAG(one(materializable=False), asset).materialize(partition)

        assert result.status is ExecutionStatus.COMPLETED
        assert mem.read(il.IOContext(asset=asset, partition_or_window=partition)) == [
            {"date": dt.date(2030, 5, 5), "got": False}
        ]

        class Broken(il.Destination):
            """Destination whose reads always fail for a reason other than missing data."""

            def read(self, context: il.IOContext) -> Any:
                raise RuntimeError("boom")

            def write(self, context: il.IOContext, data: Any) -> None:
                return None

        broken = FbLike(destinations=[Broken()])
        asset = lenient(destinations=[mem], c=broken.campaigns)  # ty: ignore[unknown-argument]
        assert DAG(broken(materializable=False), asset).materialize(partition).status is ExecutionStatus.FAILED

    def test_an_upstream_absent_from_the_dag_is_skipped_with_a_warning(self):
        mem = il.MemoryDestination()
        one = FbLike(destinations=[mem])

        @il.asset(relations={"c": il.Relation("asset", "fb_like.campaigns", optional=True)})
        def lenient(c: il.Upstream | None = None) -> Any:
            return [{"got": c is not None}]

        # A DAG pulls every bound upstream in read-only, so the only way to run
        # against one that lacks it is to bind after the graph was built.
        asset = lenient(destinations=[mem])
        dag = DAG(asset)
        asset.bind("c", one.campaigns)
        warnings_seen: list[Event] = []

        def handler(event: Event) -> None:
            if event.metadata.get("level") == "WARNING":
                warnings_seen.append(event)

        EventBus.subscribe(handler)
        try:
            assert asset.run(dag=dag) == [{"got": False}]
            EventBus.flush(timeout=5.0)
        finally:
            EventBus.unsubscribe(handler)

        assert any("is not in the DAG" in e.metadata.get("message", "") for e in warnings_seen)


class TestUpstreamReadFailures:
    """Reading an upstream's data is where a run most often breaks."""

    async def test_a_missing_upstream_destination_is_named(self):
        one = FbLike()

        @il.asset(relations={"c": il.Relation("asset", "fb_like.campaigns")}, partitioning=PARTITION)
        def consumer(context: il.ExecutionContext, c: il.Upstream) -> Any:  # pragma: no cover - never reached
            return []

        asset = consumer(destinations=[il.MemoryDestination()], c=one.campaigns)  # ty: ignore[unknown-argument]
        dag = DAG(one(materializable=False), asset)

        with pytest.raises(AssetError, match="No destination found for upstream asset 'campaigns'"):
            await asset.run_async(TimePartition(dt.date(2026, 1, 1)), dag=dag)

    async def test_a_failing_read_is_wrapped_and_reported(self):
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

        one = FbLike(destinations=[BrokenReadDestination()])

        @il.asset(relations={"c": il.Relation("asset", "fb_like.campaigns")}, partitioning=PARTITION)
        def consumer(context: il.ExecutionContext, c: il.Upstream) -> Any:  # pragma: no cover - never reached
            return []

        asset = consumer(destinations=[il.MemoryDestination()], c=one.campaigns)  # ty: ignore[unknown-argument]
        dag = DAG(one(materializable=False), asset)
        captured: list[Event] = []
        EventBus.subscribe(captured.append)
        try:
            with pytest.raises(AssetError, match="Failed to load data from upstream asset 'campaigns'"):
                await asset.run_async(TimePartition(dt.date(2026, 1, 1)), dag=dag)
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
