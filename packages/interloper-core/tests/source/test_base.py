"""Tests for ``interloper.source.base``."""

# Note: no ``from __future__ import annotations``: an annotation naming a
# component class declares a relation, and the collector needs it as a real
# class, not a lazy string.

from typing import Any

import pytest
from pydantic import ValidationError

import interloper as il
from interloper.asset.base import AssetDefinition
from interloper.component.base import Component
from interloper.normalizer import MaterializationStrategy, Normalizer
from interloper.serializable import Spec
from interloper.source.base import SourceDefinition

# -- Fixtures ------------------------------------------------------------------


class FakeConnection(il.Connection):
    """Connection fixture trickled from a source down to its assets.

    The token is required, and a connection reads it from the environment, so
    an unbound relation targeting it builds and fails on the read.
    """

    token: str = il.SecretField()


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
    """Source with two nested assets; the second names the first as its upstream."""

    class FakeFirst(il.Asset):
        """First asset (no upstream relations)."""

        def data(self) -> Any:  # pragma: no cover
            return None

    class FakeSecond(il.Asset):
        """Second asset naming its sibling explicitly."""

        fake_first: il.Asset = il.Relation("asset", "fake_first")

        def data(self, fake_first: il.Upstream) -> Any:  # pragma: no cover
            return None


class FakeDiscriminatedSource(il.Source):
    """Source whose asset tables carry the instance's account id."""

    account_id: str = il.InputField(default="", discriminator=True)

    class FakeDiscriminated(il.Asset):
        """Asset whose table name carries the source instance discriminator."""

        def data(self) -> Any:  # pragma: no cover
            return None


class Shop(il.Source):
    """Source whose assets take its connection and wire to each other."""

    connection: FakeConnection

    class Orders(il.Asset):
        """Upstream asset filled with the source's connection."""

        connection: FakeConnection = il.Relation(FakeConnection)

        def data(self, connection: FakeConnection) -> Any:  # pragma: no cover
            return []

    class Revenue(il.Asset):
        """Downstream asset naming its sibling by bare key."""

        orders: il.Asset = il.Relation("asset", "orders")

        def data(self, orders: il.Upstream) -> Any:  # pragma: no cover
            return []


class Finance(il.Source):
    """Source whose asset names an upstream owned by another source."""

    class Revenue(il.Asset):
        """Downstream asset naming a qualified, cross-source upstream.

        Left optional: nothing but the DAG can resolve a cross-source key, so
        this source must stay constructible with it unbound.
        """

        orders: il.Asset | None = il.Relation("asset", "shop.orders", optional=True)

        def data(self, orders: il.Upstream) -> Any:  # pragma: no cover
            return []


class FakeUnfillableSource(il.Source):
    """Source whose asset names a non-optional relation nothing can fill."""

    class FakeOrphan(il.Asset):
        """Asset naming a sibling its own source does not have.

        A bare key is source-local, so the source itself is the authority on
        whether anything can fill it and says so at construction; a qualified
        key would name another source's asset, which only a DAG can rule on.
        """

        orders: il.Asset = il.Relation("asset", "orders")

        def data(self, orders: il.Upstream) -> Any:  # pragma: no cover
            return []


# -- Identity and class metadata -----------------------------------------------


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

    def test_invalid_dataset_raises_at_init(self):
        # ``validate_key`` raises a ValueError, so pydantic wraps it at init time.
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="invalid"):
            FakeSource(dataset="bad dataset!")


# -- Relations -----------------------------------------------------------------


class TestSourceRelations:
    def test_anchor_declares_destinations(self):
        relation = il.Source.relations["destinations"]
        assert (relation.kind, relation.many, relation.optional) == ("destination", True, True)

    def test_decorator_destinations_narrows_keys(self):
        @il.source(relations={"destinations": [il.MemoryDestination]})
        class Narrow(il.Source):
            pass

        assert Narrow.relations["destinations"].keys == [il.MemoryDestination.key]

    def test_narrowed_destinations_reject_another_key(self):
        @il.source(relations={"destinations": [il.MemoryDestination]})
        class Narrow(il.Source):
            pass

        from interloper.errors import ConfigError

        with pytest.raises(ConfigError, match="does not accept"):
            Narrow(destinations=[FakeDestination()])

    def test_destinations_bind_from_the_constructor(self):
        destination = FakeDestination()
        assert FakeSource(destinations=[destination]).destinations == [destination]

    def test_a_single_destination_is_accepted(self):
        destination = FakeDestination()
        assert FakeSource(destinations=destination).destinations == [destination]  # ty: ignore[invalid-argument-type]

    def test_annotation_declares_a_relation(self):
        relation = Shop.relations["connection"]
        assert (relation.kind, relation.key, relation.target) == ("connection", "fake_connection", FakeConnection)

    def test_connection_trickles_to_assets(self):
        connection = FakeConnection(token="secret")
        source = Shop(connection=connection)
        assert source.orders.connection is connection  # ty: ignore[unresolved-attribute]

    def test_missing_connection_builds_and_fails_on_the_read(self, monkeypatch):
        # A connection reads its credentials from the environment, so an
        # unbound one is a read-time failure, not a build-time one.
        monkeypatch.delenv("token", raising=False)
        monkeypatch.delenv("TOKEN", raising=False)
        shop = Shop()  # ty: ignore[missing-argument]

        assert shop.bound("connection") is None
        with pytest.raises(ValidationError):
            shop.resolve("connection")
        with pytest.raises(ValidationError):
            shop.orders.resolve("connection")

    def test_asset_relation_nothing_can_fill_raises_naming_asset_and_relation(self):
        from interloper.errors import ConfigError

        with pytest.raises(ConfigError) as excinfo:
            il.DAG(FakeUnfillableSource())
        assert "FakeOrphan" in str(excinfo.value)
        assert "orders" in str(excinfo.value)

    def test_destination_bound_after_construction_still_trickles(self):
        class FakeConnectedDestination(FakeDestination):
            connection: FakeConnection | None = il.Relation(FakeConnection, optional=True)

        connection = FakeConnection(token="secret")
        destination = FakeConnectedDestination()
        source = Shop(connection=connection)
        source.bind("destinations", destination)
        assert destination.connection is connection

    def test_destination_assigned_after_construction_still_trickles(self):
        destination = FakeDestination()
        source = Shop(connection=FakeConnection(token="secret"))
        source.destinations = [destination]
        assert source.orders.destinations == [destination]

    def test_sibling_bindings_and_bind(self):
        assert Shop.sibling_bindings() == {"revenue": {"orders": "orders"}}
        source = Shop(connection=FakeConnection(token="secret"))
        assert source.revenue.orders is source.orders  # ty: ignore[unresolved-attribute]

    def test_cross_source_key_stays_unbound(self):
        assert Finance.sibling_bindings() == {}
        source = Finance()
        assert source.revenue.bound("orders") is None

    def test_relations_reach_the_definition(self):
        assert Shop.definition().relations["connection"].target is FakeConnection


# -- Per-instance table names ----------------------------------------------------


class TestAssetTable:
    def test_table_defaults_to_asset_key(self):
        source = FakeSourceWithAssets()
        assert [a.table for a in source.assets] == ["fake_first", "fake_second"]

    def test_discriminator_suffixes_table(self):
        source = FakeDiscriminatedSource(account_id="123")
        (asset,) = source.assets
        assert source.asset_table(asset) == "fake_discriminated__123"
        assert asset.table == "fake_discriminated__123"
        # The logical identity is untouched — only the physical name varies.
        assert type(asset).key == "fake_discriminated"
        assert asset.dataset == "fake_discriminated_source"

    def test_asset_table_is_sanitized(self):
        source = FakeDiscriminatedSource(account_id="act_123-DE")
        (asset,) = source.assets
        assert asset.table == "fake_discriminated__act_123_de"

    def test_default_asset_table_is_the_asset_key(self):
        source = FakeDiscriminatedSource()
        (asset,) = source.assets
        assert asset.table == "fake_discriminated"

    def test_table_survives_spec_round_trip(self):
        source = FakeDiscriminatedSource(account_id="123")
        restored = FakeDiscriminatedSource.from_spec(source.to_spec())
        assert [a.table for a in restored.assets] == ["fake_discriminated__123"]

    def test_asset_table_override_takes_full_control(self):
        class FakeOverridingSource(FakeDiscriminatedSource):
            def asset_table(self, asset: il.Asset) -> str:
                return f"custom__{asset.key}"

        (asset,) = FakeOverridingSource(account_id="123").assets
        assert asset.table == "custom__fake_discriminated"

    def test_invalid_asset_table_raises_at_init(self):
        # The composed name is validated during ``_resolve``; ``validate_key``
        # raises a ValueError, so pydantic wraps it at init time.
        from pydantic import ValidationError

        class FakeBadTableSource(FakeDiscriminatedSource):
            def asset_table(self, asset: il.Asset) -> str:
                """Return a name that sanitizes to an invalid identifier."""
                return f"123_{asset.key}"

        with pytest.raises(ValidationError, match="invalid"):
            FakeBadTableSource()


# -- Definition metadata -------------------------------------------------------


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
        assert defn.relations["destinations"].kind == "destination"
        assert defn.relations["destinations"].keys == []

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


# -- Asset collection and lookup -----------------------------------------------


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

    def test_assets_are_parented_by_their_source(self):
        source = FakeSourceWithAssets()
        assert all(a.parent is source for a in source.assets)
        assert source.fake_first.qualified_key == "fake_source_with_assets.fake_first"


# -- Trickle-down resolution ---------------------------------------------------


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

    def test_trickles_materialization_strategy_when_asset_is_on_the_default(self):
        class FakeTrickleSource(il.Source):
            class FakeChild(il.Asset):
                pass

        source = FakeTrickleSource(materialization_strategy=MaterializationStrategy.STRICT)
        assert source.assets[0].materialization_strategy == MaterializationStrategy.STRICT

    def test_preserves_asset_own_materialization_strategy(self):
        class FakeTrickleSource(il.Source):
            class FakeChild(il.Asset):
                materialization_strategy: MaterializationStrategy = MaterializationStrategy.STRICT

        source = FakeTrickleSource(materialization_strategy=MaterializationStrategy.RECONCILE)
        assert source.assets[0].materialization_strategy == MaterializationStrategy.STRICT

    def test_default_strategy_is_reconcile_and_overrides_nothing(self):
        # The RECONCILE default pre-fills the UI select; like the old None
        # default it must leave every asset's own strategy alone.
        class FakeTrickleSource(il.Source):
            class DefaultChild(il.Asset):
                pass

            class StrictChild(il.Asset):
                materialization_strategy: MaterializationStrategy = MaterializationStrategy.STRICT

        source = FakeTrickleSource()
        assert source.materialization_strategy == MaterializationStrategy.RECONCILE
        by_key = {type(a).key: a.materialization_strategy for a in source.assets}
        assert by_key["default_child"] == MaterializationStrategy.RECONCILE
        assert by_key["strict_child"] == MaterializationStrategy.STRICT

    def test_legacy_null_strategy_still_hydrates(self):
        # Older configs stored materialization_strategy: null (the UI wrote
        # the previous None default); the field stays optional so they load.
        class FakeTrickleSource(il.Source):
            class FakeChild(il.Asset):
                pass

        source = FakeTrickleSource(materialization_strategy=None)
        assert source.materialization_strategy is None
        assert source.assets[0].materialization_strategy == MaterializationStrategy.RECONCILE

    def test_trickles_default_destination_key(self):
        class FakeTrickleSource(il.Source):
            class FakeChild(il.Asset):
                pass

        source = FakeTrickleSource(default_destination_key="primary")
        assert source.assets[0].default_destination_key == "primary"

    def test_assets_get_source_backref(self):
        class FakeTrickleSource(il.Source):
            class FakeChild(il.Asset):
                pass

        source = FakeTrickleSource()
        assert source.assets[0].source is source

    def test_trickles_into_bound_destinations(self):
        class FakeConnectedDestination(FakeDestination):
            connection: FakeConnection | None = il.Relation(FakeConnection, optional=True)

        class FakeConnectedSource(il.Source):
            connection: FakeConnection

        connection = FakeConnection(token="secret")
        destination = FakeConnectedDestination()
        FakeConnectedSource(connection=connection, destinations=[destination])
        assert destination.connection is connection


# -- __call__ reconfiguration --------------------------------------------------


class TestReconfiguration:
    def test_returns_a_copy(self):
        source = FakeSourceWithAssets()
        reconfigured = source(dataset="new")
        assert reconfigured is not source
        assert type(reconfigured) is type(source)

    def test_override_dataset(self):
        assert FakeSourceWithAssets()(dataset="override").dataset == "override"

    def test_override_dataset_repropagates_to_assets(self):
        reconfigured = FakeSourceWithAssets()(dataset="override")
        assert all(a.dataset == "override" for a in reconfigured.assets)

    def test_override_dataset_preserves_per_asset_overrides(self):
        source = FakeSourceWithAssets()
        source.assets[0].dataset = "pinned"
        reconfigured = source(dataset="override")
        assert [a.dataset for a in reconfigured.assets] == ["pinned", "override"]

    def test_override_destination(self):
        new_dest = FakeDestination()
        reconfigured = FakeSourceWithAssets()(destinations=new_dest)
        assert reconfigured.destinations == [new_dest]

    def test_override_a_relation_leaves_the_original_alone(self):
        source = FakeSourceWithAssets(destinations=[FakeDestination()])
        reconfigured = source(destinations=FakeOtherDestination())
        assert isinstance(source.destinations[0], FakeDestination)
        assert isinstance(reconfigured.destinations[0], FakeOtherDestination)

    def test_unknown_relation_name_is_rejected(self):
        with pytest.raises(TypeError, match="declares no relation"):
            FakeSourceWithAssets()(watches=[FakeDestination()])

    def test_materializable_override_propagates_to_assets(self):
        source = FakeSourceWithAssets()
        reconfigured = source(materializable=False)
        assert all(a.materializable is False for a in reconfigured.assets)

    def test_copied_assets_have_source_backref_on_copy(self):
        source = FakeSourceWithAssets()
        reconfigured = source(dataset="new")
        assert all(a.source is reconfigured for a in reconfigured.assets)

    def test_omitted_relations_preserved(self):
        source = FakeSourceWithAssets(dataset="original", destinations=[FakeDestination()])
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

    def test_repointing_a_connection_reaches_a_trickled_asset(self):
        a, b = FakeConnection(token="a"), FakeConnection(token="b")
        source = Shop(connection=a)
        reconfigured = source(connection=b)
        assert reconfigured.orders.connection is b  # ty: ignore[unresolved-attribute]

    def test_repointing_a_connection_preserves_an_asset_s_own_binding(self):
        a, b, own = FakeConnection(token="a"), FakeConnection(token="b"), FakeConnection(token="own")
        source = Shop(connection=a, assets={"orders": {"connection": own}})  # ty: ignore[invalid-argument-type]
        reconfigured = source(connection=b)
        # The copy is deep: what survives is an equal value, not the same object.
        assert reconfigured.orders.connection.token == "own"  # ty: ignore[unresolved-attribute]

    def test_repointing_a_connection_on_a_chained_copy_reaches_its_assets(self):
        a, b = FakeConnection(token="a"), FakeConnection(token="b")
        source = Shop(connection=a)
        copy = source(dataset="other")
        rebound = copy(connection=b)
        assert rebound.orders.connection is b  # ty: ignore[unresolved-attribute]
        assert source.orders.connection is a  # ty: ignore[unresolved-attribute]
        # The intermediate copy is deep: what it holds is an equal value, not the same object.
        assert copy.orders.connection.token == "a"  # ty: ignore[unresolved-attribute]

    def test_repointing_a_connection_leaves_the_original_source_untouched(self):
        a, b = FakeConnection(token="a"), FakeConnection(token="b")
        source = Shop(connection=a)
        source(connection=b)
        assert source.orders.connection is a  # ty: ignore[unresolved-attribute]


# -- Serialization round-trip --------------------------------------------------


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

    def test_source_preserves_instance_id(self):
        source = FakeSource(id="fixed123")
        restored = Component.from_spec(source.to_spec())
        assert restored.id == "fixed123"

    def test_roundtrip_via_json_string(self):
        source = FakeSourceWithAssets(dataset="ds")
        source.assets[0].materializable = False

        spec_json = source.to_spec().model_dump_json()
        restored = Spec.model_validate_json(spec_json).reconstruct()

        assert isinstance(restored, FakeSourceWithAssets)
        assert restored.dataset == "ds"
        assert restored.assets[0].materializable is False


class TestSpecRule:
    """Relations in a source's wire format: assets travel under the source, never under a relation."""

    def test_an_asset_target_is_a_reference_and_lives_under_its_source(self):
        shop = Shop(connection=FakeConnection(token="t"))
        finance = Finance()
        finance.revenue.bind("orders", shop.orders)
        job = il.CronJob(cron="0 6 * * *", targets=[shop, finance])

        init = job.to_spec().init or {}
        assert init["targets"][1]["init"]["assets"]["revenue"]["orders"] == {"ref": shop.orders.id}
        assert init["targets"][0]["init"]["assets"]["orders"]["id"] == shop.orders.id

    def test_a_trickled_relation_is_omitted_from_the_asset_override(self):
        destination = FakeDestination()
        shop = Shop(connection=FakeConnection(token="t"), destinations=[destination])

        init = shop.to_spec().init or {}
        assert init["destinations"][0]["path"] == FakeDestination.classpath()
        assert init["connection"]["path"] == FakeConnection.classpath()
        assert "destinations" not in init["assets"]["orders"]
        assert "connection" not in init["assets"]["orders"]

    def test_an_asset_keeps_a_binding_of_its_own(self):
        destination, own = FakeDestination(), FakeOtherDestination()
        shop = Shop(connection=FakeConnection(token="t"), destinations=[destination])
        shop.orders.bind("destinations", own)

        entries = (shop.to_spec().init or {})["assets"]["orders"]["destinations"]
        assert entries[0] == {"ref": destination.id}
        assert entries[1]["path"] == FakeOtherDestination.classpath()

        rebuilt = Shop.from_spec(shop.to_spec())
        assert [d.id for d in rebuilt.orders.destinations] == [destination.id, own.id]
        assert rebuilt.orders._read_destination().id == destination.id

    def test_round_trip_shares_instances_across_sources(self):
        destination = FakeDestination()
        shop = Shop(connection=FakeConnection(token="t"), destinations=[destination])
        finance = Finance(destinations=[destination])
        finance.revenue.bind("orders", shop.orders)
        job = il.CronJob(cron="0 6 * * *", targets=[shop, finance])

        rebuilt = il.CronJob.from_spec(job.to_spec())
        rebuilt_shop, rebuilt_finance = rebuilt.targets
        assert rebuilt_finance.revenue.orders is rebuilt_shop.orders  # ty: ignore[unresolved-attribute]
        assert rebuilt_shop.destinations[0] is rebuilt_finance.destinations[0]
        assert rebuilt_shop.orders.destinations == [  # ty: ignore[unresolved-attribute]
            rebuilt_shop.destinations[0]
        ]

    def test_resolve_supplies_a_reference_inside_an_asset_override(self):
        shop = Shop(connection=FakeConnection(token="t"))
        spec = Spec(path=Finance.classpath(), init={"assets": {"revenue": {"orders": {"ref": shop.orders.id}}}})

        finance = il.Source.from_spec(spec, resolve={shop.orders.id: shop.orders}.__getitem__)

        assert finance.revenue.orders is shop.orders  # ty: ignore[unresolved-attribute]


class TestSelect:
    """Init-time asset selection via the ``select`` field."""

    def test_unselected_assets_stay_as_non_materializable_deps(self):
        source = FakeSourceWithAssets(select=["fake_second"])
        by_key = {type(a).key: a for a in source.assets}
        assert by_key["fake_second"].materializable
        assert not by_key["fake_first"].materializable
        # The non-materializable sibling stays wired as an upstream.
        assert by_key["fake_second"].bound("fake_first") is by_key["fake_first"]

    def test_selected_assets_keep_their_source(self):
        source = FakeSourceWithAssets(select=["fake_first"])
        assert all(a.source is source for a in source.assets)

    def test_unknown_select_key_raises(self):
        # SourceError is a ValueError, so pydantic wraps it at init time.
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="has no asset"):
            FakeSourceWithAssets(select=["nope"])

    def test_dag_over_selected_source(self):
        dag = il.DAG(FakeSourceWithAssets(select=["fake_second"]))
        generations = dag.topological_generations()
        assert [[type(a).key for a in g] for g in generations] == [["fake_second"]]
