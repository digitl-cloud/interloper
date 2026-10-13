"""Tests for ``interloper.source.base``."""

# Note: no ``from __future__ import annotations``: an annotation naming a
# component class declares a relation, and the collector needs it as a real
# class, not a lazy string.

from typing import Any, ClassVar

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


class FakeDestination(il.MemoryDestination):
    """A destination-shaped fixture."""


class FakeOtherDestination(il.MemoryDestination):
    """A destination-shaped fixture."""


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


class Cascade(il.Source):
    """Source whose method assets exercise inheritance."""

    region: str = il.InputField(default="eu")

    @il.asset(materialization_strategy=MaterializationStrategy.RECONCILE)
    def explicit(self) -> list[dict[str, Any]]:
        return []

    @il.asset
    def plain(self) -> list[dict[str, Any]]:
        return []

    @il.asset(dataset="raw_{region}")  # noqa: RUF027 - a template the source fills
    def templated(self) -> list[dict[str, Any]]:
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


class TestInheritance:
    def test_an_explicit_reconcile_survives_a_strict_source(self):
        source = Cascade(materialization_strategy=MaterializationStrategy.STRICT)
        assert source.explicit.materialization_strategy is MaterializationStrategy.RECONCILE
        assert source.plain.materialization_strategy is MaterializationStrategy.STRICT

    def test_a_reconfigured_default_destination_key_reaches_the_assets(self):
        assert Cascade()(default_destination_key="warehouse").plain.default_destination_key == "warehouse"

    def test_a_reconfigured_normalizer_reaches_the_assets(self):
        normalizer = Normalizer()
        assert Cascade()(normalizer=normalizer).plain.normalizer is normalizer

    def test_a_retry_set_after_construction_reaches_the_assets(self):
        source = Cascade()
        source.retry = il.RetryPolicy(max_attempts=3)
        assert source.plain.retry == il.RetryPolicy(max_attempts=3)

    def test_a_dataset_set_after_construction_reaches_the_assets(self):
        source = Cascade()
        source.dataset = "other"
        assert source.plain.dataset == "other"

    def test_a_spec_carries_the_assets_own_values_only(self):
        payload = (Cascade(dataset="raw").to_spec().init or {})["assets"]["plain"]
        inherited = {"dataset", "default_destination_key", "materialization_strategy", "normalizer", "retry"}
        assert inherited.isdisjoint(payload)

    def test_an_assets_dataset_defaults_to_the_source_key(self):
        source = Cascade()
        assert source.dataset is None
        assert source.plain.dataset == Cascade.key

    def test_a_dataset_template_names_a_field_of_the_source(self):
        assert Cascade(region="us").templated.dataset == "raw_us"

    def test_a_blank_persisted_value_inherits(self):
        overrides = {"plain": {"dataset": "", "materialization_strategy": None}}
        source = Cascade(dataset="raw", assets=overrides)  # ty: ignore[invalid-argument-type]
        assert source.plain.dataset == "raw"
        assert source.plain.materialization_strategy is MaterializationStrategy.RECONCILE

    def test_a_disabled_source_disables_its_assets(self):
        source = Cascade(enabled=False)
        assert source.plain.enabled is False
        assert source.plain.__dict__["enabled"] is True

    def test_an_asset_disabled_alone_stays_disabled(self):
        assert Cascade().plain(enabled=False).enabled is False

    def test_the_source_enabled_flag_is_not_a_form_field(self):
        assert "enabled" not in Cascade.config_schema().get("properties", {})

    def test_a_standalone_asset_reads_its_own_values(self):
        @il.asset
        def lonely() -> list[dict[str, Any]]:
            return []

        asset = lonely()
        assert asset.dataset is None
        assert asset.materialization_strategy is None
        assert asset.normalizer is None


class TestIdentity:
    def test_key_auto_derived_from_class_name(self):
        assert FakeSource.key == "fake_source"
        assert FakeOtherSource.key == "fake_other_source"
        assert FakeSourceWithAssets.key == "fake_source_with_assets"

    def test_kind_is_source(self):
        assert il.Source.kind == "source"
        assert FakeSource.kind == "source"

    def test_dataset_defaults_to_unset_and_assets_read_the_source_key(self):
        source = FakeSourceWithAssets()
        assert source.dataset is None
        assert source.assets[0].dataset == FakeSourceWithAssets.key

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
        assert source.orders.connection is connection

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
        assert source.revenue.orders is source.orders

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
        assert source.table(asset) == "fake_discriminated__123"
        assert asset.table == "fake_discriminated__123"
        # The logical identity is untouched — only the physical name varies.
        assert type(asset).key == "fake_discriminated"
        assert asset.dataset == "fake_discriminated_source"

    def test_table_is_sanitized(self):
        source = FakeDiscriminatedSource(account_id="act_123-DE")
        (asset,) = source.assets
        assert asset.table == "fake_discriminated__act_123_de"

    def test_default_table_is_the_asset_key(self):
        source = FakeDiscriminatedSource()
        (asset,) = source.assets
        assert asset.table == "fake_discriminated"

    def test_table_survives_spec_round_trip(self):
        source = FakeDiscriminatedSource(account_id="123")
        restored = FakeDiscriminatedSource.from_spec(source.to_spec())
        assert [a.table for a in restored.assets] == ["fake_discriminated__123"]

    def test_table_override_takes_full_control(self):
        class FakeOverridingSource(FakeDiscriminatedSource):
            def table(self, asset: il.Asset) -> str:
                return f"custom__{asset.key}"

        (asset,) = FakeOverridingSource(account_id="123").assets
        assert asset.table == "custom__fake_discriminated"

    def test_invalid_table_raises_at_init(self):
        # The composed name is validated during ``_resolve``; ``validate_key``
        # raises a ValueError, so pydantic wraps it at init time.
        from pydantic import ValidationError

        class FakeBadTableSource(FakeDiscriminatedSource):
            def table(self, asset: il.Asset) -> str:
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

    def test_partitionings_are_the_partitioned_assets_own(self):
        class FakeDaily(il.Asset):
            partitioning = il.TimePartitionConfig(column="date")

            def data(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
                return []

        class FakeUndated(il.Asset):
            def data(self) -> list[dict[str, Any]]:
                return []

        class FakeMixedSource(il.Source):
            asset_types: ClassVar[list[type[il.Asset]]] = [FakeDaily, FakeUndated]

        [partitioning] = FakeMixedSource.definition().partitionings()

        assert partitioning["column"] == "date"
        assert FakeSource.definition().partitionings() == []

    def test_config_leads_with_the_sources_own_fields(self):
        class FakeAccountSource(il.Source):
            account_id: str = il.InputField(default="")

        properties = FakeAccountSource.config_schema()["properties"]
        assert [(name, prop.get("x-section")) for name, prop in properties.items()] == [
            ("account_id", None),
            ("dataset", "Destination"),
            ("default_destination_key", "Destination"),
            ("materialization_strategy", "Materialization"),
            ("retry", "Operation"),
        ]

    def test_assets_show_the_less_mature_of_their_own_and_the_sources_maturity(self):
        @il.source(maturity="beta")
        class FakeBetaSource(il.Source):
            @il.asset
            def plain(self) -> list:
                return []

            @il.asset(maturity="alpha")
            def early(self) -> list:
                return []

        defn = FakeBetaSource.definition()
        assert defn.maturity is il.Maturity.BETA
        assert {a.key: a.maturity for a in defn.assets} == {"plain": il.Maturity.BETA, "early": il.Maturity.ALPHA}

    def test_a_deprecated_source_deprecates_its_assets(self):
        @il.source(maturity="deprecated")
        class FakeDeprecatedSource(il.Source):
            @il.asset(maturity="alpha")
            def early(self) -> list:
                return []

        assert FakeDeprecatedSource.definition().assets[0].maturity is il.Maturity.DEPRECATED

    def test_a_stable_source_keeps_each_assets_own_maturity(self):
        @il.source
        class FakeStableSource(il.Source):
            @il.asset(maturity="beta")
            def early(self) -> list:
                return []

        assert FakeStableSource.definition().assets[0].maturity is il.Maturity.BETA

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

    def test_nested_asset_classes_stay_classes_on_the_class(self):
        assert FakeSourceWithAssets.FakeFirst.key == "fake_first"
        assert FakeSourceWithAssets.__dict__["fake_first"] is FakeSourceWithAssets.FakeFirst

    def test_model_post_init_auto_instantiates_assets_from_types(self):
        source = FakeSourceWithAssets()
        assert len(source.assets) == 2
        assert {type(a).key for a in source.assets} == {"fake_first", "fake_second"}

    def test_model_post_init_skips_auto_instantiation_when_assets_supplied(self):
        # Reconstruction / explicit construction must not duplicate the assets.
        asset = FakeSourceWithAssets.asset_types[0]()
        source = FakeSourceWithAssets(assets=[asset])
        assert source.assets == [asset]

    def test_an_asset_is_reachable_by_key(self):
        source = FakeSourceWithAssets()
        looked_up = source.fake_first
        assert type(looked_up).key == "fake_first"
        assert looked_up is source.assets[0]

    def test_an_unknown_key_raises(self):
        source = FakeSourceWithAssets()
        with pytest.raises(AttributeError):
            _ = source.does_not_exist

    def test_assets_are_parented_by_their_source(self):
        source = FakeSourceWithAssets()
        assert all(a.parent is source for a in source.assets)
        assert source.fake_first.qualified_key == "fake_source_with_assets.fake_first"


class TestReachingAnAsset:
    def test_the_class_read_is_the_asset_class(self):
        assert Cascade.plain.key == "plain"
        assert isinstance(Cascade.plain, type)

    def test_the_instance_read_is_the_live_asset(self):
        source = Cascade()
        assert source.plain is source.assets[1]
        assert source.plain.parent is source

    def test_a_nested_asset_class_is_reachable_under_its_key(self):
        source = Shop(connection=FakeConnection(token="t"))
        assert source.orders is source.Orders

    def test_a_listed_asset_is_reachable_under_its_key(self):
        @il.asset
        def listed() -> list[dict[str, Any]]:
            return []

        class Listing(il.Source):
            asset_types: ClassVar[list[type[il.Asset]]] = [listed]

        assert Listing.listed is listed  # ty: ignore[unresolved-attribute]
        assert Listing().listed.parent is not None

    def test_another_holder_reads_the_class(self):
        class Holder:
            asset_cls = Cascade.plain

        assert Holder().asset_cls is Cascade.plain

    def test_a_standalone_asset_is_untouched(self):
        @il.asset
        def lonely() -> list[dict[str, Any]]:
            return []

        assert lonely().parent is None
        assert lonely.classpath().endswith(".lonely")

    def test_an_asset_owned_by_another_source_is_refused(self):
        with pytest.raises(TypeError, match="already belongs to source"):

            class Thief(il.Source):
                asset_types: ClassVar[list[type[il.Asset]]] = [Cascade.plain]

    def test_an_asset_key_taken_by_another_attribute_is_refused(self):
        @il.asset
        def region() -> list[dict[str, Any]]:
            return []

        clashing = region
        with pytest.raises(TypeError, match="the name is taken"):

            class Clash(il.Source):
                region: ClassVar[str] = "eu"
                asset_types: ClassVar[list[type[il.Asset]]] = [clashing]

    def test_an_asset_key_naming_an_inherited_attribute_is_refused(self):
        @il.asset
        def table() -> list[dict[str, Any]]:
            return []

        clashing = table
        with pytest.raises(TypeError, match="the name is taken"):

            class Clash(il.Source):
                asset_types: ClassVar[list[type[il.Asset]]] = [clashing]

    def test_a_subclass_keeps_its_parents_assets_reachable(self):
        class Child(Shop):
            pass

        assert Child(connection=FakeConnection(token="t")).orders.key == "orders"

    def test_a_pickled_source_keeps_its_assets_and_their_reads(self):
        import pickle

        source = pickle.loads(pickle.dumps(Cascade(dataset="raw")))
        assert source.plain.parent is source
        assert source.plain.dataset == "raw"

    def test_select_asset_def_and_getattr_are_gone(self):
        assert not hasattr(il.Source, "asset_def")
        assert "select" not in il.Source.model_fields
        assert "__getattr__" not in vars(il.Source)


# -- Trickle-down resolution ---------------------------------------------------


class TestRelationInheritance:
    def test_an_asset_reads_the_sources_connection_without_holding_it(self):
        connection = FakeConnection(token="t")
        source = Shop(connection=connection)
        assert source.orders.connection is connection
        assert source.orders.bound("connection") is None

    def test_a_connection_rebound_on_the_source_reaches_the_asset(self):
        source = Shop(connection=FakeConnection(token="a"))
        replacement = FakeConnection(token="b")
        source.connection = replacement
        assert source.orders.connection is replacement

    def test_an_assets_own_binding_wins(self):
        own = FakeConnection(token="own")
        source = Shop(connection=FakeConnection(token="source"))
        source.orders.connection = own
        assert source.orders.connection is own

    def test_destinations_read_through_the_source(self):
        destination = FakeDestination()
        source = Shop(connection=FakeConnection(token="t"), destinations=[destination])
        assert source.orders.destinations == [destination]
        assert source.orders.bound("destinations") == []

    def test_a_target_the_assets_relation_does_not_accept_is_not_inherited(self):
        class Narrowed(il.Source):
            class Picky(il.Asset):
                destinations: list[il.Destination] = il.Relation(
                    "destination", "fake_destination", many=True, optional=True
                )

                def data(self) -> Any:  # pragma: no cover
                    return []

        source = Narrowed(destinations=[FakeOtherDestination()])
        assert source.picky.destinations == []

    def test_a_relation_filled_through_the_source_validates(self):
        source = Shop(connection=FakeConnection(token="t"))
        source.orders.validate_relations()


class TestInheritedSettings:
    def test_a_source_policy_fills_its_assets(self):
        policy = il.RetryPolicy(max_attempts=5)

        class Shop(il.Source):
            class Orders(il.Asset):
                def data(self) -> Any:  # pragma: no cover
                    return []

        assert Shop(retry=policy).assets[0].retry == policy

    def test_an_assets_own_policy_wins(self):
        source_policy = il.RetryPolicy(max_attempts=5)
        asset_policy = il.RetryPolicy(max_attempts=2)

        class Shop(il.Source):
            class Orders(il.Asset):
                retry: il.RetryPolicy | None = asset_policy

                def data(self) -> Any:  # pragma: no cover
                    return []

        assert Shop(retry=source_policy).assets[0].retry == asset_policy

    def test_no_policy_anywhere_leaves_assets_unset(self):
        class Shop(il.Source):
            class Orders(il.Asset):
                def data(self) -> Any:  # pragma: no cover
                    return []

        assert Shop().assets[0].retry is None

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
        # Read through the source; conform treats an unset strategy as reconcile.
        assert source.assets[0].materialization_strategy is None
        assert source.assets[0].__dict__["materialization_strategy"] is None

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
    def test_naming_a_field_changes_it_on_a_copy(self):
        source = Cascade()
        copy = source(dataset="raw")
        assert copy.dataset == "raw"
        assert source.dataset is None

    def test_a_wrong_value_is_refused(self):
        with pytest.raises(ValidationError):
            Cascade()(region=42)

    def test_an_unknown_name_is_refused(self):
        with pytest.raises(TypeError, match="no field or relation"):
            Cascade()(bogus=1)

    def test_a_disabled_copy_disables_its_assets(self):
        copy = Cascade()(enabled=False)
        assert copy.plain.enabled is False

    def test_an_inherited_none_inherits_again(self):
        source = Cascade(dataset="raw")
        asset = source.plain(dataset="own")
        assert asset.dataset == "own"
        assert asset(dataset=None).dataset == "raw"

    def test_naming_a_relation_rebinds_it(self):
        replacement = FakeConnection(token="b")
        copy = Shop(connection=FakeConnection(token="a"))(connection=replacement)
        assert copy.orders.connection is replacement

    def test_none_clears_an_optional_relation(self):
        copy = Shop(connection=FakeConnection(token="a"), destinations=[FakeDestination()])(destinations=None)
        assert copy.destinations == []

    def test_a_copy_rewires_siblings_and_shares_outside_targets(self):
        connection = FakeConnection(token="t")
        source = Shop(connection=connection)
        copy = source(dataset="raw")
        assert copy.revenue.orders is copy.orders
        assert copy.orders is not source.orders
        assert copy.orders.parent is copy
        assert copy.connection is connection

    def test_the_original_is_left_alone(self):
        source = Shop(connection=FakeConnection(token="a"))
        source(connection=FakeConnection(token="b"))
        assert source.connection.token == "a"


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
        source.assets[0].enabled = False
        source.assets[0].dataset = "custom"

        restored = FakeSourceWithAssets.from_spec(source.to_spec())
        assert restored.assets[0].enabled is False
        assert restored.assets[0].dataset == "custom"
        # Other asset unchanged
        assert restored.assets[1].enabled is True

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
        source.assets[0].enabled = False

        spec_json = source.to_spec().model_dump_json()
        restored = Spec.model_validate_json(spec_json).reconstruct()

        assert isinstance(restored, FakeSourceWithAssets)
        assert restored.dataset == "ds"
        assert restored.assets[0].enabled is False


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
        assert [entry["path"] for entry in entries] == [FakeOtherDestination.classpath()]

        rebuilt = Shop.from_spec(shop.to_spec())
        assert [d.id for d in rebuilt.orders.destinations] == [own.id]
        assert rebuilt.orders.default_destination.id == own.id

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

        assert finance.revenue.orders is shop.orders
