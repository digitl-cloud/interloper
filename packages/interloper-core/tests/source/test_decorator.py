"""Tests for ``interloper.source.decorator``."""

from typing import Any

import pytest

import interloper as il
from interloper.normalizer import MaterializationStrategy, Normalizer

# -- Fixtures ------------------------------------------------------------------

# A usable value per override the anchor accepts, so the whole routed surface is
# exercised: a name the routing accepts but the build step cannot stamp crashes
# at build.
OVERRIDES: dict[str, Any] = {
    "tags": ["Tag"],
    "key": "custom_key",
    "name": "Custom Name",
    "icon": "icon:custom",
    "dataset": "custom_dataset",
    "default_destination_key": "custom_destination",
    "normalizer": Normalizer(),
    "materialization_strategy": MaterializationStrategy.RECONCILE,
}


class FakeDecoratedShop(il.Source):
    """A source decorated after the fact."""

    class Orders(il.Asset):
        """Orders."""

        def data(self) -> list[dict[str, Any]]:
            return []


# -- Tests ---------------------------------------------------------------------


class TestOverrides:
    @pytest.mark.parametrize("name", sorted(OVERRIDES))
    def test_every_override_builds_a_function_source(self, name):
        @il.source(**{name: OVERRIDES[name]})
        def probe():
            return []

        assert issubclass(probe, il.Source)

    @pytest.mark.parametrize("name", sorted(OVERRIDES))
    def test_every_override_builds_a_class_source(self, name):
        @il.source(**{name: OVERRIDES[name]})
        class Probe:
            pass

        assert issubclass(Probe, il.Source)


class TestRelations:
    """``relations=`` declares the source's links."""

    def test_relations_kwarg_declares_a_relation(self):
        class Conn(il.Connection):
            """Connection fixture for the decorator surface."""

        @il.source(relations={"connection": il.Relation(Conn)})
        class Probe(il.Source):
            pass

        assert Probe.relations["connection"].target is Conn

    def test_declared_relations_do_not_drop_the_anchor_relations(self):
        class Conn(il.Connection):
            """Connection fixture for the decorator surface."""

        @il.source(relations={"connection": il.Relation(Conn)})
        class Probe(il.Source):
            pass

        assert set(Probe.relations) == {"connection", "destinations"}

    def test_a_list_of_classes_narrows_the_destinations_relation(self):
        @il.source(relations={"destinations": [il.MemoryDestination]})
        class Probe(il.Source):
            pass

        relation = Probe.relations["destinations"]
        assert relation.keys == [il.MemoryDestination.key]
        assert (relation.kind, relation.many, relation.optional) == ("destination", True, True)

    def test_a_component_class_is_the_shorthand_for_a_relation_on_it(self):
        class Conn(il.Connection):
            """Connection fixture for the decorator surface."""

        @il.source(relations={"connection": Conn})
        class Probe(il.Source):
            pass

        assert Probe.relations["connection"].target is Conn


class TestMaterializable:
    """``enabled`` is a switch on the source that its assets read through it."""

    def test_the_decorator_may_declare_a_disabled_source(self):
        @il.asset
        def declared_asset() -> list[dict[str, Any]]:
            return []

        @il.source(enabled=False)
        def declared_source():
            return [declared_asset]

        assert all(not asset.enabled for asset in declared_source().assets)

    def test_still_available_as_an_instance_override(self):
        @il.asset
        def probe_asset() -> list[dict[str, Any]]:
            return []

        @il.source
        def probe_source():
            return [probe_asset]

        assert all(not a.enabled for a in probe_source()(enabled=False).assets)


class TestFunctionForm:
    """``@il.source`` on a function that returns its assets."""

    def test_a_list_of_asset_classes_becomes_the_source(self):
        @il.asset
        def one() -> list[dict[str, Any]]:
            return [{"a": 1}]

        @il.asset
        def two() -> list[dict[str, Any]]:
            return [{"a": 2}]

        @il.source
        def pair() -> list[type[il.Asset]]:
            return [one, two]

        assert issubclass(pair, il.Source)
        assert {asset.key for asset in pair().assets} == {"one", "two"}

    def test_a_single_asset_class_is_accepted(self):
        @il.asset
        def solo() -> list[dict[str, Any]]:
            return [{"a": 1}]

        @il.source
        def wrapper() -> type[il.Asset]:
            return solo

        assert [asset.key for asset in wrapper().assets] == ["solo"]

    def test_a_function_returning_nothing_yields_no_assets(self):
        @il.source
        def empty() -> None:
            pass

        assert empty().assets == []

    def test_annotated_parameters_become_config_fields(self):
        @il.asset
        def rows() -> list[dict[str, Any]]:
            return [{"a": 1}]

        @il.source
        def configured(
            account_id: str = il.InputField(default="acc-1"),
            region: str = il.InputField(default="eu"),
        ) -> list[type[il.Asset]]:
            return [rows]

        assert set(configured.model_fields) >= {"account_id", "region"}
        instance = configured(account_id="acc-2")
        assert instance.account_id == "acc-2"
        assert instance.region == "eu"

    def test_the_docstring_is_carried_over(self):
        # Source docstrings ship as the component's description in the app.
        @il.source
        def documented() -> None:
            """Everything the vendor exposes."""

        assert documented.__doc__ == "Everything the vendor exposes."

    def test_the_key_and_module_come_from_the_function(self):
        @il.source
        def google_ads() -> None:
            pass

        assert google_ads.key == "google_ads"
        assert google_ads.__module__ == __name__


class TestDecoratedSubclass:
    def test_a_decorated_source_subclass_keeps_its_assets(self):
        built = il.source(tags=["Shop"])(FakeDecoratedShop)

        source = built()

        assert built.Orders is FakeDecoratedShop.Orders
        assert source.Orders is source.assets[0]
        assert type(source.Orders).key == "orders"
        assert il.Serializable.resolve_path(built.Orders.classpath()) is built.Orders


def test_a_source_function_returning_something_else_is_refused():
    """A source function returns asset classes; anything else is named in the refusal."""
    with pytest.raises(TypeError, match=r"returned 'str'"):

        @il.source
        def broken() -> Any:
            return "not assets"
