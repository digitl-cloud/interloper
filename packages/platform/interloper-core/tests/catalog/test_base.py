"""Tests for ``interloper.catalog.base``."""

from __future__ import annotations

from types import ModuleType, SimpleNamespace

import pytest

import interloper as il
from interloper.asset.base import AssetDefinition
from interloper.catalog import Catalog
from interloper.catalog import base as catalog_base
from interloper.component import Relation
from interloper.settings import AppSettings

_FRAMEWORK = {"cron_job", "trigger_hook", "webhook_hook"}
_DAILY = il.TimePartitionConfig(column="date", allow_window=False)


class DemoSource(il.Source):
    """A daily source whose assets form the ``a -> b,c,d -> e`` DAG."""

    @il.asset(partitioning=_DAILY)
    def a(self) -> list[dict]:
        return []

    @il.asset(partitioning=_DAILY)
    def b(self, a: il.Upstream) -> list[dict]:
        return []

    @il.asset(partitioning=_DAILY)
    def c(self, a: il.Upstream) -> list[dict]:
        return []

    @il.asset(partitioning=_DAILY)
    def d(self, a: il.Upstream) -> list[dict]:
        return []

    @il.asset(partitioning=_DAILY)
    def e(self, b: il.Upstream, c: il.Upstream, d: il.Upstream) -> list[dict]:
        return []


class DemoConnection(il.Connection):
    """Connection the demo destination writes through."""


class DemoDestination(il.MemoryDestination):
    """Destination that needs a connection."""

    connection: DemoConnection


class LookupConnection(il.Connection):
    """Connection the reading source's asset looks values up through."""


class ReadingSource(il.Source):
    """Source whose asset, not the source itself, needs a connection."""

    class Enriched(il.Asset):
        """Asset reading through the lookup connection."""

        lookup: LookupConnection

        def data(self, context: il.ExecutionContext) -> list[dict]:
            return []


class TestDiscovery:
    """Entry-point discovery of the component universe."""

    def test_discovers_the_components_installed_packages_declare(self):
        # Core declares its jobs and hooks under interloper.components the
        # way any package declares its own, so it is the discovery end to end.
        assert _FRAMEWORK <= set(Catalog.discover().components)

    def test_an_entry_naming_a_module_declares_the_component_classes_in_it(self, monkeypatch):
        module = ModuleType("demo_components")
        module.__dict__.update(DemoSource=DemoSource, DemoConnection=DemoConnection, helper=len)
        entry = SimpleNamespace(load=lambda: module)
        monkeypatch.setattr(catalog_base, "entry_points", lambda group: [entry])

        assert set(Catalog._declared_classes()) == {DemoSource, DemoConnection}


class TestFromSettings:
    """Configured paths are the enablement list; discovery is the fallback."""

    def test_empty_settings_fall_back_to_discovery(self, monkeypatch):
        stub = AppSettings.model_construct(catalog=[])
        monkeypatch.setattr(AppSettings, "get", classmethod(lambda cls: stub))
        assert Catalog.from_settings().components.keys() == Catalog.discover().components.keys()

    def test_configured_paths_narrow(self, monkeypatch):
        stub = AppSettings.model_construct(catalog=[DemoSource.definition().path])
        monkeypatch.setattr(AppSettings, "get", classmethod(lambda cls: stub))
        assert set(Catalog.from_settings().components) == {"demo_source", *_FRAMEWORK}


class TestEnablement:
    """An enabled catalog is the listed classes, their dependencies and the framework."""

    def test_framework_in_every_catalog(self):
        # Content is opt-in, and the kind anchors are framework, not content.
        assert set(Catalog.from_paths([]).components) == _FRAMEWORK

    def test_dependencies_come_along(self):
        # DemoDestination declares `connection: DemoConnection`, so the
        # closure imports that class from the relation's target.
        catalog = Catalog.from_paths([DemoDestination.definition().path])
        assert set(catalog.components) == {"demo_destination", "demo_connection", *_FRAMEWORK}

    def test_asset_dependencies_of_a_source_come_along(self):
        catalog = Catalog.from_paths([ReadingSource.definition().path])
        assert set(catalog.components) == {"reading_source", "lookup_connection", *_FRAMEWORK}

    def test_unimportable_paths_are_skipped(self):
        catalog = Catalog.from_paths(["not_a_module.Nope", DemoSource.definition().path])
        assert "demo_source" in catalog.components

    def test_paths_round_trip(self):
        enabled = Catalog.from_paths([DemoDestination.definition().path])
        assert Catalog.from_paths(enabled.to_paths()).components.keys() == enabled.components.keys()


class TestGroups:
    """A definition's group is where the app shows it: a key of the catalog, or any name."""

    def test_a_group_names_another_definition(self):
        @il.source
        class FakePlatform(il.Source):
            pass

        @il.source(group="fake_platform")
        class FakePlatformStarSchema(il.Source):
            pass

        catalog = Catalog.from_assets([FakePlatform, FakePlatformStarSchema])
        assert catalog.components["fake_platform_star_schema"].group == "fake_platform"
        assert catalog.components["fake_platform"].group == "fake_platform"

    def test_a_group_need_not_be_a_catalog_key(self):
        @il.source(group="cross_platform")
        class FakeCrossPlatformAnalysis(il.Source):
            pass

        catalog = Catalog.from_assets([FakeCrossPlatformAnalysis])
        assert catalog.components["fake_cross_platform_analysis"].group == "cross_platform"


class TestKindContract:
    """Kinds are registered first; unknown kinds fail the catalog build."""

    def test_component_of_unregistered_kind_fails_loudly(self):
        from interloper.component import Component
        from interloper.errors import ConfigError

        class FakeUnregisteredKind(Component):
            """Direct Component subclass: its auto-derived kind has no anchor."""

        with pytest.raises(ConfigError, match="kind 'fake_unregistered_kind'"):
            Catalog._definitions_from([FakeUnregisteredKind])


class TestSourceOwnedAssets:
    """A source's assets are declared inside it, and resolve by their qualified key."""

    def test_asset_key_is_not_a_flat_entry(self):
        assert Catalog.from_assets([DemoSource]).get("a") is None

    def test_a_qualified_key_resolves_the_owning_declaration(self):
        definition = Catalog.from_assets([DemoSource]).get("demo_source.a")
        assert isinstance(definition, AssetDefinition)
        # The composite path is the point: the flat key cannot name it.
        assert definition.path == f"{DemoSource.__module__}:DemoSource.a"
        assert definition.partitioning is not None

    @pytest.mark.parametrize("key", ["gone_source.a", "demo_source.not_an_asset", "cron_job.a", "gone_source.cron_job"])
    def test_a_qualified_key_resolves_only_through_a_source_that_declares_it(self, key: str):
        assert Catalog.from_assets([DemoSource]).get(key) is None


class TestVocabulary:
    """catalog.vocabulary: class definition first, anchor as drift fallback."""

    def test_returns_relations(self):
        catalog = Catalog.discover()
        relation = catalog.vocabulary("job", "cron_job")["targets"]
        assert isinstance(relation, Relation)
        assert relation.kinds == ["source", "asset"]

    def test_class_definition_is_authoritative(self):
        catalog = Catalog.discover()
        assert "targets" in catalog.vocabulary("hook", "trigger_hook")
        assert "targets" not in catalog.vocabulary("hook", "webhook_hook")

    def test_unresolved_key_falls_back_to_the_anchor(self):
        catalog = Catalog(components={})
        assert set(catalog.vocabulary("hook", "gone_hook")) == {"watches"}

    def test_kind_mismatch_falls_back_to_the_anchor(self):
        catalog = Catalog.discover()
        # 'cron_job' resolves, but as a job — a hook row with that key is drift.
        assert set(catalog.vocabulary("hook", "cron_job")) == {"watches"}

    def test_source_owned_asset_resolves_through_its_parent(self):
        catalog = Catalog.from_assets([DemoSource])
        # The anchor knows only what every asset declares; which upstreams an
        # asset has comes from the source's own declaration. Their keys are
        # bare, which is what scopes them to the parent source.
        assert set(catalog.vocabulary("asset", "e")) == {"destinations"}
        relations = catalog.vocabulary("asset", "demo_source.e")
        upstreams = {name: relation for name, relation in relations.items() if "asset" in relation.kinds}
        assert {name: relation.keys for name, relation in upstreams.items()} == {
            "b": ["b"],
            "c": ["c"],
            "d": ["d"],
        }
        assert all(not relation.optional for relation in upstreams.values())

    def test_a_source_owned_bare_key_binds_the_sibling(self):
        # The other half of the parent scoping: the bare keys the catalog
        # reports are the sibling assets the source binds at construction.
        assert DemoSource.sibling_bindings()["e"] == {"b": "b", "c": "c", "d": "d"}
        source = DemoSource()
        by_key = {asset.key: asset for asset in source.assets}
        assert by_key["e"].bound("b") is by_key["b"]


def test_get_returns_none_for_an_unknown_key():
    """An unknown key resolves to ``None``."""
    assert Catalog.discover().get("no_such_component") is None
