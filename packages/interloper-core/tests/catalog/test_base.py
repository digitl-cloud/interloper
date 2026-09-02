"""Tests for ``interloper.catalog.base``."""

from __future__ import annotations

import pytest

from interloper.asset.base import AssetDefinition
from interloper.catalog.base import Catalog
from interloper.settings import AppSettings


class TestDiscovery:
    """Entry-point discovery of the component universe."""

    def test_discovers_components_from_installed_packages(self):
        # interloper-assets and interloper-google-cloud declare themselves
        # under the interloper.components group; this asserts the discovery
        # end to end, with no package names hardcoded anywhere in core.
        catalog = Catalog.discover()
        assert "amazon_ads" in catalog.components
        assert "bigquery_destination" in catalog.components

    def test_discovers_nested_resources(self):
        catalog = Catalog.discover()
        assert "google_cloud_connection" in catalog.components


class TestFromSettings:
    """Configured paths are the enablement list; discovery is the fallback."""

    def test_empty_settings_fall_back_to_discovery(self, monkeypatch):
        stub = AppSettings.model_construct(catalog=[])
        monkeypatch.setattr(AppSettings, "get", classmethod(lambda cls: stub))
        catalog = Catalog.from_settings()
        assert "amazon_ads" in catalog.components

    def test_configured_paths_narrow(self, monkeypatch):
        stub = AppSettings.model_construct(catalog=["interloper_assets.demo.source.DemoSource"])
        monkeypatch.setattr(AppSettings, "get", classmethod(lambda cls: stub))
        catalog = Catalog.from_settings()
        assert "demo_source" in catalog.components
        assert "amazon_ads" not in catalog.components


class TestEnablement:
    """An enabled catalog is the listed classes, their dependencies and the framework."""

    def test_universe_in_discovery(self):
        components = Catalog.discover().components
        assert {"cron_job", "trigger_hook", "webhook_hook", "demo_source"} <= set(components)

    def test_framework_in_every_catalog(self):
        components = Catalog.from_paths([]).components
        assert {"cron_job", "trigger_hook", "webhook_hook"} <= set(components)
        assert "job" not in components  # the anchor is framework, not content
        assert "demo_source" not in components  # content is opt-in

    def test_dependencies_come_along(self):
        catalog = Catalog.from_paths(["interloper_google_cloud.BigQueryDestination"])
        assert "bigquery_destination" in catalog.components
        assert "google_cloud_connection" in catalog.components
        assert "gcs_destination" not in catalog.components

    def test_unimportable_paths_are_skipped(self):
        catalog = Catalog.from_paths(["not_a_module.Nope", "interloper_assets.demo.source.DemoSource"])
        assert "demo_source" in catalog.components

    def test_paths_round_trip(self):
        enabled = Catalog.from_paths(["interloper_google_cloud.BigQueryDestination"])
        assert Catalog.from_paths(enabled.to_paths()).components.keys() == enabled.components.keys()


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
    """A source's assets are declared inside it, and resolve through parent_key."""

    def test_asset_key_is_not_a_flat_entry(self):
        assert Catalog.discover().get("a") is None

    def test_parent_key_resolves_the_owning_declaration(self):
        definition = Catalog.discover().get("a", parent_key="demo_source")
        assert isinstance(definition, AssetDefinition)
        # The composite path is the point: the flat key cannot name it.
        assert definition.path == "interloper_assets.demo.source:DemoSource.a"
        assert definition.partitioning is not None

    def test_unknown_parent_falls_back_to_the_flat_lookup(self):
        catalog = Catalog.discover()
        assert catalog.get("a", parent_key="gone_source") is None
        assert catalog.get("cron_job", parent_key="gone_source") is not None

    def test_parent_that_does_not_declare_the_key_falls_back(self):
        assert Catalog.discover().get("not_an_asset", parent_key="demo_source") is None

    def test_non_source_parent_falls_back(self):
        assert Catalog.discover().get("a", parent_key="cron_job") is None


class TestVocabulary:
    """catalog.vocabulary: class definition first, anchor as drift fallback."""

    def test_class_definition_is_authoritative(self):
        catalog = Catalog.discover()
        assert "target" in catalog.vocabulary("hook", "trigger_hook")
        assert "target" not in catalog.vocabulary("hook", "webhook_hook")

    def test_unresolved_key_falls_back_to_the_anchor(self):
        catalog = Catalog(components={})
        assert set(catalog.vocabulary("hook", "gone_hook")) == {"watch", "resource"}

    def test_kind_mismatch_falls_back_to_the_anchor(self):
        catalog = Catalog.discover()
        # 'cron_job' resolves, but as a job — a hook row with that key is drift.
        assert set(catalog.vocabulary("hook", "cron_job")) == {"watch", "resource"}

    def test_source_owned_asset_resolves_through_its_parent(self):
        catalog = Catalog.discover()
        # The anchor knows assets have dependencies; only the source's own
        # declaration knows which ones, and which of them are required.
        assert catalog.vocabulary("asset", "e")["dependency"].slots == {}
        slots = catalog.vocabulary("asset", "e", parent_key="demo_source")["dependency"].slots
        assert {name: slot.key for name, slot in slots.items()} == {
            "b": "demo_source.b",
            "c": "demo_source.c",
            "d": "demo_source.d",
        }
        assert all(slot.required for slot in slots.values())
