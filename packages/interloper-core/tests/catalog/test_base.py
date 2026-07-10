"""Tests for ``interloper.catalog.base``."""

from __future__ import annotations

import pytest

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
    """Explicit settings paths are the enablement list; discovery is the fallback."""

    def test_empty_settings_fall_back_to_discovery(self, monkeypatch):
        stub = AppSettings.model_construct(catalog=[])
        monkeypatch.setattr(AppSettings, "get", classmethod(lambda cls: stub))
        catalog = Catalog.from_settings()
        assert "amazon_ads" in catalog.components


class TestDeclaredUniverse:
    """Every catalog contains the declared universe; anchors never appear."""

    def test_universe_in_discovery(self):
        components = Catalog.discover().components
        assert {"cron_job", "trigger_hook", "webhook_hook", "demo_source"} <= set(components)

    def test_configured_paths_add_rather_than_narrow(self, monkeypatch):
        stub = AppSettings.model_construct(catalog=["interloper_assets.demo.source.DemoSource"])
        monkeypatch.setattr(AppSettings, "get", classmethod(lambda cls: stub))
        catalog = Catalog.from_settings()
        assert "cron_job" in catalog.components
        assert "demo_source" in catalog.components

    def test_universe_in_empty_catalog(self):
        components = Catalog.from_paths([]).components
        assert "cron_job" in components
        assert "job" not in components  # the anchor is framework, not content


class TestKindContract:
    """Kinds are registered first; unknown kinds fail the catalog build."""

    def test_component_of_unregistered_kind_fails_loudly(self):
        from interloper.catalog.base import _definitions_from
        from interloper.component import Component
        from interloper.errors import ConfigError

        class FakeUnregisteredKind(Component):
            """Direct Component subclass: its auto-derived kind has no anchor."""

        with pytest.raises(ConfigError, match="kind 'fake_unregistered_kind'"):
            _definitions_from([FakeUnregisteredKind])


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
