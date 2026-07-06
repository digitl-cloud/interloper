"""Tests for ``interloper.catalog.base``."""

from __future__ import annotations

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

    def test_explicit_paths_win(self, monkeypatch):
        stub = AppSettings.model_construct(catalog=["interloper_assets.demo.source.DemoSource"])
        monkeypatch.setattr(AppSettings, "get", classmethod(lambda cls: stub))
        catalog = Catalog.from_settings()
        assert "demo_source" in catalog.components
        assert "amazon_ads" not in catalog.components  # not enabled

    def test_empty_settings_fall_back_to_discovery(self, monkeypatch):
        stub = AppSettings.model_construct(catalog=[])
        monkeypatch.setattr(AppSettings, "get", classmethod(lambda cls: stub))
        catalog = Catalog.from_settings()
        assert "amazon_ads" in catalog.components


class TestBuiltins:
    """Framework built-ins are always present, regardless of enablement."""

    def test_job_in_discovery(self):
        assert "job" in Catalog.discover().components

    def test_job_survives_explicit_enablement(self, monkeypatch):
        stub = AppSettings.model_construct(catalog=["interloper_assets.demo.source.DemoSource"])
        monkeypatch.setattr(AppSettings, "get", classmethod(lambda cls: stub))
        catalog = Catalog.from_settings()
        assert "job" in catalog.components

    def test_job_in_empty_catalog(self):
        assert "job" in Catalog.from_paths([]).components
