"""Tests for ``interloper.cli.manifest``."""

# Note: no ``from __future__ import annotations`` — the fixtures below define
# methods whose parameter annotations must be real classes (not lazy strings)
# so that sibling-dep inference in ``Source._infer_all_requires`` can resolve
# them by name.

import datetime as dt
from typing import Any

import pytest

import interloper as il
from interloper.catalog import Catalog
from interloper.cli.manifest import (
    AssetItemManifest,
    PartitionManifest,
    RunManifest,
)
from interloper.errors import ManifestError
from interloper.partitioning import TimePartition, TimePartitionWindow

# ---------------------------------------------------------------------------
# Fixtures: a small source (alpha -> beta) and a standalone asset, referenced
# in manifests via their real import paths.
# ---------------------------------------------------------------------------


class FakeManifestSource(il.Source):
    """Two-asset source used for compile tests."""

    greeting: str = "hello"

    class Alpha(il.Asset):
        """Root asset."""

        def data(self) -> Any:  # pragma: no cover
            return [{"x": 1}]

    class Beta(il.Asset):
        """Depends on alpha."""

        def data(self, alpha: Any) -> Any:  # pragma: no cover
            return alpha


class FakeStandaloneAsset(il.Asset):
    """Standalone asset used for compile tests."""

    def data(self) -> Any:  # pragma: no cover
        return [{"x": 1}]


class FakeManifestResource(il.Resource):
    """Resource fixture (no required fields) for auto-use tests."""

    token: str = ""


class FakeResourceSource(il.Source):
    """Source with a ``conn`` resource slot, for auto-resource tests."""

    conn: FakeManifestResource

    class Solo(il.Asset):
        """Single asset."""

        def data(self) -> Any:  # pragma: no cover
            return [{"x": 1}]


SOURCE_PATH = f"{FakeManifestSource.__module__}.FakeManifestSource"
ASSET_PATH = f"{FakeStandaloneAsset.__module__}.FakeStandaloneAsset"
RESOURCE_SOURCE_PATH = f"{FakeResourceSource.__module__}.FakeResourceSource"
RESOURCE_PATH = f"{FakeManifestResource.__module__}.FakeManifestResource"
MEMORY_DESTINATION_PATH = "interloper.destination.memory.MemoryDestination"
FILE_DESTINATION_PATH = "interloper.destination.file.FileDestination"


def _manifest(body: str) -> RunManifest:
    return RunManifest.from_yaml(body)


# ---------------------------------------------------------------------------
# Loading and env interpolation
# ---------------------------------------------------------------------------


class TestFromYaml:
    """Parsing, validation, and env interpolation of the YAML document."""

    def test_minimal_manifest(self) -> None:
        manifest = _manifest(f"assets: [{{source: {SOURCE_PATH}}}]")
        assert manifest.assets[0].source == SOURCE_PATH
        assert manifest.runner is None
        assert manifest.partition is None

    def test_invalid_yaml_raises(self) -> None:
        with pytest.raises(ManifestError, match="Invalid YAML"):
            _manifest("assets: [unclosed")

    def test_non_mapping_raises(self) -> None:
        with pytest.raises(ManifestError, match="must be a YAML mapping"):
            _manifest("- just\n- a list")

    def test_unknown_top_level_key_raises(self) -> None:
        with pytest.raises(ManifestError, match="Invalid manifest"):
            _manifest(f"assets: [{{source: {SOURCE_PATH}}}]\nrunenr: {{type: serial}}")

    def test_empty_assets_raises(self) -> None:
        with pytest.raises(ManifestError, match="Invalid manifest"):
            _manifest("assets: []")

    def test_env_interpolation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MANIFEST_TEST_GREETING", "bonjour")
        manifest = _manifest(
            f"""
            assets:
              - source: {SOURCE_PATH}
                config:
                  greeting: ${{MANIFEST_TEST_GREETING}}
            """
        )
        assert manifest.assets[0].config["greeting"] == "bonjour"

    def test_missing_env_var_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("MANIFEST_TEST_MISSING", raising=False)
        with pytest.raises(ManifestError, match="MANIFEST_TEST_MISSING"):
            _manifest(
                f"""
                assets:
                  - source: {SOURCE_PATH}
                    config:
                      greeting: ${{MANIFEST_TEST_MISSING}}
                """
            )


# ---------------------------------------------------------------------------
# Sub-model validation
# ---------------------------------------------------------------------------


class TestPartitionManifest:
    """Partition block validation and resolution."""

    def test_date_resolves_to_partition(self) -> None:
        partition = PartitionManifest(date=dt.date(2026, 6, 1)).resolve()
        assert isinstance(partition, TimePartition)
        assert partition.id == "2026-06-01"

    def test_window_resolves_to_partition_window(self) -> None:
        window = PartitionManifest(start=dt.date(2026, 6, 1), end=dt.date(2026, 6, 3)).resolve()
        assert isinstance(window, TimePartitionWindow)
        assert sorted(p.id for p in window) == ["2026-06-01", "2026-06-02", "2026-06-03"]

    def test_date_and_window_rejected(self) -> None:
        with pytest.raises(ValueError, match="cannot be combined"):
            PartitionManifest(date=dt.date(2026, 6, 1), start=dt.date(2026, 6, 1), end=dt.date(2026, 6, 2))

    def test_partial_window_rejected(self) -> None:
        with pytest.raises(ValueError, match="both 'start' and 'end'"):
            PartitionManifest(start=dt.date(2026, 6, 1))

    def test_empty_rejected(self) -> None:
        with pytest.raises(ValueError, match="either 'date' or 'start'/'end'"):
            PartitionManifest()


class TestAssetItemManifest:
    """Asset item shape validation."""

    def test_source_and_asset_rejected(self) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            AssetItemManifest(source=SOURCE_PATH, asset=ASSET_PATH)

    def test_neither_source_nor_asset_rejected(self) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            AssetItemManifest()

    def test_select_on_asset_rejected(self) -> None:
        with pytest.raises(ValueError, match="only valid on source items"):
            AssetItemManifest(asset=ASSET_PATH, select=["alpha"])


# ---------------------------------------------------------------------------
# Compilation
# ---------------------------------------------------------------------------


class TestCompile:
    """Manifest compilation into a run plan."""

    def test_source_with_config_and_destination(self) -> None:
        plan = _manifest(
            f"""
            name: test-run
            assets:
              - source: {SOURCE_PATH}
                config:
                  greeting: hi
                destinations:
                  - type: {MEMORY_DESTINATION_PATH}
            """
        ).compile()

        assert plan.name == "test-run"
        assert len(plan.dag.assets) == 2
        for asset in plan.dag.assets:
            assert asset.source is not None
            assert asset.source.greeting == "hi"
            assert isinstance(asset.destination, list)
            assert isinstance(asset.destination[0], il.MemoryDestination)

    def test_select_marks_unselected_non_materializable(self) -> None:
        plan = _manifest(
            f"""
            assets:
              - source: {SOURCE_PATH}
                select: [beta]
            """
        ).compile()

        by_key = {type(a).key: a for a in plan.dag.assets}
        assert by_key["beta"].materializable
        assert not by_key["alpha"].materializable
        generations = plan.dag.topological_generations()
        assert [[type(a).key for a in g] for g in generations] == [["beta"]]
        # The non-materializable parent stays wired as a dependency.
        assert by_key["alpha"].id in by_key["beta"].deps.values()

    def test_select_unknown_key_raises(self) -> None:
        with pytest.raises(ManifestError, match="has no asset"):
            _manifest(
                f"""
                assets:
                  - source: {SOURCE_PATH}
                    select: [gamma]
                """
            ).compile()

    def test_standalone_asset(self) -> None:
        plan = _manifest(
            f"""
            assets:
              - asset: {ASSET_PATH}
                destinations:
                  - type: {MEMORY_DESTINATION_PATH}
            """
        ).compile()

        assert len(plan.dag.assets) == 1
        assert type(plan.dag.assets[0]).key == "fake_standalone_asset"
        assert isinstance(plan.dag.assets[0].destination, list)
        assert isinstance(plan.dag.assets[0].destination[0], il.MemoryDestination)

    def test_multiple_destinations_fan_out(self, tmp_path: Any) -> None:
        plan = _manifest(
            f"""
            destinations:
              mem: {{type: {MEMORY_DESTINATION_PATH}}}
            assets:
              - source: {SOURCE_PATH}
                destinations:
                  - {{ref: mem}}
                  - type: {FILE_DESTINATION_PATH}
                    config:
                      base_path: {tmp_path}
            """
        ).compile()

        for asset in plan.dag.assets:
            assert isinstance(asset.destination, list)
            types = {type(d) for d in asset.destination}
            assert types == {il.MemoryDestination, il.FileDestination}

    def test_ref_reuses_one_instance(self) -> None:
        plan = _manifest(
            f"""
            destinations:
              mem: {{type: {MEMORY_DESTINATION_PATH}}}
            assets:
              - source: {SOURCE_PATH}
                destinations: [{{ref: mem}}]
              - asset: {ASSET_PATH}
                destinations: [{{ref: mem}}]
            """
        ).compile()

        instances = set()
        for a in plan.dag.assets:
            assert isinstance(a.destination, list)
            instances.add(id(a.destination[0]))
        assert len(instances) == 1  # same instance across both items and all assets

    def test_ref_resolved_inside_config(self) -> None:
        plan = _manifest(
            f"""
            destinations:
              mem: {{type: {MEMORY_DESTINATION_PATH}}}
            assets:
              - source: {SOURCE_PATH}
                config:
                  destination: {{ref: mem}}
            """
        ).compile()

        for asset in plan.dag.assets:
            assert isinstance(asset.destination, il.MemoryDestination)

    def test_unknown_ref_raises(self) -> None:
        with pytest.raises(ManifestError, match="Unknown reference 'nope'"):
            _manifest(
                f"""
                assets:
                  - source: {SOURCE_PATH}
                    destinations: [{{ref: nope}}]
                """
            ).compile()

    def test_resources_alias_must_be_a_resource(self) -> None:
        # A Destination declared under 'resources' is rejected when referenced.
        with pytest.raises(ManifestError, match="is not a Resource"):
            _manifest(
                f"""
                resources:
                  mem: {{type: {MEMORY_DESTINATION_PATH}}}
                assets:
                  - source: {SOURCE_PATH}
                    destinations: [{{ref: mem}}]
                """
            ).compile()

    def test_duplicate_alias_across_blocks_raises(self) -> None:
        with pytest.raises(ManifestError, match="declared in both 'resources' and 'destinations'"):
            _manifest(
                f"""
                resources:
                  dup: {{type: {MEMORY_DESTINATION_PATH}}}
                destinations:
                  dup: {{type: {MEMORY_DESTINATION_PATH}}}
                assets:
                  - source: {SOURCE_PATH}
                    destinations: [{{ref: dup}}]
                """
            ).compile()

    def test_auto_destination_used_when_item_declares_none(self) -> None:
        plan = _manifest(
            f"""
            destinations:
              mem:
                type: {MEMORY_DESTINATION_PATH}
                auto: true
            assets:
              - source: {SOURCE_PATH}
            """
        ).compile()

        for asset in plan.dag.assets:
            assert isinstance(asset.destination, list)
            assert isinstance(asset.destination[0], il.MemoryDestination)

    def test_explicit_destinations_override_auto(self, tmp_path: Any) -> None:
        plan = _manifest(
            f"""
            destinations:
              mem:
                type: {MEMORY_DESTINATION_PATH}
                auto: true
            assets:
              - source: {SOURCE_PATH}
                destinations:
                  - type: {FILE_DESTINATION_PATH}
                    config:
                      base_path: {tmp_path}
            """
        ).compile()

        for asset in plan.dag.assets:
            assert isinstance(asset.destination, list)
            assert {type(d) for d in asset.destination} == {il.FileDestination}  # auto 'mem' not applied

    def test_auto_resource_fills_empty_slot(self) -> None:
        plan = _manifest(
            f"""
            resources:
              tok:
                type: {RESOURCE_PATH}
                auto: true
            assets:
              - source: {RESOURCE_SOURCE_PATH}
            """
        ).compile()

        source = plan.dag.assets[0].source
        assert source is not None
        assert isinstance(source.resources["conn"], FakeManifestResource)

    def test_explicit_resource_overrides_auto(self) -> None:
        plan = _manifest(
            f"""
            resources:
              auto_tok:
                type: {RESOURCE_PATH}
                auto: true
                config: {{token: auto}}
              explicit_tok:
                type: {RESOURCE_PATH}
                config: {{token: explicit}}
            assets:
              - source: {RESOURCE_SOURCE_PATH}
                config:
                  resources:
                    conn: {{ref: explicit_tok}}
            """
        ).compile()

        source = plan.dag.assets[0].source
        assert source is not None
        assert source.resources["conn"].token == "explicit"

    def test_destinations_and_config_destination_conflict_raises(self, tmp_path: Any) -> None:
        with pytest.raises(ManifestError, match="both 'config' and the 'destinations' field"):
            _manifest(
                f"""
                assets:
                  - source: {SOURCE_PATH}
                    config:
                      destination:
                        type: {MEMORY_DESTINATION_PATH}
                    destinations:
                      - type: {MEMORY_DESTINATION_PATH}
                """
            ).compile()

    def test_nested_component_ref_in_config(self, tmp_path: Any) -> None:
        plan = _manifest(
            f"""
            assets:
              - source: {SOURCE_PATH}
                config:
                  destination:
                    type: {FILE_DESTINATION_PATH}
                    config:
                      base_path: {tmp_path}
            """
        ).compile()

        for asset in plan.dag.assets:
            assert isinstance(asset.destination, il.FileDestination)

    def test_partition_and_runner_carried_into_plan(self) -> None:
        plan = _manifest(
            f"""
            runner:
              type: serial
              config:
                fail_fast: true
            assets:
              - source: {SOURCE_PATH}
            partition:
              date: 2026-06-01
            """
        ).compile()

        assert isinstance(plan.partition, TimePartition)
        assert plan.runner is not None
        assert plan.runner.type == "serial"
        assert plan.runner.config == {"fail_fast": True}

    def test_source_path_must_be_source(self) -> None:
        with pytest.raises(ManifestError, match="is not a Source subclass"):
            _manifest(f"assets: [{{source: {ASSET_PATH}}}]").compile()

    def test_asset_path_must_be_asset(self) -> None:
        with pytest.raises(ManifestError, match="is not a Asset subclass"):
            _manifest(f"assets: [{{asset: {SOURCE_PATH}}}]").compile()

    def test_unimportable_path_raises(self) -> None:
        with pytest.raises(ManifestError, match="Failed to import"):
            _manifest("assets: [{source: not_a_module.Nope}]").compile()

    def test_destination_must_be_component(self) -> None:
        with pytest.raises(ManifestError, match="not an interloper component"):
            _manifest(
                f"""
                assets:
                  - source: {SOURCE_PATH}
                    destinations:
                      - type: interloper.cli.manifest.RunManifest
                """
            ).compile()

    def test_destination_must_be_a_destination(self) -> None:
        # A real Component that is not a Destination (a Source) is rejected.
        with pytest.raises(ManifestError, match="is not a Destination"):
            _manifest(
                f"""
                assets:
                  - source: {SOURCE_PATH}
                    destinations:
                      - type: {SOURCE_PATH}
                """
            ).compile()


class TestCatalogResolution:
    """Bare keys resolve through the catalog; dotted paths bypass it."""

    @pytest.fixture
    def fake_catalog(self, monkeypatch: pytest.MonkeyPatch) -> None:
        catalog = Catalog.from_assets([FakeManifestSource])
        monkeypatch.setattr(Catalog, "from_settings", classmethod(lambda cls: catalog))

    def test_bare_key_resolves_via_catalog(self, fake_catalog: None) -> None:
        plan = _manifest("assets: [{source: fake_manifest_source}]").compile()
        assert len(plan.dag.assets) == 2

    def test_unknown_catalog_key_raises(self, fake_catalog: None) -> None:
        with pytest.raises(ManifestError, match="Unknown catalog key 'nope'"):
            _manifest("assets: [{source: nope}]").compile()
