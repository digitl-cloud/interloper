"""Round-trip tests: generic store writes → generic hydration → live framework objects.

These exercise the full pipeline against a real (SQLite) database using the
real ``DemoSource`` catalog component: create rows through the generic store
surface, hydrate through the one generic spec builder, and assert on the
reconstructed framework instances.
"""

from __future__ import annotations

from uuid import uuid4

import interloper as il
import pytest
from interloper_assets.demo.source import DemoSource, demo_asset
from sqlalchemy import Engine

from interloper_db.store import Store

_ORG = uuid4()
_CATALOG = il.Catalog.from_assets([DemoSource, demo_asset])


@pytest.fixture
def store(component_db: Engine) -> Store:
    """A store over the in-memory database with the demo catalog."""
    return Store(catalog=_CATALOG)


class TestSourceRoundTrip:
    """Sources with child assets, intra-source deps, and overrides."""

    def test_create_source_creates_children_and_intra_deps(self, store: Store):
        db_source = store.create_component(_ORG, kind="source", key="demo_source", name="Demo")
        assert db_source.kind == "source"
        assert sorted(child.key for child in db_source.children) == ["a", "b", "c", "d", "e"]

        deps = store.list_relations(_ORG, type="dependency")
        by_child = {}
        children_by_id = {child.id: child.key for child in db_source.children}
        for rel in deps:
            by_child.setdefault(children_by_id[rel.src_id], set()).add((rel.slot, children_by_id[rel.dst_id]))
        assert by_child == {
            "b": {("a", "a")},
            "c": {("a", "a")},
            "d": {("a", "a")},
            "e": {("b", "b"), ("c", "c"), ("d", "d")},
        }

    def test_load_hydrates_with_stable_ids_and_deps(self, store: Store):
        db_source = store.create_component(
            _ORG, kind="source", key="demo_source", name="Demo", config={"hello": "there"}
        )
        source = store.load(db_source.id)

        assert isinstance(source, DemoSource)
        assert source.id == str(db_source.id)
        assert source.hello == "there"

        rows_by_key = {child.key: str(child.id) for child in db_source.children}
        assets_by_key = {type(asset).key: asset for asset in source.assets}
        assert {key: asset.id for key, asset in assets_by_key.items()} == rows_by_key
        assert assets_by_key["e"].deps == {
            "b": rows_by_key["b"],
            "c": rows_by_key["c"],
            "d": rows_by_key["d"],
        }

    def test_source_owned_asset_loads_through_its_parent(self, store: Store):
        db_source = store.create_component(_ORG, kind="source", key="demo_source", name="Demo")
        child = next(child for child in db_source.children if child.key == "a")
        store.update_component(child.id, config={"materializable": False})

        asset = store.load(child.id)
        assert isinstance(asset, il.Asset)
        assert type(asset).key == "a"
        assert asset.materializable is False

    def test_children_selection_drops_rows_and_relations(self, store: Store):
        db_source = store.create_component(_ORG, kind="source", key="demo_source", name="Demo")
        store.update_component(db_source.id, children=["a", "b"])

        refreshed = store.get_component(db_source.id, kind="source")
        assert sorted(child.key for child in refreshed.children) == ["a", "b"]
        # e (and its dependency relations) are gone; b keeps its dep on a.
        remaining = store.list_relations(_ORG, type="dependency")
        assert [rel.slot for rel in remaining] == ["a"]


class TestStandaloneAsset:
    """Standalone assets hydrate directly through the generic builder."""

    def test_create_and_load(self, store: Store):
        db_asset = store.create_component(_ORG, kind="asset", key="demo_asset", config={"materializable": False})
        asset = store.load(db_asset.id)
        assert isinstance(asset, il.Asset)
        assert type(asset).key == "demo_asset"
        assert asset.id == str(db_asset.id)
        assert asset.materializable is False


class TestJobRoundTrip:
    """Jobs persist as components with target relations and hydrate to core Jobs."""

    def test_create_and_read_back(self, store: Store):
        db_source = store.create_component(_ORG, kind="source", key="demo_source", name="Demo")
        db_job = store.create_component(
            _ORG,
            kind="job",
            key="job",
            name="Demo Daily",
            config={"cron": "0 6 * * *", "tags": ["daily"], "enabled": True, "partitioned": True},
            relations={"target": [(db_source.id, "")]},
        )
        assert db_job.name == "Demo Daily"
        assert db_job.config == {"cron": "0 6 * * *", "tags": ["daily"], "enabled": True, "partitioned": True}
        assert db_job.state is None
        assert [rel.dst_id for rel in db_job.out_relations] == [db_source.id]

        assert [job.id for job in store.list_components(_ORG, kinds=["job"])] == [db_job.id]

    def test_load_hydrates_targets(self, store: Store):
        db_source = store.create_component(_ORG, kind="source", key="demo_source", name="Demo")
        db_asset = store.create_component(_ORG, kind="asset", key="demo_asset")
        db_job = store.create_component(
            _ORG,
            kind="job",
            key="job",
            name="Demo Daily",
            config={"cron": "0 6 * * *"},
            relations={"target": [(db_source.id, ""), (db_asset.id, "")]},
        )

        job = store.load(db_job.id)
        assert isinstance(job, il.Job)
        assert job.cron == "0 6 * * *"
        assert {type(target).key for target in job.targets} == {"demo_source", "demo_asset"}
        assert {type(asset).key for asset in job.dag().assets} == {"a", "b", "c", "d", "e", "demo_asset"}

    def test_update_preserves_state(self, store: Store):
        db_job = store.create_component(_ORG, kind="job", key="job", name="Job", config={"cron": "0 6 * * *"})

        # Simulate the scheduler's targeted state write.
        from sqlmodel import Session

        from interloper_db.engine import get_engine
        from interloper_db.models import Component

        with Session(get_engine()) as session:
            row = session.get(Component, db_job.id)
            assert row is not None
            row.state = {"next_run_at": "2026-07-07T06:00:00+00:00"}
            session.add(row)
            session.commit()

        updated = store.update_component(db_job.id, name="Renamed", config={"cron": "0 7 * * *"})
        assert updated.name == "Renamed"
        assert updated.state == {"next_run_at": "2026-07-07T06:00:00+00:00"}
