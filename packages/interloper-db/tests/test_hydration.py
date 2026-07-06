"""Round-trip tests: store writes → generic hydration → live framework objects.

These exercise the full pipeline against a real (SQLite) database using the
real ``DemoSource`` catalog component: create rows through the store facades,
hydrate through the one generic spec builder, and assert on the reconstructed
framework instances.
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
        db_source = store.create_source(_ORG, key="demo_source", name="Demo")
        assert db_source.kind == "source"
        assert sorted(child.key for child in db_source.children) == ["a", "b", "c", "d", "e"]

        deps = store.list_dependencies(_ORG)
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

    def test_load_source_hydrates_with_stable_ids_and_deps(self, store: Store):
        db_source = store.create_source(_ORG, key="demo_source", name="Demo", config={"hello": "there"})
        source = store.load_source(db_source.id)

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

    def test_update_asset_materializable_survives_config_replacement(self, store: Store):
        db_source = store.create_source(_ORG, key="demo_source", name="Demo")
        child = next(child for child in db_source.children if child.key == "a")

        store.update_asset(child.id, materializable=False)
        db_asset = store.update_asset(child.id, config={"dataset": "override"})
        assert db_asset.config == {"materializable": False, "dataset": "override"}

        source = store.load_source(db_source.id)
        asset_a = next(asset for asset in source.assets if type(asset).key == "a")
        assert asset_a.materializable is False

    def test_update_source_asset_selection_drops_rows_and_relations(self, store: Store):
        db_source = store.create_source(_ORG, key="demo_source", name="Demo")
        store.update_source(db_source.id, asset_keys=["a", "b"])

        refreshed = store.get_source(db_source.id)
        assert sorted(child.key for child in store.list_assets(_ORG)) == ["a", "b"]
        assert refreshed.id == db_source.id
        # e (and its dependency relations) are gone; b keeps its dep on a.
        remaining = store.list_dependencies(_ORG)
        assert [rel.slot for rel in remaining] == ["a"]


class TestStandaloneAsset:
    """Standalone assets hydrate directly through the generic builder."""

    def test_create_and_load(self, store: Store):
        db_asset = store.create_asset(_ORG, key="demo_asset", config={"materializable": False})
        asset = store.load_asset(db_asset.id)
        assert type(asset).key == "demo_asset"
        assert asset.id == str(db_asset.id)
        assert asset.materializable is False


class TestJobRoundTrip:
    """Jobs persist as components with target relations and hydrate to core Jobs."""

    def test_create_and_record(self, store: Store):
        db_source = store.create_source(_ORG, key="demo_source", name="Demo")
        record = store.create_job(
            _ORG,
            name="Demo Daily",
            cron="0 6 * * *",
            source_ids=[db_source.id],
            tags=["daily"],
            partitioned=True,
            backfill_days=7,
        )
        assert record.name == "Demo Daily"
        assert record.cron == "0 6 * * *"
        assert record.enabled is True
        assert record.partitioned is True
        assert record.backfill_days == 7
        assert record.source_ids == [db_source.id]
        assert record.asset_ids == []
        assert record.next_run_at is None

        assert [job.id for job in store.list_jobs(_ORG)] == [record.id]

    def test_load_job_hydrates_targets(self, store: Store):
        db_source = store.create_source(_ORG, key="demo_source", name="Demo")
        db_asset = store.create_asset(_ORG, key="demo_asset")
        record = store.create_job(
            _ORG,
            name="Demo Daily",
            cron="0 6 * * *",
            source_ids=[db_source.id],
            asset_ids=[db_asset.id],
        )

        job = store.load_job(record.id)
        assert isinstance(job, il.Job)
        assert job.cron == "0 6 * * *"
        assert {type(target).key for target in job.targets} == {"demo_source", "demo_asset"}
        assert {type(asset).key for asset in job.dag().assets} == {"a", "b", "c", "d", "e", "demo_asset"}

    def test_update_job_preserves_state(self, store: Store):
        record = store.create_job(_ORG, name="Job", cron="0 6 * * *")

        # Simulate the scheduler's targeted state write.
        from sqlmodel import Session

        from interloper_db.engine import get_engine
        from interloper_db.models import Component

        with Session(get_engine()) as session:
            db_job = session.get(Component, record.id)
            assert db_job is not None
            db_job.state = {"next_run_at": "2026-07-07T06:00:00+00:00"}
            session.add(db_job)
            session.commit()

        updated = store.update_job(record.id, name="Renamed", cron="0 7 * * *")
        assert updated.name == "Renamed"
        assert updated.next_run_at is not None
        assert updated.next_run_at.isoformat() == "2026-07-07T06:00:00+00:00"
