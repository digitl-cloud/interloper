"""Tests for the shared read-only toolkit (``interloper_toolkit``).

A real Store over in-memory SQLite plus a hand-built dumped catalog; the
properties under test are the structured ``status`` contract, org scoping,
and the pure logic (lineage traversal, coverage math, schema search).
"""

from __future__ import annotations

import datetime
from collections.abc import Iterator
from typing import Any
from uuid import uuid4

import interloper as il
import pytest
from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, ComponentRelation, Run
from interloper_db.store import Store
from sqlalchemy import Engine, event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session

from interloper_toolkit import ToolkitContext, analytics, lineage, scheduling
from interloper_toolkit import catalog as catalog_tools

ORG_ID = uuid4()
OTHER_ORG_ID = uuid4()

CATALOG_DUMP: dict[str, Any] = {
    "facebook_ads": {
        "kind": "source",
        "name": "Facebook Ads",
        "assets": [
            {
                "key": "ads",
                "asset_schema": {
                    "properties": {
                        "campaign_id": {"type": "string", "description": "Campaign identifier"},
                        "spend": {"type": "number", "description": "Total spend"},
                    }
                },
            },
        ],
    },
    "google_ads": {
        "kind": "source",
        "name": "Google Ads",
        "assets": [
            {
                "key": "campaigns",
                "asset_schema": {
                    "properties": {
                        "campaign_id": {"type": "string", "description": "Campaign identifier"},
                        "clicks": {"type": "integer", "description": "Click count"},
                    }
                },
            },
        ],
    },
}


@pytest.fixture
def toolkit_db() -> Iterator[Engine]:
    """A fresh in-memory database with component, relation, and run tables."""
    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _configure_connection(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.execute("PRAGMA foreign_keys=ON")
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    for model in (Component, ComponentRelation, Backfill, Run):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        yield eng
    finally:
        eng.dispose()
        engine_module._engine = None


@pytest.fixture
def ctx(toolkit_db: Engine) -> ToolkitContext:
    store = Store(catalog=il.Catalog(components={}))
    return ToolkitContext(store=store, catalog=CATALOG_DUMP, org_id=ORG_ID)


def _seed_chain(org_id: Any = ORG_ID) -> dict[str, Any]:
    """Seed source→assets a→b→c (b depends on a, c depends on b)."""
    source = Component(org_id=org_id, kind="source", key="facebook_ads")
    a = Component(org_id=org_id, kind="asset", key="a", parent_id=source.id)
    b = Component(org_id=org_id, kind="asset", key="b", parent_id=source.id)
    c = Component(org_id=org_id, kind="asset", key="c", parent_id=source.id)
    deps = [
        ComponentRelation(
            src_id=b.id, dst_id=a.id, type="dependency", slot="a", org_id=org_id, src_kind="asset", dst_kind="asset"
        ),
        ComponentRelation(
            src_id=c.id, dst_id=b.id, type="dependency", slot="b", org_id=org_id, src_kind="asset", dst_kind="asset"
        ),
    ]
    ids = {"source": source.id, "a": a.id, "b": b.id, "c": c.id}
    with Session(engine_module.get_engine()) as session:
        session.add_all([source, a, b, c, *deps])
        session.commit()
    return ids


class TestLineage:
    def test_full_upstream_lineage_walks_the_chain(self, ctx: ToolkitContext):
        ids = _seed_chain()

        result = lineage.get_full_lineage(ctx, str(ids["c"]), direction="upstream")

        assert result["status"] == "success"
        assert result["lineage_count"] == 2
        assert [(item["asset_key"], item["depth"]) for item in result["lineage"]] == [("b", 1), ("a", 2)]

    def test_impact_analysis_groups_downstream_by_source(self, ctx: ToolkitContext):
        ids = _seed_chain()

        result = lineage.impact_analysis(ctx, str(ids["a"]))

        assert result["status"] == "success"
        assert result["total_affected"] == 2
        assert {i["asset_key"] for i in result["by_source"]["facebook_ads"]} == {"b", "c"}

    def test_other_orgs_edges_are_invisible(self, ctx: ToolkitContext):
        ids = _seed_chain(org_id=OTHER_ORG_ID)

        result = lineage.get_full_lineage(ctx, str(ids["c"]), direction="upstream")

        assert result["status"] == "success"
        assert result["lineage_count"] == 0


class TestCatalog:
    def test_search_fields_matches_across_sources(self, ctx: ToolkitContext):
        result = catalog_tools.search_fields(ctx, "campaign")

        assert result["status"] == "success"
        assert result["match_count"] == 2
        assert {m["qualified_key"] for m in result["matches"]} == {"facebook_ads.ads", "google_ads.campaigns"}

    def test_compare_schemas_reports_shared_and_unique(self, ctx: ToolkitContext):
        result = catalog_tools.compare_schemas(ctx, "facebook_ads", "ads", "google_ads", "campaigns")

        assert result["status"] == "success"
        assert result["shared_fields"][0]["field"] == "campaign_id"
        assert result["only_in_a"] == ["spend"]
        assert result["only_in_b"] == ["clicks"]

    def test_unknown_definition_is_a_structured_error(self, ctx: ToolkitContext):
        result = catalog_tools.get_definition(ctx, "nope")

        assert result["status"] == "error"


class TestAnalytics:
    def test_partition_coverage_reports_missing_dates(self, ctx: ToolkitContext):
        job = Component(org_id=ORG_ID, kind="job", key="daily")
        job_id = job.id
        runs = [
            Run(id=uuid4(), org_id=ORG_ID, component_id=job_id, status="success", partition_date=date)
            for date in (datetime.date(2026, 7, 1), datetime.date(2026, 7, 3))
        ]
        with Session(engine_module.get_engine()) as session:
            session.add_all([job, *runs])
            session.commit()

        result = analytics.partition_coverage(ctx, str(job_id), "2026-07-01", "2026-07-03")

        assert result["status"] == "success"
        assert result["covered_days"] == 2
        assert result["missing_dates"] == ["2026-07-02"]


class TestScheduling:
    def test_get_job_health_computes_success_rate(self, ctx: ToolkitContext):
        job = Component(org_id=ORG_ID, kind="job", key="daily")
        job_id = job.id
        now = datetime.datetime(2026, 7, 16, 12, 0, tzinfo=datetime.timezone.utc)
        runs = [
            Run(
                id=uuid4(),
                org_id=ORG_ID,
                component_id=job_id,
                status=status,
                started_at=now,
                completed_at=now + datetime.timedelta(seconds=60),
            )
            for status in ("success", "success", "failed")
        ]
        with Session(engine_module.get_engine()) as session:
            session.add_all([job, *runs])
            session.commit()

        result = scheduling.get_job_health(ctx, str(job_id))

        assert result["status"] == "success"
        assert result["health"]["success_rate"] == 0.67
        assert result["health"]["avg_duration_seconds"] == 60.0
