"""Shared fixtures: a seeded in-memory database behind a real Store.

SQLite stands in for Postgres as in the interloper-db tests; the tables
created are exactly those the read-only toolkit and the PAT verifier touch.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from uuid import uuid4

import interloper as il
import pytest
from interloper_db import engine as engine_module
from interloper_db.models import (
    Backfill,
    Component,
    ComponentRelation,
    Organisation,
    PersonalAccessToken,
    Profile,
    Run,
    UserOrganisation,
)
from interloper_db.store import Store
from sqlalchemy import Engine, event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session

from interloper_mcp import context as mcp_context


@pytest.fixture
def mcp_db() -> Iterator[Engine]:
    """A fresh in-memory database with auth, token, component, and run tables."""
    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _configure_connection(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.execute("PRAGMA foreign_keys=ON")
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    for model in (
        Profile,
        Organisation,
        UserOrganisation,
        PersonalAccessToken,
        Component,
        ComponentRelation,
        Backfill,
        Run,
    ):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        yield eng
    finally:
        eng.dispose()
        engine_module._engine = None


@pytest.fixture
def catalog() -> il.Catalog:
    return il.Catalog(components={})


@pytest.fixture
def store(mcp_db: Engine, catalog: il.Catalog) -> Store:
    return Store(catalog=catalog)


@pytest.fixture
def seeded(store: Store) -> dict[str, Any]:
    """An org with one member, a valid PAT, one job component, and one run."""
    profile = store.upsert_profile(google_id="g-user", email="user@example.com", name="User")
    org = store.create_organisation(name="Acme", creator_id=profile.id)
    _, raw_token = store.create_token(profile.id, org.id, name="test")

    job = Component(org_id=org.id, kind="job", key="daily_sync", name="Daily sync", config={"enabled": True})
    run = Run(id=uuid4(), org_id=org.id, component_id=job.id, status="success")
    other_org_job = Component(org_id=uuid4(), kind="job", key="other_org_job")
    with Session(engine_module.get_engine()) as session:
        session.add_all([job, run, other_org_job])
        session.commit()
        session.refresh(job)

    return {"profile": profile, "org": org, "token": raw_token, "job_id": job.id}


@pytest.fixture(autouse=True)
def reset_mcp_context() -> Iterator[None]:
    """Keep the module-level MCP context from leaking between tests."""
    yield
    mcp_context._store = None
    mcp_context._catalog_dump = None
    mcp_context._static_ctx.set(None)
