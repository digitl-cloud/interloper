#!/usr/bin/env python
"""Seed a local/dev interloper database with a minimal, loginnable dataset.

Creates (idempotently):

- a super-admin :class:`Profile` for the dev user (see ``INTERLOPER_DEV_USER``),
- the repository's dev :class:`Organisation`, with that profile as ``admin``
  member: the one ``interloper dev`` serves the repository in, found by the
  same key,
- unless ``--no-demo``: the ``demo`` catalog source and its assets (the
  ``a -> b,c,d -> e`` DAG) and a daily partitioned :class:`Job` over it.

The host path (``make dev-reset``) seeds with ``--no-demo``: ``make dev-up``
runs ``interloper dev -f collection.yaml``, which declares the demo source and
its job itself. The compose stack, which cannot run ``interloper dev``, and
the screenshot tooling built on it seed the demo here.

It reuses the same Store/CLI APIs the app uses — no hand-rolled row inserts —
so the seeded shape stays in sync with production code paths. Safe to re-run:
every step is guarded so repeated invocations neither error nor duplicate.

``INTERLOPER_DEV_USER_EMAIL`` (default ``admin@dev.local``) names the dev user.
The seed promotes the profile holding that address, or provisions one holding
it that nothing signs in to yet. The first login whose address such a profile
holds claims it, so your first login with that address, through any provider,
lands on the seeded profile: super-admin in the seed org, no duplicate, and no
mail server needed.

Run directly against a reachable Postgres (host path or inside the compose
``seed`` service)::

    INTERLOPER_DEV_USER_EMAIL=you@example.com uv run python dev/seed.py

Connection and catalog come from settings (``interloper.yaml`` + ``INTERLOPER_*``
env), so point those at the target database before running.
"""

from __future__ import annotations

import argparse
import os
from uuid import UUID

import interloper as il
from interloper_db import create_all, ensure_database, init_engine
from interloper_db.models import Component, Organisation, Profile
from interloper_db.organisation_keys import derive_key
from interloper_db.settings import PostgresSettings
from interloper_db.store import ComponentQuery, ProfileQuery, Store

# -- Seed identity (stable so re-runs are idempotent) -------------------------

DEV_USER_EMAIL = os.environ.get("INTERLOPER_DEV_USER_EMAIL", "admin@dev.local")

ORG_NAME = "Dev Org"
# The key `interloper dev` derives from the workspace's [project].name, so both find the same organisation.
ORG_KEY = derive_key("interloper-workspace")

DEMO_SOURCE_KEY = "demo_source"  # catalog key of interloper_assets.demo.basic.source.DemoSource
DEMO_SOURCE_NAME = "Demo Data"

DEMO_JOB_NAME = "Demo Daily"
DEMO_JOB_CRON = "0 6 * * *"  # every day at 06:00


def _ensure_super_admin(store: Store) -> tuple[Profile, bool]:
    """Promote the oldest profile holding the dev user's address, provisioning one when none does.

    Returns ``(profile, created)``.
    """
    holders = store.profiles.list(ProfileQuery(email=DEV_USER_EMAIL, limit=None)).items
    created = not holders
    profile = (
        store.profiles.provision(email=DEV_USER_EMAIL, name=DEV_USER_EMAIL.split("@", 1)[0]) if created else holders[0]
    )
    assert profile.id is not None
    return store.profiles.set_super_admin(profile.id, value=True), created


def _ensure_org(store: Store, admin_id: UUID) -> Organisation:
    """Return the repository's dev org, creating it (with admin as member) if absent."""
    return store.organisations.ensure(ORG_NAME, key=ORG_KEY, admin_id=admin_id)


def _ensure_demo_source(store: Store, org_id: UUID) -> Component:
    """Register the demo source (and its assets) for the org if not present.

    Returns the seeded source with its assets eagerly loaded.
    """
    query = ComponentQuery(kind=["source"], roots_only=False, limit=None)
    sources = {source.key: source for source in store.components.list(org_id, query).items}
    if DEMO_SOURCE_KEY not in sources:
        return store.components.create(org_id, kind="source", key=DEMO_SOURCE_KEY, name=DEMO_SOURCE_NAME)
    return sources[DEMO_SOURCE_KEY]


def _ensure_demo_job(store: Store, org_id: UUID, source_id: UUID) -> bool:
    """Create a daily partitioned job over the demo source if absent.

    Returns True if it created the job, False if one already existed.
    """
    jobs = store.components.list(org_id, ComponentQuery(kind=["job"], roots_only=False, limit=None)).items
    if any(job.name == DEMO_JOB_NAME for job in jobs):
        return False
    store.components.create(
        org_id,
        kind="job",
        key="cron_job",
        name=DEMO_JOB_NAME,
        config={"cron": DEMO_JOB_CRON, "enabled": True},
        relations={"targets": [source_id]},
    )
    return True


def main() -> None:
    """Provision the dev dataset against the configured database."""
    parser = argparse.ArgumentParser(description="Seed a local/dev interloper database.")
    parser.add_argument("--no-demo", action="store_true", help="Seed the dev user and organisation only")
    demo = not parser.parse_args().no_demo
    postgres = PostgresSettings.get()
    catalog = il.Catalog.from_settings()

    dsn = postgres.dsn
    print(f"Seeding database at {dsn} ...")
    ensure_database(dsn)
    engine = init_engine(dsn)
    create_all(engine)

    store = Store.from_settings(catalog)

    admin, created = _ensure_super_admin(store)
    assert admin.id is not None  # populated on insert; narrows Optional PK for type-checkers
    org = _ensure_org(store, admin.id)
    assert org.id is not None

    how = "provisioned, your first login claims it" if created else "matched the profile holding the address"
    print("Seed complete:")
    print(f"  org    : {org.name} ({org.id})")
    print(f"  admin  : {DEV_USER_EMAIL} ({admin.id}) [super_admin] ({how})")
    if not demo:
        return
    source = _ensure_demo_source(store, org.id)
    assert source.id is not None
    asset_count = len(source.children)
    job_created = _ensure_demo_job(store, org.id, source.id)
    print(f"  source : {DEMO_SOURCE_KEY} -> {DEMO_SOURCE_NAME} ({asset_count} assets)")
    print(f"  job    : {DEMO_JOB_NAME} (cron {DEMO_JOB_CRON}, {'created' if job_created else 'exists'})")


if __name__ == "__main__":
    main()
