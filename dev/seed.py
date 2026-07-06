#!/usr/bin/env python
"""Seed a local/dev interloper database with a minimal, loginnable dataset.

Creates (idempotently):

- a super-admin :class:`Profile` for the dev user (see ``INTERLOPER_DEV_USER``),
- one :class:`Organisation` with that profile as ``admin`` member,
- the ``demo`` catalog source and its assets (the ``a -> b,c,d -> e`` DAG),
- a daily partitioned :class:`Job` over the demo source.

It reuses the same Store/CLI APIs the app uses — no hand-rolled row inserts —
so the seeded shape stays in sync with production code paths. Safe to re-run:
every step is guarded so repeated invocations neither error nor duplicate.

``INTERLOPER_DEV_USER_EMAIL`` (default ``admin@dev.local``) and
``INTERLOPER_DEV_USER_GOOGLE_ID`` (optional) name the dev user. Login resolves a
profile by Google ``google_id``, not email — so setting the google id makes the
seed write the **exact** profile your login lands on: super-admin out of the box,
no duplicate. Without the id, the seed matches an existing profile by email (e.g.
one created by a prior login, whose google id it prints so you can set it) or
creates a synthetic placeholder.

Run directly against a reachable Postgres (host path or inside the compose
``seed`` service)::

    INTERLOPER_DEV_USER_EMAIL=you@example.com \
    INTERLOPER_DEV_USER_GOOGLE_ID=1234567890 uv run python dev/seed.py

Connection and catalog come from settings (``interloper.yaml`` + ``INTERLOPER_*``
env), so point those at the target database before running.
"""

from __future__ import annotations

import os
from uuid import UUID

import interloper as il
from interloper.settings import AppSettings
from interloper_db import create_all, ensure_database, init_engine
from interloper_db.engine import get_engine
from interloper_db.models import Component, Organisation, Profile, UserOrganisation
from interloper_db.store import Store
from sqlmodel import Session, col, select

# -- Seed identity (stable so re-runs are idempotent) -------------------------

# The dev user to make super-admin. Set INTERLOPER_DEV_USER_GOOGLE_ID to your
# Google subject id so the profile the seed creates is the exact one your login
# resolves to (login matches on google_id, not email) — super-admin out of the
# box, no duplicate profile. Without it the seed matches/creates by email only.
DEV_USER_EMAIL = os.environ.get("INTERLOPER_DEV_USER_EMAIL", "admin@dev.local")
DEV_USER_GOOGLE_ID = (os.environ.get("INTERLOPER_DEV_USER_GOOGLE_ID") or "").strip() or None

# google_id prefix for the placeholder profiles this script creates when no real
# google_id is configured (vs. ids from an actual Google login).
SYNTHETIC_GOOGLE_ID_PREFIX = "dev:"

ORG_NAME = "Dev Org"

DEMO_SOURCE_KEY = "demo_source"  # catalog key of interloper_assets.demo.source.DemoSource
DEMO_SOURCE_NAME = "Demo Data"

DEMO_JOB_NAME = "Demo Daily"
DEMO_JOB_CRON = "0 6 * * *"  # every day at 06:00


def _is_synthetic(profile: Profile) -> bool:
    """True for profiles this seed created, vs. ones from a real Google login."""
    return (profile.google_id or "").startswith(SYNTHETIC_GOOGLE_ID_PREFIX)


def _upsert_by_google_id(google_id: str, email: str) -> Profile:
    """Create or promote the profile keyed by ``google_id``.

    This is the id a Google login resolves on (``upsert_profile`` in the auth
    route), so seeding it means the login lands on this exact row — no second
    profile. An existing profile keeps its name/email; only super-admin is set.
    """
    with Session(get_engine()) as session:
        profile = session.exec(select(Profile).where(Profile.google_id == google_id)).first()
        if profile is None:
            profile = Profile(email=email, name=email.split("@", 1)[0], google_id=google_id)
            session.add(profile)
        profile.is_super_admin = True
        session.add(profile)
        session.commit()
        session.refresh(profile)
        session.expunge(profile)
        return profile


def _remove_synthetic_duplicates(email: str, keep_id: UUID) -> int:
    """Delete leftover synthetic (``dev:``) profiles for ``email`` + their memberships."""
    with Session(get_engine()) as session:
        dupes = [
            profile
            for profile in session.exec(
                select(Profile).where(
                    Profile.email == email,
                    col(Profile.google_id).like(f"{SYNTHETIC_GOOGLE_ID_PREFIX}%"),
                )
            ).all()
            if profile.id != keep_id
        ]
        if not dupes:
            return 0

        dupe_ids = [profile.id for profile in dupes]
        # Drop FK references (org memberships) and flush before deleting the
        # profiles, so the user_organisations rows are gone first.
        for membership in session.exec(
            select(UserOrganisation).where(col(UserOrganisation.user_id).in_(dupe_ids))
        ).all():
            session.delete(membership)
        session.flush()
        for profile in dupes:
            session.delete(profile)
        session.commit()
        return len(dupes)


def _ensure_super_admin(store: Store) -> tuple[Profile, str]:
    """Ensure the dev super-admin profile. Returns ``(profile, mode)``.

    ``mode`` is ``"google_id"`` (keyed by the configured real id — login matches
    out of the box), ``"matched"`` (promoted an existing profile found by email),
    or ``"synthetic"`` (created a placeholder).
    """
    if DEV_USER_GOOGLE_ID is not None:
        profile = _upsert_by_google_id(DEV_USER_GOOGLE_ID, DEV_USER_EMAIL)
        assert profile.id is not None
        _remove_synthetic_duplicates(DEV_USER_EMAIL, keep_id=profile.id)
        return profile, "google_id"

    with Session(get_engine()) as session:
        rows = session.exec(
            select(Profile).where(Profile.email == DEV_USER_EMAIL).order_by(col(Profile.created_at))
        ).all()
        # Prefer a real (Google-authenticated) profile over a synthetic dev one.
        real = [p for p in rows if not _is_synthetic(p)]
        chosen = real[0] if real else (rows[0] if rows else None)
        if chosen is not None:
            session.expunge(chosen)

    if chosen is not None:
        assert chosen.id is not None
        store.set_super_admin(chosen.id)
        return chosen, "matched"

    # No profile with this email yet — create a synthetic placeholder. A real
    # Google login (matched on google_id) would otherwise create its own row;
    # re-run with INTERLOPER_DEV_USER_GOOGLE_ID, or log in then re-run.
    name = DEV_USER_EMAIL.split("@", 1)[0]
    profile = store.upsert_profile(
        google_id=f"{SYNTHETIC_GOOGLE_ID_PREFIX}{DEV_USER_EMAIL}",
        email=DEV_USER_EMAIL,
        name=name,
    )
    assert profile.id is not None
    store.set_super_admin(profile.id)
    return profile, "synthetic"


def _ensure_org(store: Store, admin_id: UUID) -> Organisation:
    """Return the seed org, creating it (with admin as member) if absent."""
    with Session(get_engine()) as session:
        existing = session.exec(select(Organisation).where(Organisation.name == ORG_NAME)).first()
        if existing:
            session.expunge(existing)

    if existing is None:
        return store.create_organisation(name=ORG_NAME, creator_id=admin_id)

    # Org already there — make sure the admin is still a member.
    assert existing.id is not None  # persisted row always has a PK
    if store.get_user_role(admin_id, existing.id) is None:
        with Session(get_engine()) as session:
            session.add(UserOrganisation(user_id=admin_id, organisation_id=existing.id, role="admin"))
            session.commit()
    return existing


def _ensure_demo_source(store: Store, org_id: UUID) -> Component:
    """Register the demo source (and its assets) for the org if not present.

    Returns the seeded source with its assets eagerly loaded.
    """
    sources = {source.key: source for source in store.list_sources(org_id)}
    if DEMO_SOURCE_KEY not in sources:
        return store.create_source(org_id, key=DEMO_SOURCE_KEY, name=DEMO_SOURCE_NAME)
    return sources[DEMO_SOURCE_KEY]


def _ensure_demo_job(store: Store, org_id: UUID, source_id: UUID) -> bool:
    """Create a daily partitioned job over the demo source if absent.

    Returns True if it created the job, False if one already existed.
    """
    if any(job.name == DEMO_JOB_NAME for job in store.list_jobs(org_id)):
        return False
    store.create_job(
        org_id,
        name=DEMO_JOB_NAME,
        cron=DEMO_JOB_CRON,
        source_ids=[source_id],
        partitioned=True,
    )
    return True


def main() -> None:
    """Provision the dev dataset against the configured database."""
    settings = AppSettings.get()
    catalog = il.Catalog.from_settings()

    dsn = settings.postgres.dsn
    print(f"Seeding database at {dsn} ...")
    ensure_database(dsn)
    engine = init_engine(dsn)
    create_all(engine)

    store = Store.from_settings(catalog)

    admin, mode = _ensure_super_admin(store)
    assert admin.id is not None  # populated on insert; narrows Optional PK for type-checkers
    org = _ensure_org(store, admin.id)
    assert org.id is not None
    source = _ensure_demo_source(store, org.id)
    assert source.id is not None
    asset_count = len(source.children)
    job_created = _ensure_demo_job(store, org.id, source.id)

    how = {
        "google_id": "keyed by google_id — your login will match",
        "matched": "matched existing profile by email",
        "synthetic": "created synthetic placeholder",
    }[mode]
    print("Seed complete:")
    print(f"  org    : {org.name} ({org.id})")
    print(f"  admin  : {DEV_USER_EMAIL} ({admin.id}) [super_admin] ({how})")
    print(f"  source : {DEMO_SOURCE_KEY} -> {DEMO_SOURCE_NAME} ({asset_count} assets)")
    print(f"  job    : {DEMO_JOB_NAME} (cron {DEMO_JOB_CRON}, {'created' if job_created else 'exists'})")
    if mode == "matched":
        print(
            f"  hint   : set INTERLOPER_DEV_USER_GOOGLE_ID={admin.google_id} (e.g. in .env) so a\n"
            f"           fresh seed matches your login without creating a second profile."
        )
    elif mode == "synthetic":
        print(
            f"  note   : no profile for {DEV_USER_EMAIL} yet. Log in via Google as that user,\n"
            f"           then re-run the seed (it prints your google_id to set in .env)."
        )


if __name__ == "__main__":
    main()
