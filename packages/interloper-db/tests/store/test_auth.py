"""Tests for the auth store (``interloper_db.store.auth``)."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import interloper as il
import pytest
from interloper.errors import NotFoundError
from sqlalchemy import Engine, event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session as SQLSession
from sqlmodel import select

from interloper_db import engine as engine_module
from interloper_db.models import (
    AuthSession,
    Backfill,
    Component,
    ComponentRelation,
    Event,
    Invitation,
    Organisation,
    PersonalAccessToken,
    Profile,
    Quota,
    Run,
    Usage,
    UserOrganisation,
)
from interloper_db.store import Store


@pytest.fixture
def auth_db() -> Iterator[Engine]:
    """A fresh in-memory database with the auth tables, FKs enforced."""
    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _configure_connection(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.execute("PRAGMA foreign_keys=ON")
        # The auth tables use Postgres' gen_random_uuid() as server default.
        # Dashless hex to match how SQLAlchemy's Uuid type binds values on SQLite.
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    auth_models = (Profile, Organisation, UserOrganisation, Invitation, AuthSession, PersonalAccessToken)
    org_data_models = (Component, ComponentRelation, Backfill, Run, Event, Quota, Usage)
    for model in auth_models + org_data_models:
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        yield eng
    finally:
        eng.dispose()
        engine_module._engine = None


@pytest.fixture
def store(auth_db: Engine) -> Store:
    """A store over the in-memory database (no catalog needed for these)."""
    return Store(catalog=il.Catalog(components={}))


class TestAcceptInvitation:
    def test_accept_adds_membership_and_returns_usable_org(self, store: Store):
        admin = store.upsert_profile(google_id="g-admin", email="admin@example.com", name="Admin")
        invitee = store.upsert_profile(google_id="g-invitee", email="new@example.com", name="New")
        org = store.create_organisation(name="Acme", creator_id=admin.id)
        invitation = store.create_invitation(org_id=org.id, email=invitee.email, role="member", invited_by=admin.id)

        joined = store.accept_invitation(invitation.token, invitee.id)

        assert joined is not None
        # Attributes must be loaded on the detached instance (regression:
        # expunging the commit-expired org made any access raise
        # DetachedInstanceError).
        assert joined.id == org.id
        assert joined.name == "Acme"
        assert store.get_user_role(invitee.id, org.id) == "member"
        assert store.get_invitation_by_token(invitation.token) is None

    def test_accept_invalid_token_returns_none(self, store: Store):
        invitee = store.upsert_profile(google_id="g-invitee", email="new@example.com", name="New")

        assert store.accept_invitation("no-such-token", invitee.id) is None


class TestListAllProfiles:
    def test_lists_profiles_with_their_organisations(self, store: Store):
        admin = store.upsert_profile(google_id="g-admin", email="admin@example.com", name="Admin")
        loner = store.upsert_profile(google_id="g-loner", email="loner@example.com", name="Loner")
        org_a = store.create_organisation(name="Acme", creator_id=admin.id)
        org_b = store.create_organisation(name="Beta", creator_id=admin.id)
        store.add_org_member(org_a.id, admin.id, "admin")
        store.add_org_member(org_b.id, admin.id, "admin")

        orgs = {profile.id: organisations for profile, organisations in store.list_all_profiles()}

        assert sorted(org.name for org in orgs[admin.id]) == ["Acme", "Beta"]
        assert orgs[loner.id] == []


class TestDeleteOrganisation:
    def _seed_org_data(self, session: SQLSession, org_id) -> None:
        """Plant one row of every org-owned kind directly (no catalog needed)."""
        source = Component(org_id=org_id, kind="source", key="demo")
        asset = Component(org_id=org_id, kind="asset", key="demo.a", parent_id=source.id)
        session.add(source)
        session.add(asset)
        session.add(
            ComponentRelation(
                src_id=asset.id, dst_id=source.id, org_id=org_id, src_kind="asset", dst_kind="source", type="owner"
            )
        )
        backfill = Backfill(org_id=org_id, start_date=dt.date(2026, 1, 1), end_date=dt.date(2026, 1, 2))
        session.add(backfill)
        session.commit()
        run = Run(org_id=org_id, component_id=source.id)
        session.add(run)
        session.commit()
        session.add(Event(org_id=org_id, run_id=run.id, event_type="run_started", timestamp=datetime.now(timezone.utc)))
        session.commit()

    def test_purges_payload_but_keeps_the_ledger(self, store: Store, auth_db: Engine):
        admin = store.upsert_profile(google_id="g-admin", email="admin@example.com", name="Admin")
        org = store.create_organisation(name="Doomed", creator_id=admin.id)
        keeper = store.create_organisation(name="Keeper", creator_id=admin.id)
        store.add_org_member(org.id, admin.id, "admin")
        store.add_org_member(keeper.id, admin.id, "admin")
        store.create_invitation(org_id=org.id, email="new@example.com", role="viewer", invited_by=admin.id)
        store.create_token(user_id=admin.id, organisation_id=org.id, name="laptop")
        session_token = store.create_session(user_id=admin.id, organisation_id=org.id)
        with SQLSession(auth_db) as session:
            self._seed_org_data(session, org.id)
            db_admin = session.get(Profile, admin.id)
            assert db_admin is not None
            db_admin.last_organisation_id = org.id
            session.add(db_admin)
            session.commit()

        store.delete_organisation(org.id)

        # The org reads as missing everywhere but the row survives, stamped.
        assert store.get_organisation(org.id) is None
        retained = store.get_organisation(org.id, include_deleted=True)
        assert retained is not None and retained.deleted_at is not None
        with SQLSession(auth_db) as session:
            # Sensitive payload is purged...
            for model in (Component, ComponentRelation):
                assert session.exec(select(model).where(model.org_id == org.id)).first() is None
            assert (
                session.exec(select(UserOrganisation).where(UserOrganisation.organisation_id == org.id)).first()
                is None
            )
            assert session.exec(select(Invitation).where(Invitation.organisation_id == org.id)).first() is None
            assert (
                session.exec(select(PersonalAccessToken).where(PersonalAccessToken.organisation_id == org.id)).first()
                is None
            )
            # ...but execution history survives for billing, detached from
            # the purged components via the FK's SET NULL.
            surviving_run = session.exec(select(Run).where(Run.org_id == org.id)).one()
            assert surviving_run.component_id is None
            assert session.exec(select(Backfill).where(Backfill.org_id == org.id)).first() is not None
            assert session.exec(select(Event).where(Event.org_id == org.id)).first() is not None
        # The user, their session, and the other organisation survive; org refs are cleared.
        resolved = store.resolve_session(session_token)
        assert resolved is not None
        profile, auth_session = resolved
        assert auth_session.organisation_id is None
        assert profile.last_organisation_id is None
        assert store.get_organisation(keeper.id) is not None
        assert store.get_user_role(admin.id, keeper.id) == "admin"

    def test_double_delete_reads_as_missing(self, store: Store):
        org = store.create_organisation(name="Once")
        store.delete_organisation(org.id)
        with pytest.raises(NotFoundError):
            store.delete_organisation(org.id)
        with pytest.raises(NotFoundError):
            store.update_organisation(org.id, "Renamed")

    def test_missing_organisation_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.delete_organisation(uuid4())


class TestDeleteProfile:
    def test_deletes_profile_and_everything_anchored_to_it(self, store: Store):
        admin = store.upsert_profile(google_id="g-admin", email="admin@example.com", name="Admin")
        keeper = store.upsert_profile(google_id="g-keeper", email="keeper@example.com", name="Keeper")
        org = store.create_organisation(name="Acme", creator_id=admin.id)
        store.add_org_member(org.id, admin.id, "admin")
        store.add_org_member(org.id, keeper.id, "viewer")
        session_token = store.create_session(user_id=admin.id)
        keeper_token = store.create_session(user_id=keeper.id)
        store.create_token(user_id=admin.id, organisation_id=org.id, name="laptop")
        store.create_invitation(org_id=org.id, email="new@example.com", role="viewer", invited_by=admin.id)

        store.delete_profile(admin.id)

        assert store.get_profile(admin.id) is None
        assert store.resolve_session(session_token) is None
        assert not store.has_pending_invitation("new@example.com")
        assert store.get_user_role(admin.id, org.id) is None
        # Other users' data is untouched.
        assert store.get_profile(keeper.id) is not None
        assert store.resolve_session(keeper_token) is not None
        assert store.get_user_role(keeper.id, org.id) == "viewer"

    def test_missing_profile_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.delete_profile(uuid4())


class TestGetProfileByGoogleId:
    def test_returns_matching_profile(self, store: Store):
        profile = store.upsert_profile(google_id="g-1", email="user@example.com", name="User")

        found = store.get_profile_by_google_id("g-1")

        assert found is not None
        assert found.id == profile.id

    def test_returns_none_when_absent(self, store: Store):
        assert store.get_profile_by_google_id("g-missing") is None


class TestHasPendingInvitation:
    def _invite(self, store: Store, email: str) -> Invitation:
        admin = store.upsert_profile(google_id="g-admin", email="admin@example.com", name="Admin")
        org = store.create_organisation(name="Acme", creator_id=admin.id)
        return store.create_invitation(org_id=org.id, email=email, role="member", invited_by=admin.id)

    def test_pending_invitation_matches_case_insensitively(self, store: Store):
        self._invite(store, "New@Example.com")

        assert store.has_pending_invitation("new@example.com")

    def test_no_invitation_returns_false(self, store: Store):
        assert not store.has_pending_invitation("nobody@example.com")

    def test_expired_invitation_returns_false(self, store: Store, auth_db: Engine):
        invitation = self._invite(store, "new@example.com")

        with SQLSession(auth_db) as session:
            db_invitation = session.get(Invitation, invitation.id)
            assert db_invitation is not None
            db_invitation.expires_at = datetime.now(timezone.utc) - timedelta(days=1)
            session.add(db_invitation)
            session.commit()

        assert not store.has_pending_invitation("new@example.com")


class TestOrganisationActivity:
    def test_composes_and_sorts_the_derived_feed(self, store: Store, auth_db: Engine):
        admin = store.upsert_profile(google_id="g-act", email="act@example.com", name="Act Min")
        org = store.create_organisation(name="Busy", creator_id=admin.id)
        store.add_org_member(org.id, admin.id, "admin")
        store.create_invitation(org_id=org.id, email="new@example.com", role="viewer", invited_by=admin.id)
        with SQLSession(auth_db) as session:
            session.add(Component(org_id=org.id, kind="source", key="bing_ads", name="Bing"))
            session.add(
                Run(
                    id=uuid4(),
                    org_id=org.id,
                    status="success",
                    completed_at=datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc),
                )
            )
            session.add(
                Run(
                    id=uuid4(),
                    org_id=org.id,
                    status="success",
                    completed_at=datetime(2026, 8, 10, 13, 0, tzinfo=timezone.utc),
                )
            )
            session.add(Run(id=uuid4(), org_id=org.id, status="failed"))
            session.commit()

        entries = store.list_organisation_activity(org.id)

        kinds = [entry["kind"] for entry in entries]
        assert set(kinds) == {"org_created", "member_joined", "invitation_sent", "source_added", "runs_completed"}
        whens = [entry["when"] for entry in entries]
        assert whens == sorted(whens, reverse=True)
        assert all(when.tzinfo is not None for when in whens)
        joined = next(entry for entry in entries if entry["kind"] == "member_joined")
        assert joined["subject"] == "Act Min" and joined["extra"] == "admin"
        invited = next(entry for entry in entries if entry["kind"] == "invitation_sent")
        assert invited["subject"] == "new@example.com" and invited["extra"] == "Act Min"
        runs = next(entry for entry in entries if entry["kind"] == "runs_completed")
        assert runs["subject"] == "2"  # only the successful runs, aggregated per day

    def test_limit_caps_the_feed(self, store: Store):
        admin = store.upsert_profile(google_id="g-cap", email="cap@example.com", name="Cap")
        org = store.create_organisation(name="Capped", creator_id=admin.id)
        store.add_org_member(org.id, admin.id, "admin")
        assert len(store.list_organisation_activity(org.id, limit=1)) == 1

    def test_unknown_org_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.list_organisation_activity(uuid4())
