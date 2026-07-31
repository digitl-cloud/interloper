"""Tests for the auth store (``interloper_db.store.auth``)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import interloper as il
import pytest
from sqlalchemy import Engine, event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session as SQLSession

from interloper_db import engine as engine_module
from interloper_db.models import Invitation, Organisation, Profile, UserOrganisation
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

    for model in (Profile, Organisation, UserOrganisation, Invitation):
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
    def test_lists_profiles_with_membership_counts(self, store: Store):
        admin = store.upsert_profile(google_id="g-admin", email="admin@example.com", name="Admin")
        loner = store.upsert_profile(google_id="g-loner", email="loner@example.com", name="Loner")
        org_a = store.create_organisation(name="Acme", creator_id=admin.id)
        org_b = store.create_organisation(name="Beta", creator_id=admin.id)
        store.add_org_member(org_a.id, admin.id, "admin")
        store.add_org_member(org_b.id, admin.id, "admin")

        counts = {profile.id: count for profile, count in store.list_all_profiles()}

        assert counts[admin.id] == 2
        assert counts[loner.id] == 0


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
