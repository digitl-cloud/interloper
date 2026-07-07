"""Tests for the auth store (``interloper_db.store.auth``)."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from uuid import uuid4

import interloper as il
import pytest
from sqlalchemy import Engine, event
from sqlalchemy.pool import StaticPool

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
