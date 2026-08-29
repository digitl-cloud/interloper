"""Tests for the token store (``interloper_db.store.tokens``)."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import interloper as il
import pytest
from interloper.errors import NotFoundError
from sqlalchemy import Engine, event
from sqlalchemy.pool import StaticPool

from interloper_db import engine as engine_module
from interloper_db.models import Organisation, PersonalAccessToken, Profile, UserOrganisation
from interloper_db.store import Store
from interloper_db.store.tokens import TOKEN_PREFIX_LEN


@pytest.fixture
def token_db() -> Iterator[Engine]:
    """A fresh in-memory database with the auth + token tables, FKs enforced.

    Yields:
        The engine bound to that database, disposed once the test finishes.
    """
    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _configure_connection(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.execute("PRAGMA foreign_keys=ON")
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    for model in (Profile, Organisation, UserOrganisation, PersonalAccessToken):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        yield eng
    finally:
        eng.dispose()
        engine_module._engine = None


@pytest.fixture
def store(token_db: Engine) -> Store:
    """A store over the in-memory database (no catalog needed for these).

    Returns:
        A store with an empty catalog, reading and writing the fixture database.
    """
    return Store(catalog=il.Catalog(components={}))


@pytest.fixture
def member(store: Store) -> tuple[Profile, Organisation]:
    """A profile that is an admin member of a fresh organisation.

    Returns:
        The profile and the organisation it was made admin of, in that order.
    """
    profile = store.auth.upsert_profile(google_id="g-user", email="user@example.com", name="User")
    org = store.auth.create_organisation(name="Acme", creator_id=profile.id)
    return profile, org


class TestCreateToken:
    def test_create_returns_raw_and_stores_only_hash(self, store: Store, member: tuple[Profile, Organisation]):
        profile, org = member

        row, raw = store.tokens.create(profile.id, org.id, name="laptop")

        assert raw.startswith("ilp_")
        assert row.token_hash == hashlib.sha256(raw.encode()).hexdigest()
        assert row.token_prefix == raw[:TOKEN_PREFIX_LEN]
        assert raw not in (row.token_hash, row.token_prefix)
        assert row.name == "laptop"
        assert row.expires_at is None
        assert row.revoked_at is None


class TestResolveToken:
    def test_resolve_returns_profile_and_live_role(self, store: Store, member: tuple[Profile, Organisation]):
        profile, org = member
        _, raw = store.tokens.create(profile.id, org.id, name="t")

        resolved = store.tokens.resolve(raw)

        assert resolved is not None
        got_profile, got_token, role = resolved
        assert got_profile.id == profile.id
        assert got_token.organisation_id == org.id
        assert role == "admin"

    def test_role_change_reflected_on_next_resolve(self, store: Store, member: tuple[Profile, Organisation]):
        profile, org = member
        _, raw = store.tokens.create(profile.id, org.id, name="t")

        store.auth.update_member_role(org.id, profile.id, "viewer")

        resolved = store.tokens.resolve(raw)
        assert resolved is not None
        assert resolved[2] == "viewer"

    def test_membership_removal_invalidates_token(self, store: Store, member: tuple[Profile, Organisation]):
        profile, org = member
        _, raw = store.tokens.create(profile.id, org.id, name="t")

        store.auth.remove_org_member(org.id, profile.id)

        assert store.tokens.resolve(raw) is None

    def test_expired_token_resolves_to_none(self, store: Store, member: tuple[Profile, Organisation]):
        profile, org = member
        past = datetime.now(timezone.utc) - timedelta(seconds=1)
        _, raw = store.tokens.create(profile.id, org.id, name="t", expires_at=past)

        assert store.tokens.resolve(raw) is None

    def test_revoked_token_resolves_to_none(self, store: Store, member: tuple[Profile, Organisation]):
        profile, org = member
        row, raw = store.tokens.create(profile.id, org.id, name="t")

        store.tokens.revoke(row.id)

        assert store.tokens.resolve(raw) is None

    def test_unknown_token_resolves_to_none(self, store: Store):
        assert store.tokens.resolve("ilp_no-such-token") is None

    def test_resolve_bumps_last_used_at_throttled(self, store: Store, member: tuple[Profile, Organisation]):
        profile, org = member
        row, raw = store.tokens.create(profile.id, org.id, name="t")
        assert row.last_used_at is None

        first = store.tokens.resolve(raw)
        assert first is not None
        first_used = first[1].last_used_at
        assert first_used is not None

        # A second resolve within the throttle window must not move the stamp.
        second = store.tokens.resolve(raw)
        assert second is not None
        assert second[1].last_used_at == first_used


class TestListAndRevoke:
    def test_list_tokens_filters_by_org(self, store: Store, member: tuple[Profile, Organisation]):
        profile, org = member
        other_org = store.auth.create_organisation(name="Other", creator_id=profile.id)
        store.tokens.create(profile.id, org.id, name="a")
        store.tokens.create(profile.id, other_org.id, name="b")

        assert {t.name for t in store.tokens.list_all(profile.id)} == {"a", "b"}
        assert {t.name for t in store.tokens.list_all(profile.id, org.id)} == {"a"}

    def test_revoke_missing_token_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.tokens.revoke(uuid4())

    def test_revoke_is_idempotent(self, store: Store, member: tuple[Profile, Organisation]):
        profile, org = member
        row, _ = store.tokens.create(profile.id, org.id, name="t")

        first = store.tokens.revoke(row.id)
        second = store.tokens.revoke(row.id)

        assert first.revoked_at is not None
        assert second.revoked_at == first.revoked_at
