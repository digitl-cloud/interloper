"""Tests for the token store (``interloper_db.store.tokens``)."""

from __future__ import annotations

import hashlib
import importlib
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import interloper as il
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Engine, event, inspect, text
from sqlalchemy.pool import StaticPool

from interloper_db import NotFoundError, PageQuery
from interloper_db import engine as engine_module
from interloper_db.crypto import hash_token
from interloper_db.models import Audience, Organisation, Profile, Token, TokenKind, UserOrganisation
from interloper_db.session import save, session_scope
from interloper_db.store import Store
from interloper_db.store.tokens import TOKEN_PREFIX_LEN, TokenStore


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

    for model in (Profile, Organisation, UserOrganisation, Token):
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
def profile(store: Store) -> Profile:
    """A profile to hold tokens.

    Returns:
        The profile.
    """
    return store.profiles.upsert(google_id="g-user", email="user@example.com", name="User")


def _insert(profile: Profile, raw: str, kind: TokenKind, resource: str | None = None) -> Token:
    """Insert a token row directly, for the kinds nothing issues yet.

    Args:
        profile: The token's holder.
        raw: The raw secret whose hash the row stores.
        kind: The row's kind.
        resource: The audience an access token is issued for.

    Returns:
        The saved row.
    """
    token = Token(
        user_id=profile.id,
        kind=kind,
        resource=resource,
        token_prefix=raw[:TOKEN_PREFIX_LEN],
        token_hash=hash_token(raw),
    )
    with session_scope(engine_module.get_engine()) as session:
        save(session, token)
    return token


class TestCreatePersonal:
    def test_create_returns_raw_and_stores_only_hash(self, store: Store, profile: Profile):
        row, raw = store.tokens.create_personal(profile.id, "laptop", None)

        assert raw.startswith("ilp_")
        assert row.token_hash == hashlib.sha256(raw.encode()).hexdigest()
        assert row.token_prefix == raw[:TOKEN_PREFIX_LEN]
        assert raw not in (row.token_hash, row.token_prefix)
        assert row.name == "laptop"
        assert row.kind == TokenKind.PERSONAL
        assert row.expires_at is None
        assert row.revoked_at is None


class TestResolve:
    def test_personal_token_resolves_on_api_and_mcp(self, store: Store, profile: Profile):
        _, raw = store.tokens.create_personal(profile.id, "t", None)

        for audience in (Audience.API, Audience.MCP):
            resolved = store.tokens.resolve(raw, audience)
            assert resolved is not None
            got_profile, got_token = resolved
            assert got_profile.id == profile.id
            assert got_token.kind == TokenKind.PERSONAL

    def test_personal_token_refused_as_cookie(self, store: Store, profile: Profile):
        _, raw = store.tokens.create_personal(profile.id, "t", None)

        assert store.tokens.resolve(raw, Audience.APP) is None

    def test_session_resolves_only_as_cookie(self, store: Store, profile: Profile):
        raw = store.tokens.create_session(profile.id)

        resolved = store.tokens.resolve(raw, Audience.APP)
        assert resolved is not None
        assert resolved[1].kind == TokenKind.SESSION
        assert store.tokens.resolve(raw, Audience.API) is None
        assert store.tokens.resolve(raw, Audience.MCP) is None

    def test_access_token_resolves_only_on_its_resource(self, store: Store, profile: Profile):
        _insert(profile, "ila_api", TokenKind.ACCESS, resource=Audience.API.value)

        assert store.tokens.resolve("ila_api", Audience.API) is not None
        assert store.tokens.resolve("ila_api", Audience.MCP) is None
        assert store.tokens.resolve("ila_api", Audience.APP) is None

    def test_expired_revoked_unknown_refused(self, store: Store, profile: Profile):
        past = datetime.now(timezone.utc) - timedelta(seconds=1)
        _, expired = store.tokens.create_personal(profile.id, "expired", past)
        revoked_row, revoked = store.tokens.create_personal(profile.id, "revoked", None)
        store.tokens.revoke(revoked_row.id)

        assert store.tokens.resolve(expired, Audience.API) is None
        assert store.tokens.resolve(revoked, Audience.API) is None
        assert store.tokens.resolve("ilp_no-such-token", Audience.API) is None

    def test_resolve_stamps_last_used_at(self, store: Store, profile: Profile):
        row, raw = store.tokens.create_personal(profile.id, "t", None)
        assert row.last_used_at is None

        first = store.tokens.resolve(raw, Audience.MCP)
        assert first is not None
        first_used = first[1].last_used_at
        assert first_used is not None
        assert store.tokens.get(row.id).last_used_at is not None

        # A second resolve within the throttle window must not move the stamp.
        second = store.tokens.resolve(raw, Audience.MCP)
        assert second is not None
        assert second[1].last_used_at == first_used

    def test_a_row_with_the_old_columns_resolves_as_personal(self, store: Store, profile: Profile, token_db: Engine):
        # A personal_access_tokens row carried none of kind, client_id or
        # resource; migration 015 keeps it, and the kind's default makes it personal.
        with token_db.connect() as connection:
            connection.execute(
                text(
                    "INSERT INTO tokens (id, user_id, name, token_prefix, token_hash) "
                    "VALUES (:id, :user_id, 'old', 'ilp_old', :token_hash)"
                ),
                {"id": uuid4().hex, "user_id": profile.id.hex, "token_hash": hash_token("ilp_old-secret")},
            )
            connection.commit()

        resolved = store.tokens.resolve("ilp_old-secret", Audience.MCP)

        assert resolved is not None
        assert resolved[1].kind == TokenKind.PERSONAL


class TestAccess:
    def test_access_token_bound_to_resource(self, store: Store, profile: Profile):
        raw = store.tokens.create_access(profile.id, "client-1", Audience.MCP)

        resolved = store.tokens.resolve(raw, Audience.MCP)
        assert resolved is not None
        assert resolved[1].kind == TokenKind.ACCESS
        assert resolved[1].client_id == "client-1"
        assert resolved[1].resource == "mcp"
        assert store.tokens.resolve(raw, Audience.API) is None
        assert store.tokens.resolve(raw, Audience.APP) is None

    def test_find_access_by_raw_value_whatever_its_state(self, store: Store, profile: Profile):
        raw = store.tokens.create_access(profile.id, "client-1", Audience.MCP)
        _, personal = store.tokens.create_personal(profile.id, "laptop", None)
        session = store.tokens.create_session(profile.id)
        found = store.tokens.find_access(raw)
        assert found is not None
        store.tokens.revoke(found.id)

        revoked = store.tokens.find_access(raw)

        assert revoked is not None
        assert revoked.id == found.id
        assert revoked.revoked_at is not None
        assert store.tokens.find_access(personal) is None
        assert store.tokens.find_access(session) is None
        assert store.tokens.find_access("ila_no-such-token") is None

    def test_access_expiry_from_settings(self, token_db: Engine, profile: Profile):
        tokens = TokenStore(token_db, session_expiry=timedelta(days=30), access_token_expiry=timedelta(minutes=5))

        before = datetime.now(timezone.utc)
        raw = tokens.create_access(profile.id, "client-1", Audience.API)

        resolved = tokens.resolve(raw, Audience.API)
        assert resolved is not None
        expires_at = resolved[1].expires_at
        assert expires_at is not None
        assert before + timedelta(minutes=5) <= expires_at <= datetime.now(timezone.utc) + timedelta(minutes=5)


class TestSessions:
    def test_session_expiry_from_settings(self, token_db: Engine, profile: Profile):
        tokens = TokenStore(token_db, session_expiry=timedelta(days=2), access_token_expiry=timedelta(hours=1))

        before = datetime.now(timezone.utc)
        raw = tokens.create_session(profile.id)

        resolved = tokens.resolve(raw, Audience.APP)
        assert resolved is not None
        expires_at = resolved[1].expires_at
        assert expires_at is not None
        assert before + timedelta(days=2) <= expires_at <= datetime.now(timezone.utc) + timedelta(days=2)

    def test_revoke_sessions_ends_every_session_of_the_user_only(self, store: Store, profile: Profile):
        other = store.profiles.upsert(google_id="g-other", email="other@example.com", name="Other")
        first = store.tokens.create_session(profile.id)
        second = store.tokens.create_session(profile.id)
        theirs = store.tokens.create_session(other.id)
        _, personal = store.tokens.create_personal(profile.id, "laptop", None)

        store.tokens.revoke_sessions(profile.id)

        assert store.tokens.resolve(first, Audience.APP) is None
        assert store.tokens.resolve(second, Audience.APP) is None
        assert store.tokens.resolve(theirs, Audience.APP) is not None
        assert store.tokens.resolve(personal, Audience.API) is not None

    def test_migration_017_keeps_logins(self, store: Store, profile: Profile, token_db: Engine):
        # A login made before migration 017 lives in ``sessions``; the upgrade
        # moves it into ``tokens`` with its hash, so the cookie keeps working.
        expired_hash = hash_token("expired-cookie")
        with token_db.begin() as connection:
            connection.execute(
                text(
                    "CREATE TABLE sessions (id uuid PRIMARY KEY, user_id uuid NOT NULL, organisation_id uuid, "
                    "token_hash varchar NOT NULL, expires_at timestamp with time zone NOT NULL, "
                    "created_at timestamp with time zone)"
                )
            )
            for raw, expires_at in (
                ("live-cookie", datetime.now(timezone.utc) + timedelta(days=1)),
                ("expired-cookie", datetime.now(timezone.utc) - timedelta(days=1)),
            ):
                connection.execute(
                    text(
                        "INSERT INTO sessions (id, user_id, token_hash, expires_at) "
                        "VALUES (:id, :user_id, :token_hash, :expires_at)"
                    ),
                    {
                        "id": uuid4().hex,
                        "user_id": profile.id.hex,
                        "token_hash": hash_token(raw),
                        "expires_at": expires_at.strftime("%Y-%m-%d %H:%M:%S.%f"),
                    },
                )

            with Operations.context(MigrationContext.configure(connection)):
                importlib.import_module("interloper_db.migrations.versions.017_sessions_are_tokens").upgrade()

        resolved = store.tokens.resolve("live-cookie", Audience.APP)
        assert resolved is not None
        assert resolved[0].id == profile.id
        assert resolved[1].kind == TokenKind.SESSION
        with token_db.connect() as connection:
            moved = connection.execute(text("SELECT count(*) FROM tokens WHERE token_hash = :h"), {"h": expired_hash})
            assert moved.scalar() == 0
            assert "sessions" not in inspect(connection).get_table_names()

    def test_migration_017_downgrade_restores_sessions_the_old_code_can_write(
        self, store: Store, profile: Profile, token_db: Engine
    ):
        # The rolled-back code inserts a session without an id, relying on the
        # table's server default; a live session token moves back with its hash.
        raw = store.tokens.create_session(profile.id)
        with token_db.begin() as connection:
            with Operations.context(MigrationContext.configure(connection)):
                importlib.import_module("interloper_db.migrations.versions.017_sessions_are_tokens").downgrade()
            connection.execute(
                text("INSERT INTO sessions (user_id, token_hash, expires_at) VALUES (:user_id, 'new', :expires_at)"),
                {"user_id": profile.id.hex, "expires_at": "2099-01-01 00:00:00"},
            )

        with token_db.connect() as connection:
            rows = connection.execute(text("SELECT id, token_hash FROM sessions")).all()
            left = connection.execute(text("SELECT count(*) FROM tokens WHERE kind = 'session'")).scalar()
        assert {row.token_hash for row in rows} == {hash_token(raw), "new"}
        assert all(row.id is not None for row in rows)
        assert left == 0


class TestListAndRevoke:
    def test_list_returns_the_users_personal_tokens(self, store: Store, profile: Profile):
        other = store.profiles.upsert(google_id="g-other", email="other@example.com", name="Other")
        store.tokens.create_personal(profile.id, "a", None)
        store.tokens.create_personal(profile.id, "b", None)
        store.tokens.create_personal(other.id, "c", None)
        _insert(profile, "ils_session", TokenKind.SESSION)

        listed = store.tokens.list(profile.id, PageQuery())

        assert {t.name for t in listed.items} == {"a", "b"}
        assert listed.total == 2

    def test_revoke_missing_token_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.tokens.revoke(uuid4())

    def test_revoke_is_idempotent(self, store: Store, profile: Profile):
        row, _ = store.tokens.create_personal(profile.id, "t", None)

        first = store.tokens.revoke(row.id)
        second = store.tokens.revoke(row.id)

        assert first.revoked_at is not None
        assert second.revoked_at == first.revoked_at


class TestGetToken:
    """Id-addressed reads."""

    def test_get_returns_the_token(self, store: Store, profile: Profile):
        token, _raw = store.tokens.create_personal(profile.id, "ci", None)

        assert store.tokens.get(token.id).id == token.id

    def test_get_missing_token_raises(self, store: Store):
        missing = uuid4()

        with pytest.raises(NotFoundError, match=f"Token {missing} not found"):
            store.tokens.get(missing)


class TestResolveOrphanedToken:
    """A token whose profile vanished resolves to nothing."""

    def test_it_resolves_to_none(self, store: Store, profile: Profile, token_db: Engine):
        # The foreign key makes this unreachable through the store's own API,
        # so the orphan is fabricated with constraints off — the guard exists
        # for a row that should not be there.
        _token, raw = store.tokens.create_personal(profile.id, "ci", None)
        with token_db.connect() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
            connection.exec_driver_sql("DELETE FROM profiles")
            connection.commit()

        assert store.tokens.resolve(raw, Audience.API) is None
