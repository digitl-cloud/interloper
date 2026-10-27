"""Tests for the token store (``interloper_db.store.tokens``)."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import interloper as il
import pytest
from sqlalchemy import Engine, event, text
from sqlalchemy.pool import StaticPool

from interloper_db import NotFoundError, PageQuery
from interloper_db import engine as engine_module
from interloper_db.crypto import hash_token
from interloper_db.models import Audience, Organisation, Profile, Token, TokenKind, UserOrganisation
from interloper_db.session import save, session_scope
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

    def test_session_token_resolves_only_as_cookie(self, store: Store, profile: Profile):
        _insert(profile, "ils_session", TokenKind.SESSION)

        assert store.tokens.resolve("ils_session", Audience.APP) is not None
        assert store.tokens.resolve("ils_session", Audience.API) is None
        assert store.tokens.resolve("ils_session", Audience.MCP) is None

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
