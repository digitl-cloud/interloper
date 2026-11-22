"""Tests for the token store (``interloper_db.store.tokens``)."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from sqlalchemy import Engine, event, text, update
from sqlalchemy.pool import StaticPool
from sqlmodel import col, select

from interloper_db import NotFoundError, PageQuery
from interloper_db import engine as engine_module
from interloper_db.crypto import hash_token
from interloper_db.models import (
    Audience,
    Identity,
    Organisation,
    Profile,
    Token,
    TokenKind,
    UserOrganisation,
    table_of,
)
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

    for model in (Profile, Identity, Organisation, UserOrganisation, Token):
        table_of(model).create(eng)
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
def statements(token_db: Engine) -> list[str]:
    """Record every statement sent to the database from here on.

    Args:
        token_db: The engine to listen on.

    Returns:
        The statements, in the order they were sent.
    """
    sent: list[str] = []
    event.listen(token_db, "before_cursor_execute", lambda _c, _cursor, statement, *_: sent.append(statement))
    return sent


@pytest.fixture
def profile(store: Store) -> Profile:
    """A profile to hold tokens.

    Returns:
        The profile.
    """
    return store.profiles.upsert(
        issuer="https://accounts.google.com", subject="g-user", email="user@example.com", name="User"
    )


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

    def test_refresh_token_resolves_on_no_surface(self, store: Store, profile: Profile):
        _insert(profile, "ilr_refresh", TokenKind.REFRESH, resource=Audience.MCP.value)

        for audience in Audience:
            assert store.tokens.resolve("ilr_refresh", audience) is None

    def test_expired_revoked_unknown_refused(self, store: Store, profile: Profile):
        past = datetime.now(UTC) - timedelta(seconds=1)
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

    def test_the_token_and_its_holder_are_read_in_one_statement(
        self, store: Store, profile: Profile, statements: list[str]
    ):
        raw = store.tokens.create_session(profile.id)
        store.tokens.resolve(raw, Audience.APP)
        statements.clear()

        resolved = store.tokens.resolve(raw, Audience.APP)

        assert resolved is not None
        assert resolved[0].id == profile.id
        assert len(statements) == 1

    def test_in_a_shared_session_the_stamp_commits_and_stays_throttled(
        self, store: Store, profile: Profile, statements: list[str]
    ):
        row, raw = store.tokens.create_personal(profile.id, "t", None)
        statements.clear()

        with store.shared_session():
            store.tokens.resolve(raw, Audience.API)
            stamped = len(statements)
            store.tokens.resolve(raw, Audience.API)

        assert stamped == 2
        assert len(statements) == 3
        assert store.tokens.get(row.id).last_used_at is not None

    def test_a_row_with_the_old_columns_resolves_as_personal(self, store: Store, profile: Profile, token_db: Engine):
        # A token written before tokens had a kind, a client or a resource is
        # personal by the kind's default.
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


class TestResolveIn:
    """A secret resolved in an organisation answers its holder's standing there, in the same statement."""

    @pytest.fixture
    def organisation(self, store: Store) -> Organisation:
        """An organisation the profile is not yet a member of.

        Returns:
            The organisation.
        """
        return store.organisations.create("Acme")

    def test_a_member_gets_the_organisation_and_their_role(
        self, store: Store, profile: Profile, organisation: Organisation
    ):
        store.members.add(organisation.id, profile.id, "editor")
        raw = store.tokens.create_session(profile.id)

        resolved = store.tokens.resolve_in(raw, Audience.APP, organisation.id)

        assert resolved is not None
        holder, token, found, role = resolved
        assert (holder.id, token.kind, role) == (profile.id, TokenKind.SESSION, "editor")
        assert found is not None and found.id == organisation.id

    def test_a_non_member_gets_the_organisation_and_no_role(
        self, store: Store, profile: Profile, organisation: Organisation
    ):
        raw = store.tokens.create_session(profile.id)

        resolved = store.tokens.resolve_in(raw, Audience.APP, organisation.id)

        assert resolved is not None
        assert resolved[2] is not None
        assert resolved[3] is None

    def test_a_super_admin_acts_as_admin_without_a_membership(
        self, store: Store, profile: Profile, organisation: Organisation
    ):
        store.profiles.set_super_admin(profile.id, value=True)
        raw = store.tokens.create_session(profile.id)

        resolved = store.tokens.resolve_in(raw, Audience.APP, organisation.id)

        assert resolved is not None
        assert resolved[3] == "admin"

    @pytest.mark.parametrize("is_super_admin", [True, False])
    def test_a_deleted_or_unknown_organisation_is_none_for_everyone(
        self, store: Store, profile: Profile, organisation: Organisation, token_db: Engine, is_super_admin: bool
    ):
        store.members.add(organisation.id, profile.id, "admin")
        store.profiles.set_super_admin(profile.id, value=is_super_admin)
        with token_db.begin() as connection:
            connection.execute(
                update(Organisation).where(col(Organisation.id) == organisation.id).values(deleted_at=datetime.now(UTC))
            )
        raw = store.tokens.create_session(profile.id)

        for org_id in (organisation.id, uuid4()):
            resolved = store.tokens.resolve_in(raw, Audience.APP, org_id)
            assert resolved is not None
            assert resolved[2:] == (None, None)

    def test_a_secret_the_surface_refuses_resolves_to_nothing(
        self, store: Store, profile: Profile, organisation: Organisation
    ):
        store.members.add(organisation.id, profile.id, "admin")
        raw = store.tokens.create_session(profile.id)

        assert store.tokens.resolve_in(raw, Audience.API, organisation.id) is None
        assert store.tokens.resolve_in("ilp_no-such-token", Audience.API, organisation.id) is None

    def test_everything_is_read_in_one_statement_and_the_use_stamped_once(
        self, store: Store, profile: Profile, organisation: Organisation, statements: list[str]
    ):
        store.members.add(organisation.id, profile.id, "viewer")
        row, raw = store.tokens.create_personal(profile.id, "t", None)
        statements.clear()

        store.tokens.resolve_in(raw, Audience.API, organisation.id)
        stamped = list(statements)
        statements.clear()
        store.tokens.resolve_in(raw, Audience.API, organisation.id)

        assert [statement.split()[0] for statement in stamped] == ["SELECT", "UPDATE"]
        assert len(statements) == 1
        assert store.tokens.get(row.id).last_used_at is not None


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

        before = datetime.now(UTC)
        raw = tokens.create_access(profile.id, "client-1", Audience.API)

        resolved = tokens.resolve(raw, Audience.API)
        assert resolved is not None
        expires_at = resolved[1].expires_at
        assert expires_at is not None
        assert before + timedelta(minutes=5) <= expires_at <= datetime.now(UTC) + timedelta(minutes=5)


class TestSessions:
    def test_session_expiry_from_settings(self, token_db: Engine, profile: Profile):
        tokens = TokenStore(token_db, session_expiry=timedelta(days=2), access_token_expiry=timedelta(hours=1))

        before = datetime.now(UTC)
        raw = tokens.create_session(profile.id)

        resolved = tokens.resolve(raw, Audience.APP)
        assert resolved is not None
        expires_at = resolved[1].expires_at
        assert expires_at is not None
        assert before + timedelta(days=2) <= expires_at <= datetime.now(UTC) + timedelta(days=2)

    def test_revoke_sessions_ends_every_session_of_the_user_only(self, store: Store, profile: Profile):
        other = store.profiles.upsert(
            issuer="https://accounts.google.com", subject="g-other", email="other@example.com", name="Other"
        )
        first = store.tokens.create_session(profile.id)
        second = store.tokens.create_session(profile.id)
        theirs = store.tokens.create_session(other.id)
        _, personal = store.tokens.create_personal(profile.id, "laptop", None)

        store.tokens.revoke_sessions(profile.id)

        assert store.tokens.resolve(first, Audience.APP) is None
        assert store.tokens.resolve(second, Audience.APP) is None
        assert store.tokens.resolve(theirs, Audience.APP) is not None
        assert store.tokens.resolve(personal, Audience.API) is not None

    def test_revoke_sessions_can_keep_the_callers_own(self, store: Store, profile: Profile):
        mine = store.tokens.create_session(profile.id)
        elsewhere = store.tokens.create_session(profile.id)

        store.tokens.revoke_sessions(profile.id, keep=mine)

        assert store.tokens.resolve(mine, Audience.APP) is not None
        assert store.tokens.resolve(elsewhere, Audience.APP) is None


class TestListAndRevoke:
    def test_list_returns_the_users_personal_tokens(self, store: Store, profile: Profile):
        other = store.profiles.upsert(
            issuer="https://accounts.google.com", subject="g-other", email="other@example.com", name="Other"
        )
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


RETENTION = timedelta(days=30)


class TestPurge:
    """Tokens that ended, by expiry or revocation, longer ago than the window are deleted."""

    def _set(self, token_db: Engine, token_id: UUID, **values: Any) -> None:
        """Overwrite columns of one token.

        Args:
            token_db: The fixture database.
            token_id: The token's id.
            **values: The columns to set.
        """
        with session_scope(token_db) as session:
            session.connection().execute(update(Token).where(col(Token.id) == token_id).values(**values))
            session.commit()

    def _remaining(self, token_db: Engine) -> set[UUID]:
        """The ids of the tokens still stored.

        Args:
            token_db: The fixture database.

        Returns:
            Their ids.
        """
        with session_scope(token_db) as session:
            return set(session.exec(select(Token.id)).all())

    def test_every_kind_ended_past_the_window_is_deleted(self, store: Store, profile: Profile, token_db: Engine):
        long_ago = datetime.now(UTC) - RETENTION - timedelta(minutes=1)
        store.tokens.create_session(profile.id)
        store.tokens.create_access(profile.id, "client", Audience.MCP)
        store.tokens.create_personal(profile.id, "expired", long_ago)
        revoked_personal, _ = store.tokens.create_personal(profile.id, "revoked", None)
        with session_scope(token_db) as session:
            session.connection().execute(
                update(Token).where(col(Token.kind) != TokenKind.PERSONAL).values(expires_at=long_ago)
            )
            session.commit()
        self._set(token_db, revoked_personal.id, revoked_at=long_ago)

        assert store.tokens.purge(ended_for=RETENTION) == 4

        assert self._remaining(token_db) == set()

    def test_a_token_ended_inside_the_window_is_kept(self, store: Store, profile: Profile, token_db: Engine):
        recently = datetime.now(UTC) - RETENTION + timedelta(minutes=1)
        expired, _ = store.tokens.create_personal(profile.id, "expired", recently)
        revoked, _ = store.tokens.create_personal(profile.id, "revoked", None)
        self._set(token_db, revoked.id, revoked_at=recently)

        assert store.tokens.purge(ended_for=RETENTION) == 0

        assert self._remaining(token_db) == {expired.id, revoked.id}

    def test_a_live_token_is_kept_however_old(self, store: Store, profile: Profile, token_db: Engine):
        forever, _ = store.tokens.create_personal(profile.id, "forever", None)
        self._set(token_db, forever.id, created_at=datetime.now(UTC) - timedelta(days=3650))
        store.tokens.create_session(profile.id)

        assert store.tokens.purge(ended_for=RETENTION) == 0

        assert len(self._remaining(token_db)) == 2

    def test_a_refresh_token_is_left_to_the_oauth_purge(self, store: Store, profile: Profile, token_db: Engine):
        refresh = _insert(profile, "ilr_ended", TokenKind.REFRESH, resource=Audience.MCP.value)
        long_ago = datetime.now(UTC) - RETENTION - timedelta(days=1)
        self._set(token_db, refresh.id, expires_at=long_ago, revoked_at=long_ago)

        assert store.tokens.purge(ended_for=RETENTION) == 0

        assert self._remaining(token_db) == {refresh.id}

    def test_a_token_revoked_recently_but_expired_long_ago_is_deleted(
        self, store: Store, profile: Profile, token_db: Engine
    ):
        token, _ = store.tokens.create_personal(profile.id, "t", datetime.now(UTC) - timedelta(days=365))
        store.tokens.revoke(token.id)

        assert store.tokens.purge(ended_for=RETENTION) == 1

        assert self._remaining(token_db) == set()
