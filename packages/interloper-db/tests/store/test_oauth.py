"""Tests for the OAuth store (``interloper_db.store.oauth``)."""

from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Engine, inspect, update
from sqlmodel import col

from interloper_db.crypto import hash_token
from interloper_db.models import Audience, AuthorizationCode, OAuthClient, Profile, RefreshToken
from interloper_db.session import session_scope
from interloper_db.store import Store

OAUTH_TABLES = {"oauth_clients", "oauth_authorization_codes", "oauth_refresh_tokens"}


@pytest.fixture
def profile(store: Store) -> Profile:
    """A user who authorises clients.

    Returns:
        The profile.
    """
    return store.profiles.upsert(google_id="g-user", email="user@example.com", name="User")


@pytest.fixture
def client(store: Store) -> OAuthClient:
    """A dynamically registered public client.

    Returns:
        The client.
    """
    client, _ = store.oauth.register_client("Claude Code", ["http://localhost:8765/callback"], registered_by="dynamic")
    return client


def _issue_code(store: Store, profile: Profile, client: OAuthClient) -> str:
    """Issue an authorization code for the MCP surface.

    Args:
        store: The store to issue through.
        profile: The user who approved.
        client: The client the code is for.

    Returns:
        The raw code.
    """
    return store.oauth.issue_code(
        client_id=client.client_id,
        user_id=profile.id,
        redirect_uri="http://localhost:8765/callback",
        code_challenge="challenge",
        resource=Audience.MCP,
    )


class TestClients:
    def test_public_client_has_no_secret(self, store: Store):
        client, secret = store.oauth.register_client("Claude Code", ["http://localhost/cb"], registered_by="dynamic")

        assert secret is None
        assert client.secret_hash is None
        assert store.oauth.get_client(client.client_id) == client

    def test_confidential_client_stores_only_the_secret_hash(self, store: Store):
        client, secret = store.oauth.register_client(
            "Slack", ["https://slack.example.com/cb"], registered_by="operator", confidential=True
        )

        assert secret is not None
        assert client.secret_hash == hash_token(secret)
        assert client.registered_by == "operator"
        stored = store.oauth.get_client(client.client_id)
        assert stored is not None
        assert stored.redirect_uris == ["https://slack.example.com/cb"]

    def test_unknown_client_is_none(self, store: Store):
        assert store.oauth.get_client("no-such-client") is None


class TestCodes:
    def test_code_single_use(self, store: Store, profile: Profile, client: OAuthClient):
        raw = _issue_code(store, profile, client)

        first = store.oauth.consume_code(raw)
        second = store.oauth.consume_code(raw)

        assert first is not None
        assert first.client_id == client.client_id
        assert first.user_id == profile.id
        assert first.redirect_uri == "http://localhost:8765/callback"
        assert first.code_challenge == "challenge"
        assert first.resource == "mcp"
        assert first.used_at is not None
        assert first.code_hash == hash_token(raw)
        assert second is None

    def test_code_lives_ten_minutes(self, store: Store, profile: Profile, client: OAuthClient):
        before = datetime.now(timezone.utc)
        code = store.oauth.consume_code(_issue_code(store, profile, client))

        assert code is not None
        assert before + timedelta(minutes=10) <= code.expires_at <= datetime.now(timezone.utc) + timedelta(minutes=10)

    def test_code_expires(self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine):
        raw = _issue_code(store, profile, client)
        with session_scope(auth_db) as session:
            session.connection().execute(
                update(AuthorizationCode).values(expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
            )
            session.commit()

        assert store.oauth.consume_code(raw) is None

    def test_unknown_code_is_none(self, store: Store):
        assert store.oauth.consume_code("no-such-code") is None

    def test_code_reuse_revokes_family(self, store: Store, profile: Profile, client: OAuthClient):
        raw = _issue_code(store, profile, client)
        code = store.oauth.consume_code(raw)
        assert code is not None
        refresh = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP, family_id=code.family_id)
        access = store.tokens.create_access(profile.id, client.client_id, Audience.MCP)

        assert store.oauth.consume_code(raw) is None

        assert store.oauth.rotate_refresh(refresh, client.client_id) is None
        assert store.tokens.resolve(access, Audience.MCP) is None


class TestRefreshTokens:
    def test_refresh_rotation_issues_new_and_retires_old(self, store: Store, profile: Profile, client: OAuthClient):
        old = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)

        rotated = store.oauth.rotate_refresh(old, client.client_id)
        assert rotated is not None
        assert rotated.rotated_at is not None
        assert rotated.user_id == profile.id
        assert rotated.resource == "mcp"
        before = datetime.now(timezone.utc)
        new = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP, family_id=rotated.family_id)

        successor = store.oauth.rotate_refresh(new, client.client_id)
        assert successor is not None
        assert successor.family_id == rotated.family_id
        assert successor.token_hash == hash_token(new)
        assert before + timedelta(days=30) <= successor.expires_at
        assert new != old

    def test_rotated_refresh_reuse_revokes_family(self, store: Store, profile: Profile, client: OAuthClient):
        old = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)
        rotated = store.oauth.rotate_refresh(old, client.client_id)
        assert rotated is not None
        new = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP, family_id=rotated.family_id)
        access = store.tokens.create_access(profile.id, client.client_id, Audience.MCP)

        assert store.oauth.rotate_refresh(old, client.client_id) is None

        assert store.oauth.rotate_refresh(new, client.client_id) is None
        assert store.tokens.resolve(access, Audience.MCP) is None
        assert store.oauth.authorised_clients(profile.id) == []

    def test_family_revocation_reaches_a_token_issued_after_it(
        self, store: Store, profile: Profile, client: OAuthClient
    ):
        # Two requests racing on one refresh token: the loser revokes the family
        # before the winner has saved its successor.
        old = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)
        rotated = store.oauth.rotate_refresh(old, client.client_id)
        assert rotated is not None
        assert store.oauth.rotate_refresh(old, client.client_id) is None

        late = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP, family_id=rotated.family_id)

        assert store.oauth.rotate_refresh(late, client.client_id) is None
        assert store.oauth.authorised_clients(profile.id) == []

    def test_refresh_is_bound_to_its_client(self, store: Store, profile: Profile, client: OAuthClient):
        other, _ = store.oauth.register_client("Other", ["http://localhost/cb"], registered_by="dynamic")
        raw = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)

        assert store.oauth.rotate_refresh(raw, other.client_id) is None
        assert store.oauth.rotate_refresh(raw, client.client_id) is not None

    def test_expired_and_unknown_refresh_are_none(
        self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine
    ):
        raw = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)
        with session_scope(auth_db) as session:
            session.connection().execute(
                update(RefreshToken).values(expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
            )
            session.commit()

        assert store.oauth.rotate_refresh(raw, client.client_id) is None
        assert store.oauth.rotate_refresh("ilr_no-such-token", client.client_id) is None


class TestAuthorisedClients:
    def test_revoke_client_for_user_kills_refresh_and_access(self, store: Store, profile: Profile, client: OAuthClient):
        other_user = store.profiles.upsert(google_id="g-other", email="other@example.com", name="Other")
        refresh = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)
        access = store.tokens.create_access(profile.id, client.client_id, Audience.MCP)
        theirs = store.oauth.issue_refresh(other_user.id, client.client_id, Audience.MCP)
        _, personal = store.tokens.create_personal(profile.id, "laptop", None)

        store.oauth.revoke_client_for_user(profile.id, client.client_id)

        assert store.oauth.rotate_refresh(refresh, client.client_id) is None
        assert store.tokens.resolve(access, Audience.MCP) is None
        assert store.oauth.rotate_refresh(theirs, client.client_id) is not None
        assert store.tokens.resolve(personal, Audience.MCP) is not None

    def test_authorised_clients_are_those_holding_a_live_refresh_token(
        self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine
    ):
        store.oauth.register_client("Unused", ["http://localhost/cb"], registered_by="dynamic")
        revoked, _ = store.oauth.register_client("Revoked", ["http://localhost/cb"], registered_by="dynamic")
        expired, _ = store.oauth.register_client("Expired", ["http://localhost/cb"], registered_by="dynamic")
        store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)
        store.oauth.issue_refresh(profile.id, client.client_id, Audience.API)
        store.oauth.issue_refresh(profile.id, revoked.client_id, Audience.MCP)
        store.oauth.revoke_client_for_user(profile.id, revoked.client_id)
        store.oauth.issue_refresh(profile.id, expired.client_id, Audience.MCP)
        with session_scope(auth_db) as session:
            session.connection().execute(
                update(RefreshToken)
                .where(col(RefreshToken.client_id) == expired.client_id)
                .values(expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
            )
            session.commit()

        assert [c.client_id for c in store.oauth.authorised_clients(profile.id)] == [client.client_id]


class TestRevocation:
    def test_revoke_family_kills_the_chain_and_its_access(self, store: Store, profile: Profile, client: OAuthClient):
        other, _ = store.oauth.register_client("Other", ["http://localhost/cb"], registered_by="dynamic")
        old = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)
        rotated = store.oauth.rotate_refresh(old, client.client_id)
        assert rotated is not None
        new = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP, family_id=rotated.family_id)
        access = store.tokens.create_access(profile.id, client.client_id, Audience.MCP)
        unrelated = store.oauth.issue_refresh(profile.id, other.client_id, Audience.MCP)

        presented = store.oauth.find_refresh(new)
        assert presented is not None
        store.oauth.revoke_family(presented)

        assert store.oauth.rotate_refresh(new, client.client_id) is None
        assert store.tokens.resolve(access, Audience.MCP) is None
        assert store.oauth.rotate_refresh(unrelated, other.client_id) is not None

    def test_find_refresh_of_an_unknown_token_is_none(self, store: Store):
        assert store.oauth.find_refresh("ilr_no-such-token") is None


class TestMigration:
    def test_migration_018_creates_and_drops_the_tables(self, store: Store, profile: Profile, auth_db: Engine):
        module = importlib.import_module("interloper_db.migrations.versions.018_oauth_authorization_server")
        with auth_db.begin() as connection:
            with Operations.context(MigrationContext.configure(connection)):
                module.downgrade()
            assert not OAUTH_TABLES & set(inspect(connection).get_table_names())
            with Operations.context(MigrationContext.configure(connection)):
                module.upgrade()
                module.upgrade()
            assert OAUTH_TABLES <= set(inspect(connection).get_table_names())
            code_indexes = {index["name"] for index in inspect(connection).get_indexes("oauth_authorization_codes")}
            assert "ix_oauth_authorization_codes_user_id" in code_indexes

        client, _ = store.oauth.register_client("Claude Code", ["http://localhost/cb"], registered_by="dynamic")
        code = store.oauth.issue_code(
            client_id=client.client_id,
            user_id=profile.id,
            redirect_uri="http://localhost/cb",
            code_challenge="challenge",
            resource=Audience.API,
        )
        consumed = store.oauth.consume_code(code)
        assert consumed is not None
        refresh = store.oauth.issue_refresh(profile.id, client.client_id, Audience.API, family_id=consumed.family_id)
        assert store.oauth.rotate_refresh(refresh, client.client_id) is not None
        assert store.oauth.get_client(client.client_id) is not None
