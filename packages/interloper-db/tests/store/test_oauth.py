"""Tests for the OAuth store (``interloper_db.store.oauth``)."""

from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Engine, inspect, update
from sqlmodel import col, select

from interloper_db.crypto import hash_token
from interloper_db.models import Audience, AuthorizationCode, OAuthClient, Profile, RefreshToken, Token, TokenKind
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

    def test_a_client_id_never_reads_as_a_command_line_option(self, store: Store):
        # An id is typed after `interloper oauth delete`, where a leading dash would read as an option.
        ids = [
            store.oauth.register_client("App", ["http://127.0.0.1/cb"], registered_by="dynamic")[0].client_id
            for _ in range(512)
        ]

        assert not [client_id for client_id in ids if client_id.startswith("-")]

    def test_unknown_client_is_none(self, store: Store):
        assert store.oauth.get_client("no-such-client") is None

    def test_list_clients_filters_by_registrant_and_orders_by_name(self, store: Store):
        store.oauth.register_client("Zed", ["http://localhost/cb"], registered_by="dynamic")
        store.oauth.register_client(
            "Slack", ["https://slack.example.com/cb"], registered_by="operator", confidential=True
        )
        store.oauth.register_client(
            "Acme", ["https://acme.example.com/cb"], registered_by="operator", confidential=True
        )

        assert [client.name for client in store.oauth.list_clients()] == ["Acme", "Slack", "Zed"]
        assert [client.name for client in store.oauth.list_clients(registered_by="operator")] == ["Acme", "Slack"]

    def test_delete_client_removes_it_with_its_grants(self, store: Store, profile: Profile, client: OAuthClient):
        code = store.oauth.consume_code(_issue_code(store, profile, client))
        assert code is not None
        refresh = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP, family_id=code.family_id)
        access = store.tokens.create_access(profile.id, client.client_id, Audience.MCP)
        _issue_code(store, profile, client)

        assert store.oauth.delete_client(client.client_id) is True

        assert store.oauth.get_client(client.client_id) is None
        assert store.oauth.rotate_refresh(refresh, client.client_id) is None
        assert store.tokens.resolve(access, Audience.MCP) is None

    def test_delete_client_spares_other_clients_grants(self, store: Store, profile: Profile, client: OAuthClient):
        other, _ = store.oauth.register_client("Other", ["http://localhost/cb"], registered_by="dynamic")
        access = store.tokens.create_access(profile.id, other.client_id, Audience.MCP)

        store.oauth.delete_client(client.client_id)

        assert store.tokens.resolve(access, Audience.MCP) is not None

    def test_delete_unknown_client_is_false(self, store: Store):
        assert store.oauth.delete_client("no-such-client") is False


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

    def test_stale_replay_of_an_expired_code_spares_a_newer_grant(
        self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine
    ):
        raw = _issue_code(store, profile, client)
        assert store.oauth.consume_code(raw) is not None
        with session_scope(auth_db) as session:
            session.connection().execute(
                update(AuthorizationCode).values(expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
            )
            session.commit()
        newer = store.oauth.consume_code(_issue_code(store, profile, client))
        assert newer is not None
        issued = store.oauth.issue_tokens(profile.id, client.client_id, Audience.MCP, newer.family_id)
        assert issued is not None
        access, refresh = issued

        assert store.oauth.consume_code(raw) is None

        assert store.tokens.resolve(access, Audience.MCP) is not None
        assert store.oauth.rotate_refresh(refresh, client.client_id) is not None


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

    def test_replay_into_a_revoked_family_spares_a_newer_grant(
        self, store: Store, profile: Profile, client: OAuthClient
    ):
        old = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)
        assert store.oauth.rotate_refresh(old, client.client_id) is not None
        assert store.oauth.rotate_refresh(old, client.client_id) is None
        code = store.oauth.consume_code(_issue_code(store, profile, client))
        assert code is not None
        issued = store.oauth.issue_tokens(profile.id, client.client_id, Audience.MCP, code.family_id)
        assert issued is not None
        access, refresh = issued

        assert store.oauth.rotate_refresh(old, client.client_id) is None

        assert store.tokens.resolve(access, Audience.MCP) is not None
        assert store.oauth.rotate_refresh(refresh, client.client_id) is not None

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


class TestIssueTokens:
    def test_issue_tokens_pairs_an_access_token_with_a_refresh_token_of_the_family(
        self, store: Store, profile: Profile, client: OAuthClient
    ):
        code = store.oauth.consume_code(_issue_code(store, profile, client))
        assert code is not None

        issued = store.oauth.issue_tokens(profile.id, client.client_id, Audience.MCP, code.family_id)

        assert issued is not None
        access, refresh = issued
        resolved = store.tokens.resolve(access, Audience.MCP)
        assert resolved is not None
        assert resolved[1].client_id == client.client_id
        found = store.oauth.find_refresh(refresh)
        assert found is not None
        assert found.family_id == code.family_id
        assert found.resource == "mcp"

    def test_issue_tokens_into_a_revoked_family_leaves_nothing_live(
        self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine
    ):
        # The family was revoked while the pair was being issued: a replay
        # landed between the claim and the issue.
        old = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)
        rotated = store.oauth.rotate_refresh(old, client.client_id)
        assert rotated is not None
        assert store.oauth.rotate_refresh(old, client.client_id) is None

        assert store.oauth.issue_tokens(profile.id, client.client_id, Audience.MCP, rotated.family_id) is None

        with session_scope(auth_db) as session:
            live_access = session.exec(
                select(Token).where(col(Token.kind) == TokenKind.ACCESS, col(Token.revoked_at).is_(None))
            ).all()
            live_refresh = session.exec(select(RefreshToken).where(col(RefreshToken.revoked_at).is_(None))).all()
        assert live_access == []
        assert live_refresh == []
        assert store.oauth.authorised_clients(profile.id) == []


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

    def test_revoke_client_for_user_burns_unspent_codes(self, store: Store, profile: Profile, client: OAuthClient):
        other_user = store.profiles.upsert(google_id="g-other", email="other@example.com", name="Other")
        mine = _issue_code(store, profile, client)
        theirs = _issue_code(store, other_user, client)

        store.oauth.revoke_client_for_user(profile.id, client.client_id)

        assert store.oauth.consume_code(mine) is None
        assert store.oauth.consume_code(theirs) is not None

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
