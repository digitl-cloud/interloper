"""Tests for the OAuth store (``interloper_db.store.oauth``)."""

from __future__ import annotations

import importlib
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Engine, inspect, update
from sqlmodel import col, select

from interloper_db.crypto import hash_token
from interloper_db.models import (
    Audience,
    AuthorizationCode,
    Conversation,
    OAuthClient,
    Profile,
    RefreshToken,
    Token,
    TokenKind,
)
from interloper_db.session import session_scope
from interloper_db.store import Store

OAUTH_TABLES = {"oauth_clients", "oauth_authorization_codes", "oauth_refresh_tokens"}


@pytest.fixture
def profile(store: Store) -> Profile:
    """A user who authorises clients.

    Returns:
        The profile.
    """
    return store.profiles.upsert(
        issuer="https://accounts.google.com", subject="g-user", email="user@example.com", name="User"
    )


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


class _Conversations:
    """The conversations a grant-ending test seeds around one client and one user."""

    def __init__(self, store: Store, profile: Profile, client: OAuthClient) -> None:
        """Seed the conversations whose survival each test checks.

        Args:
            store: The store to seed through.
            profile: The user whose grant ends.
            client: The client whose grant ends.
        """
        self._store = store
        other_client, _ = store.oauth.register_client("Other", ["http://localhost/cb"], registered_by="dynamic")
        other_user = store.profiles.upsert(
            issuer="https://accounts.google.com", subject="g-other", email="other@example.com", name="Other"
        )
        org_id = store.organisations.create(name="Acme", creator_id=profile.id).id
        self.ended = store.conversations.create(org_id, profile.id, client.client_id).id
        self.other_client = store.conversations.create(org_id, profile.id, other_client.client_id).id
        self.app = store.conversations.create(org_id, profile.id).id
        self.other_user = store.conversations.create(org_id, other_user.id, client.client_id).id

    def remaining(self) -> set[UUID]:
        """Read which seeded conversations are still stored.

        Returns:
            The ids of the seeded conversations that still exist.
        """
        with session_scope(self._store.engine) as session:
            stored = set(session.exec(select(Conversation.id)).all())
        return stored & {self.ended, self.other_client, self.app, self.other_user}

    def survivors(self) -> set[UUID]:
        """Name the conversations a revocation for one user and client must spare.

        Returns:
            The ids of the conversations of another client, of the app and of another user.
        """
        return {self.other_client, self.app, self.other_user}


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

        deleted = store.oauth.delete_client(client.client_id)
        assert deleted is not None
        assert deleted.client_id == client.client_id

        assert store.oauth.get_client(client.client_id) is None
        assert store.oauth.rotate_refresh(refresh, client.client_id) is None
        assert store.tokens.resolve(access, Audience.MCP) is None

    def test_delete_client_spares_other_clients_grants(self, store: Store, profile: Profile, client: OAuthClient):
        other, _ = store.oauth.register_client("Other", ["http://localhost/cb"], registered_by="dynamic")
        access = store.tokens.create_access(profile.id, other.client_id, Audience.MCP)

        store.oauth.delete_client(client.client_id)

        assert store.tokens.resolve(access, Audience.MCP) is not None

    def test_deleting_a_client_deletes_every_users_conversations_of_it(
        self, store: Store, profile: Profile, client: OAuthClient
    ):
        conversations = _Conversations(store, profile, client)

        deleted = store.oauth.delete_client(client.client_id)
        assert deleted is not None
        assert deleted.client_id == client.client_id

        assert conversations.remaining() == {conversations.other_client, conversations.app}

    def test_delete_unknown_client_is_none(self, store: Store):
        assert store.oauth.delete_client("no-such-client") is None

    def test_delete_client_spares_a_client_registered_otherwise(self, store: Store, client: OAuthClient):
        assert store.oauth.delete_client(client.client_id, registered_by="operator") is None

        assert store.oauth.get_client(client.client_id) is not None

    def test_rotate_secret_replaces_the_hash(self, store: Store):
        client, old = store.oauth.register_client(
            "Slack", ["https://slack.example.com/cb"], registered_by="operator", confidential=True
        )
        assert old is not None

        rotated = store.oauth.rotate_secret(client.client_id)

        assert rotated is not None
        rotated_client, new = rotated
        assert new.startswith("ilc_") and new != old
        stored = store.oauth.get_client(client.client_id)
        assert stored is not None
        assert stored.secret_hash == hash_token(new) == rotated_client.secret_hash

    def test_rotating_leaves_the_clients_grants_untouched(self, store: Store, profile: Profile):
        client, _ = store.oauth.register_client(
            "Slack", ["https://slack.example.com/cb"], registered_by="operator", confidential=True
        )
        refresh = store.oauth.issue_refresh(profile.id, client.client_id, Audience.API, family_id=uuid4())
        access = store.tokens.create_access(profile.id, client.client_id, Audience.API)

        assert store.oauth.rotate_secret(client.client_id) is not None

        assert store.tokens.resolve(access, Audience.API) is not None
        assert store.oauth.rotate_refresh(refresh, client.client_id) is not None

    def test_rotate_secret_refuses_a_public_unknown_or_otherwise_registered_client(
        self, store: Store, client: OAuthClient
    ):
        dynamic, _ = store.oauth.register_client(
            "Connector", ["https://connector.example.com/cb"], registered_by="dynamic", confidential=True
        )

        assert store.oauth.rotate_secret(client.client_id) is None
        assert store.oauth.rotate_secret("no-such-client") is None
        assert store.oauth.rotate_secret(dynamic.client_id, registered_by="operator") is None
        assert store.oauth.get_client(dynamic.client_id) == dynamic

    def test_a_metadata_client_is_kept_under_its_url_as_a_public_client(self, store: Store):
        saved = store.oauth.save_metadata_client(METADATA_CLIENT_ID, "Claude Code", ["http://localhost/callback"])

        assert saved.client_id == METADATA_CLIENT_ID
        assert saved.secret_hash is None
        assert saved.registered_by == "metadata"
        assert saved.created_at is not None
        assert store.oauth.get_client(METADATA_CLIENT_ID) == saved

    def test_saving_a_metadata_client_again_rewrites_it_and_keeps_its_grants(
        self, store: Store, profile: Profile, auth_db: Engine
    ):
        first = store.oauth.save_metadata_client(METADATA_CLIENT_ID, "Claude Code", ["http://localhost/callback"])
        refresh = store.oauth.issue_refresh(profile.id, METADATA_CLIENT_ID, Audience.MCP, family_id=uuid4())
        _age(auth_db, first, timedelta(days=3))

        saved = store.oauth.save_metadata_client(METADATA_CLIENT_ID, "Claude", ["http://127.0.0.1/callback"])

        assert saved.name == "Claude"
        assert saved.redirect_uris == ["http://127.0.0.1/callback"]
        assert saved.created_at is not None
        assert saved.created_at > datetime.now(UTC) - timedelta(minutes=1)
        assert store.oauth.find_refresh(refresh) is not None
        assert [client.client_id for client in store.oauth.list_clients()] == [METADATA_CLIENT_ID]


METADATA_CLIENT_ID = "https://claude.ai/oauth/claude-code-client-metadata"

UNUSED_FOR = timedelta(hours=24)
ENDED_FOR = timedelta(days=30)


def _age(auth_db: Engine, client: OAuthClient, age: timedelta) -> None:
    """Backdate a client's registration.

    Args:
        auth_db: The fixture database.
        client: The client to age.
        age: How long ago it registered.
    """
    with session_scope(auth_db) as session:
        session.connection().execute(
            update(OAuthClient)
            .where(col(OAuthClient.client_id) == client.client_id)
            .values(created_at=datetime.now(UTC) - age)
        )
        session.commit()


def _expire_refresh(auth_db: Engine, client: OAuthClient, ago: timedelta) -> None:
    """Make every refresh token of a client expire at a given time in the past.

    Args:
        auth_db: The fixture database.
        client: The client whose refresh tokens to expire.
        ago: How long ago they expired.
    """
    with session_scope(auth_db) as session:
        session.connection().execute(
            update(RefreshToken)
            .where(col(RefreshToken.client_id) == client.client_id)
            .values(expires_at=datetime.now(UTC) - ago)
        )
        session.commit()


class TestPruneClients:
    def _prune(self, store: Store) -> set[str]:
        return {client.client_id for client in store.oauth.prune_clients(unused_for=UNUSED_FOR, ended_for=ENDED_FOR)}

    def test_a_dynamic_client_never_granted_past_the_window_is_deleted(
        self, store: Store, client: OAuthClient, auth_db: Engine
    ):
        _age(auth_db, client, UNUSED_FOR + timedelta(minutes=1))

        assert self._prune(store) == {client.client_id}
        assert store.oauth.get_client(client.client_id) is None

    def test_a_dynamic_client_still_in_the_window_is_kept(self, store: Store, client: OAuthClient, auth_db: Engine):
        _age(auth_db, client, UNUSED_FOR - timedelta(minutes=1))

        assert self._prune(store) == set()
        assert store.oauth.get_client(client.client_id) is not None

    def test_an_operator_client_is_never_deleted(self, store: Store, auth_db: Engine):
        operator, _ = store.oauth.register_client(
            "Slack", ["https://slack.example.com/cb"], registered_by="operator", confidential=True
        )
        _age(auth_db, operator, timedelta(days=365))

        assert self._prune(store) == set()
        assert store.oauth.get_client(operator.client_id) is not None

    def test_a_metadata_client_unused_past_the_window_is_deleted(self, store: Store, auth_db: Engine):
        metadata = store.oauth.save_metadata_client(METADATA_CLIENT_ID, "Claude Code", ["http://localhost/callback"])
        _age(auth_db, metadata, UNUSED_FOR + timedelta(minutes=1))

        assert self._prune(store) == {METADATA_CLIENT_ID}
        assert store.oauth.get_client(METADATA_CLIENT_ID) is None

    def test_a_metadata_client_read_again_is_kept(self, store: Store, auth_db: Engine):
        metadata = store.oauth.save_metadata_client(METADATA_CLIENT_ID, "Claude Code", ["http://localhost/callback"])
        _age(auth_db, metadata, UNUSED_FOR + timedelta(minutes=1))
        store.oauth.save_metadata_client(METADATA_CLIENT_ID, "Claude Code", ["http://localhost/callback"])

        assert self._prune(store) == set()

    def test_a_client_holding_a_live_refresh_token_is_kept(
        self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine
    ):
        store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP, family_id=uuid4())
        _age(auth_db, client, timedelta(days=365))

        assert self._prune(store) == set()

    def test_a_client_whose_last_refresh_token_expired_long_ago_is_deleted_with_its_grants(
        self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine
    ):
        refresh = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP, family_id=uuid4())
        org_id = store.organisations.create(name="Acme", creator_id=profile.id).id
        conversation = store.conversations.create(org_id, profile.id, client.client_id)
        _age(auth_db, client, timedelta(days=365))
        _expire_refresh(auth_db, client, ENDED_FOR + timedelta(days=1))

        assert self._prune(store) == {client.client_id}
        assert store.oauth.find_refresh(refresh) is None
        with session_scope(auth_db) as session:
            assert session.get(Conversation, conversation.id) is None

    def test_a_client_whose_last_refresh_token_expired_recently_is_kept(
        self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine
    ):
        store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP, family_id=uuid4())
        _age(auth_db, client, timedelta(days=365))
        _expire_refresh(auth_db, client, ENDED_FOR - timedelta(days=1))

        assert self._prune(store) == set()

    def test_a_client_with_a_code_not_yet_exchanged_is_kept(
        self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine
    ):
        _issue_code(store, profile, client)
        _age(auth_db, client, UNUSED_FOR + timedelta(hours=1))

        assert self._prune(store) == set()


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
        before = datetime.now(UTC)
        code = store.oauth.consume_code(_issue_code(store, profile, client))

        assert code is not None
        assert before + timedelta(minutes=10) <= code.expires_at <= datetime.now(UTC) + timedelta(minutes=10)

    def test_code_expires(self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine):
        raw = _issue_code(store, profile, client)
        with session_scope(auth_db) as session:
            session.connection().execute(
                update(AuthorizationCode).values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
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
                update(AuthorizationCode).values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
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
        before = datetime.now(UTC)
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
                update(RefreshToken).values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
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
        other_user = store.profiles.upsert(
            issuer="https://accounts.google.com", subject="g-other", email="other@example.com", name="Other"
        )
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
        other_user = store.profiles.upsert(
            issuer="https://accounts.google.com", subject="g-other", email="other@example.com", name="Other"
        )
        mine = _issue_code(store, profile, client)
        theirs = _issue_code(store, other_user, client)

        store.oauth.revoke_client_for_user(profile.id, client.client_id)

        assert store.oauth.consume_code(mine) is None
        assert store.oauth.consume_code(theirs) is not None

    def test_revoking_a_client_for_a_user_deletes_their_conversations_of_it(
        self, store: Store, profile: Profile, client: OAuthClient
    ):
        conversations = _Conversations(store, profile, client)
        store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)

        store.oauth.revoke_client_for_user(profile.id, client.client_id)

        assert conversations.remaining() == conversations.survivors()

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
                .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
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

    def test_revoking_a_family_deletes_the_users_conversations_of_its_client(
        self, store: Store, profile: Profile, client: OAuthClient
    ):
        conversations = _Conversations(store, profile, client)
        raw = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)
        presented = store.oauth.find_refresh(raw)
        assert presented is not None

        store.oauth.revoke_family(presented)

        assert conversations.remaining() == conversations.survivors()

    def test_a_reused_refresh_token_deletes_the_users_conversations_of_its_client(
        self, store: Store, profile: Profile, client: OAuthClient
    ):
        conversations = _Conversations(store, profile, client)
        old = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)
        assert store.oauth.rotate_refresh(old, client.client_id) is not None

        assert store.oauth.rotate_refresh(old, client.client_id) is None

        assert conversations.remaining() == conversations.survivors()

    def test_a_reused_code_deletes_the_users_conversations_of_its_client(
        self, store: Store, profile: Profile, client: OAuthClient
    ):
        conversations = _Conversations(store, profile, client)
        raw = _issue_code(store, profile, client)
        assert store.oauth.consume_code(raw) is not None

        assert store.oauth.consume_code(raw) is None

        assert conversations.remaining() == conversations.survivors()

    def test_a_replay_into_a_revoked_family_spares_a_newer_grants_conversations(
        self, store: Store, profile: Profile, client: OAuthClient
    ):
        old = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)
        rotated = store.oauth.rotate_refresh(old, client.client_id)
        assert rotated is not None
        store.oauth.revoke_family(rotated)
        store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)
        org_id = store.organisations.create(name="Acme", creator_id=profile.id).id
        newer = store.conversations.create(org_id, profile.id, client.client_id)

        assert store.oauth.rotate_refresh(old, client.client_id) is None

        assert store.conversations.get(newer.id, org_id=org_id, user_id=profile.id).client_id == client.client_id

    def test_find_refresh_of_an_unknown_token_is_none(self, store: Store):
        assert store.oauth.find_refresh("ilr_no-such-token") is None


RETENTION = timedelta(days=30)


def _set_code(auth_db: Engine, raw: str, **values: Any) -> None:
    """Overwrite columns of one authorization code.

    Args:
        auth_db: The fixture database.
        raw: The raw code whose row to change.
        **values: The columns to set.
    """
    with session_scope(auth_db) as session:
        session.connection().execute(
            update(AuthorizationCode).where(col(AuthorizationCode.code_hash) == hash_token(raw)).values(**values)
        )
        session.commit()


def _set_refresh(auth_db: Engine, raw: str, **values: Any) -> None:
    """Overwrite columns of one refresh token.

    Args:
        auth_db: The fixture database.
        raw: The raw refresh token whose row to change.
        **values: The columns to set.
    """
    with session_scope(auth_db) as session:
        session.connection().execute(
            update(RefreshToken).where(col(RefreshToken.token_hash) == hash_token(raw)).values(**values)
        )
        session.commit()


def _code_exists(auth_db: Engine, raw: str) -> bool:
    """Whether an authorization code is still stored.

    Args:
        auth_db: The fixture database.
        raw: The raw code.

    Returns:
        ``True`` when its row is still there.
    """
    with session_scope(auth_db) as session:
        return session.get(AuthorizationCode, hash_token(raw)) is not None


class TestPurge:
    def _chain(self, store: Store, profile: Profile, client: OAuthClient) -> tuple[str, str]:
        """Start a family and rotate it once.

        Args:
            store: The store to issue through.
            profile: The user the family is for.
            client: The client holding it.

        Returns:
            ``(rotated, newest)``: the raw token already rotated and its live successor.
        """
        rotated = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)
        retired = store.oauth.rotate_refresh(rotated, client.client_id)
        assert retired is not None
        newest = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP, family_id=retired.family_id)
        return rotated, newest

    def test_a_code_expired_past_the_window_is_deleted_whether_spent_or_not(
        self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine
    ):
        spent = _issue_code(store, profile, client)
        assert store.oauth.consume_code(spent) is not None
        unspent = _issue_code(store, profile, client)
        long_ago = datetime.now(UTC) - RETENTION - timedelta(minutes=1)
        _set_code(auth_db, spent, expires_at=long_ago)
        _set_code(auth_db, unspent, expires_at=long_ago)

        assert store.oauth.purge(ended_for=RETENTION) == 2

        assert not _code_exists(auth_db, spent)
        assert not _code_exists(auth_db, unspent)

    def test_a_code_expired_inside_the_window_or_still_valid_is_kept(
        self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine
    ):
        recent = _issue_code(store, profile, client)
        assert store.oauth.consume_code(recent) is not None
        _set_code(auth_db, recent, expires_at=datetime.now(UTC) - RETENTION + timedelta(minutes=1))
        valid = _issue_code(store, profile, client)

        assert store.oauth.purge(ended_for=RETENTION) == 0

        assert _code_exists(auth_db, recent)
        assert _code_exists(auth_db, valid)

    def test_a_family_whose_newest_token_expired_past_the_window_is_deleted_whole(
        self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine
    ):
        rotated, newest = self._chain(store, profile, client)
        long_ago = datetime.now(UTC) - RETENTION - timedelta(minutes=1)
        _set_refresh(auth_db, rotated, expires_at=long_ago - timedelta(days=1))
        _set_refresh(auth_db, newest, expires_at=long_ago)

        assert store.oauth.purge(ended_for=RETENTION) == 2

        assert store.oauth.find_refresh(rotated) is None
        assert store.oauth.find_refresh(newest) is None

    def test_a_family_whose_newest_token_expired_inside_the_window_is_kept(
        self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine
    ):
        rotated, newest = self._chain(store, profile, client)
        _set_refresh(auth_db, rotated, expires_at=datetime.now(UTC) - timedelta(days=365))
        _set_refresh(auth_db, newest, expires_at=datetime.now(UTC) - RETENTION + timedelta(minutes=1))

        assert store.oauth.purge(ended_for=RETENTION) == 0

        assert store.oauth.find_refresh(rotated) is not None
        assert store.oauth.find_refresh(newest) is not None

    def test_a_live_family_keeps_a_rotated_token_long_past_the_window_so_its_replay_still_revokes(
        self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine
    ):
        rotated, newest = self._chain(store, profile, client)
        _set_refresh(
            auth_db,
            rotated,
            rotated_at=datetime.now(UTC) - timedelta(days=365),
            expires_at=datetime.now(UTC) - timedelta(days=335),
        )

        assert store.oauth.purge(ended_for=RETENTION) == 0

        assert store.oauth.rotate_refresh(rotated, client.client_id) is None
        assert store.oauth.rotate_refresh(newest, client.client_id) is None

    def test_a_family_revoked_past_the_window_is_deleted_whole(
        self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine
    ):
        rotated, newest = self._chain(store, profile, client)
        token = store.oauth.find_refresh(newest)
        assert token is not None
        store.oauth.revoke_family(token)
        long_ago = datetime.now(UTC) - RETENTION - timedelta(minutes=1)
        _set_refresh(auth_db, rotated, revoked_at=long_ago)
        _set_refresh(auth_db, newest, revoked_at=long_ago)

        assert store.oauth.purge(ended_for=RETENTION) == 2

        assert store.oauth.find_refresh(rotated) is None
        assert store.oauth.find_refresh(newest) is None

    def test_a_family_revoked_inside_the_window_is_kept(
        self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine
    ):
        rotated, newest = self._chain(store, profile, client)
        token = store.oauth.find_refresh(newest)
        assert token is not None
        store.oauth.revoke_family(token)

        assert store.oauth.purge(ended_for=RETENTION) == 0

        assert store.oauth.find_refresh(rotated) is not None
        assert store.oauth.find_refresh(newest) is not None

    def test_only_the_ended_family_goes(self, store: Store, profile: Profile, client: OAuthClient, auth_db: Engine):
        ended = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)
        _set_refresh(auth_db, ended, expires_at=datetime.now(UTC) - RETENTION - timedelta(days=1))
        live = store.oauth.issue_refresh(profile.id, client.client_id, Audience.MCP)

        assert store.oauth.purge(ended_for=RETENTION) == 1

        assert store.oauth.find_refresh(ended) is None
        assert store.oauth.rotate_refresh(live, client.client_id) is not None


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
