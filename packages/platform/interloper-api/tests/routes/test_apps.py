"""Tests for ``interloper_api.routes.apps``: the applications a user has authorised.

The routes run over a real store (in-memory SQLite). An application is
authorised while it holds a live refresh token for the caller; revoking it
ends every token it holds for them.
"""

from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from interloper_db import Audience, Profile, Store

from interloper_api.app import create_app

PREFIX = "/api/auth/apps"
REDIRECT_URI = "http://127.0.0.1:8765/callback"


@pytest.fixture
def client(oauth_store: Store, fake_settings: SimpleNamespace) -> TestClient:
    fake_settings.agent.enabled = False
    return TestClient(create_app(store=oauth_store, settings=fake_settings))


@pytest.fixture
def user(oauth_store: Store, client: TestClient) -> Profile:
    """A user logged in to the app: the client carries their session cookie.

    Returns:
        The profile.
    """
    profile = oauth_store.profiles.upsert(google_id="g-user", email="user@example.com", name="User")
    client.cookies.set("session_token", oauth_store.tokens.create_session(profile.id))
    return profile


def _app(store: Store, name: str) -> str:
    client, _ = store.oauth.register_client(name, [REDIRECT_URI], registered_by="dynamic")
    return client.client_id


def _grant(store: Store, user: Profile, client_id: str) -> tuple[str, str]:
    issued = store.oauth.issue_tokens(user.id, client_id, Audience.MCP, uuid4())
    assert issued is not None
    return issued


def test_lists_clients_with_live_refresh(client: TestClient, oauth_store: Store, user: Profile):
    live = _app(oauth_store, "Claude Code")
    revoked = _app(oauth_store, "Cursor")
    _app(oauth_store, "Never approved")
    others = _app(oauth_store, "Someone else's")
    _grant(oauth_store, user, live)
    _grant(oauth_store, user, revoked)
    oauth_store.oauth.revoke_client_for_user(user.id, revoked)
    other = oauth_store.profiles.upsert(google_id="g-other", email="other@example.com", name="Other")
    _grant(oauth_store, other, others)

    response = client.get(PREFIX)

    assert response.status_code == 200
    body = response.json()
    assert [app["id"] for app in body] == [live]
    assert body[0]["name"] == "Claude Code"
    assert body[0]["redirect_uris"] == [REDIRECT_URI]
    assert set(body[0]) == {"id", "name", "redirect_uris", "created_at"}


def test_revoke_app_kills_its_tokens(client: TestClient, oauth_store: Store, user: Profile):
    client_id = _app(oauth_store, "Claude Code")
    access, refresh = _grant(oauth_store, user, client_id)
    kept = _app(oauth_store, "Cursor")
    kept_access, _ = _grant(oauth_store, user, kept)

    response = client.delete(f"{PREFIX}/{client_id}")

    assert response.status_code == 204
    assert oauth_store.tokens.resolve(access, Audience.MCP) is None
    assert oauth_store.oauth.rotate_refresh(refresh, client_id) is None
    assert [app["id"] for app in client.get(PREFIX).json()] == [kept]
    assert oauth_store.tokens.resolve(kept_access, Audience.MCP) is not None


def test_revoking_an_app_twice_or_one_never_approved_is_a_no_op(client: TestClient, oauth_store: Store, user: Profile):
    client_id = _app(oauth_store, "Claude Code")
    _grant(oauth_store, user, client_id)

    assert client.delete(f"{PREFIX}/{client_id}").status_code == 204
    assert client.delete(f"{PREFIX}/{client_id}").status_code == 204
    assert client.delete(f"{PREFIX}/unknown-client").status_code == 204


def test_revoke_spares_other_users(client: TestClient, oauth_store: Store, user: Profile):
    client_id = _app(oauth_store, "Claude Code")
    _grant(oauth_store, user, client_id)
    other = oauth_store.profiles.upsert(google_id="g-other", email="other@example.com", name="Other")
    other_access, _ = _grant(oauth_store, other, client_id)

    assert client.delete(f"{PREFIX}/{client_id}").status_code == 204

    assert oauth_store.tokens.resolve(other_access, Audience.MCP) is not None
    assert [c.client_id for c in oauth_store.oauth.authorised_clients(other.id)] == [client_id]


def test_apps_answer_the_session_cookie_only(client: TestClient, oauth_store: Store, user: Profile):
    client_id = _app(oauth_store, "Claude Code")
    access, _ = _grant(oauth_store, user, client_id)
    _, personal = oauth_store.tokens.create_personal(user.id, "laptop")
    client.cookies.clear()

    for bearer in (personal, access):
        headers = {"Authorization": f"Bearer {bearer}"}
        assert client.get(PREFIX, headers=headers).status_code == 401
        assert client.delete(f"{PREFIX}/{client_id}", headers=headers).status_code == 401
    assert oauth_store.tokens.resolve(access, Audience.MCP) is not None
