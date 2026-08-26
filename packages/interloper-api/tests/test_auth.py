"""Tests for ``interloper_api.routes.auth`` — login callback policies.

The Google OAuth exchange is faked at the httpx layer; a lightweight fake store
records the calls. Two properties under test: a user whose email is in
``auth_config.super_admin_emails`` is promoted on login (promote-only — an
existing super-admin is left alone), and ``allowed_domains`` gates
profile creation for first-time logins without touching existing profiles.
"""

from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from interloper_api.dependencies import get_auth_config, get_store
from interloper_api.routes import auth as auth_module


class FakeStore:
    """In-memory stand-in implementing only the methods the callback calls."""

    def __init__(self, *, is_super_admin: bool = False, exists: bool = False, invited: bool = False) -> None:
        self.profile = SimpleNamespace(id=uuid4(), is_super_admin=is_super_admin)
        self.exists = exists
        self.invited = invited
        self.promoted: list[UUID] = []
        self.upserted = False

    def get_profile_by_google_id(self, google_id: str) -> SimpleNamespace | None:
        return self.profile if self.exists else None

    def has_pending_invitation(self, email: str) -> bool:
        return self.invited

    def upsert_profile(self, **kwargs) -> SimpleNamespace:
        self.upserted = True
        return self.profile

    def set_super_admin(self, user_id: UUID) -> SimpleNamespace:
        self.promoted.append(user_id)
        self.profile.is_super_admin = True
        return self.profile

    def create_session(self, user_id: UUID) -> str:
        return "token"


def _auth_config(super_admin_emails: list[str], allowed_domains: list[str]) -> SimpleNamespace:
    return SimpleNamespace(
        google_client_id="client-id",
        google_client_secret="client-secret",
        google_redirect_uri="http://localhost/api/auth/google/callback",
        cookie_secure=False,
        session_expiry_days=1,
        super_admin_emails=super_admin_emails,
        allowed_domains=allowed_domains,
    )


def _client(
    store: FakeStore,
    super_admin_emails: list[str] | None = None,
    allowed_domains: list[str] | None = None,
) -> TestClient:
    app = FastAPI()
    app.include_router(auth_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_auth_config] = lambda: _auth_config(
        super_admin_emails or [], allowed_domains or []
    )
    return TestClient(app, follow_redirects=False)


@pytest.fixture(autouse=True)
def fake_google(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fake the token exchange and userinfo fetch."""
    monkeypatch.setattr(
        auth_module.httpx,
        "post",
        lambda *a, **k: SimpleNamespace(status_code=200, json=lambda: {"access_token": "at"}),
    )
    monkeypatch.setattr(
        auth_module.httpx,
        "get",
        lambda *a, **k: SimpleNamespace(
            status_code=200,
            json=lambda: {"id": "google-1", "email": "Boss@Example.com", "name": "Boss"},
        ),
    )


def test_listed_email_is_promoted_case_insensitively() -> None:
    store = FakeStore()
    resp = _client(store, ["boss@example.com"]).get("/auth/google/callback", params={"code": "c"})
    assert resp.status_code == 302
    assert store.promoted == [store.profile.id]


def test_unlisted_email_is_not_promoted() -> None:
    store = FakeStore()
    resp = _client(store, ["someone-else@example.com"]).get("/auth/google/callback", params={"code": "c"})
    assert resp.status_code == 302
    assert store.promoted == []


def test_existing_super_admin_is_not_touched() -> None:
    store = FakeStore(is_super_admin=True)
    resp = _client(store, ["boss@example.com"]).get("/auth/google/callback", params={"code": "c"})
    assert resp.status_code == 302
    assert store.promoted == []


def test_signup_blocked_when_domain_not_allowed() -> None:
    store = FakeStore()
    client = _client(store, allowed_domains=["digitlcloud.com"])
    resp = client.get("/auth/google/callback", params={"code": "c"})
    assert resp.status_code == 302
    assert resp.headers["location"] == "/login?error=signup_not_allowed"
    assert not store.upserted
    assert "session_token" not in resp.cookies


def test_signup_allowed_for_listed_domain() -> None:
    store = FakeStore()
    resp = _client(store, allowed_domains=["example.com"]).get("/auth/google/callback", params={"code": "c"})
    assert resp.status_code == 302
    assert resp.headers["location"] == "/"
    assert store.upserted


def test_existing_profile_bypasses_allowlist() -> None:
    store = FakeStore(exists=True)
    resp = _client(store, allowed_domains=["digitlcloud.com"]).get(
        "/auth/google/callback", params={"code": "c"}
    )
    assert resp.status_code == 302
    assert resp.headers["location"] == "/"
    assert store.upserted


def test_invited_email_can_sign_up() -> None:
    store = FakeStore(invited=True)
    resp = _client(store, allowed_domains=["digitlcloud.com"]).get(
        "/auth/google/callback", params={"code": "c"}
    )
    assert resp.status_code == 302
    assert resp.headers["location"] == "/"
    assert store.upserted


def test_super_admin_email_can_sign_up() -> None:
    store = FakeStore()
    client = _client(store, super_admin_emails=["boss@example.com"], allowed_domains=["digitlcloud.com"])
    resp = client.get("/auth/google/callback", params={"code": "c"})
    assert resp.status_code == 302
    assert resp.headers["location"] == "/"
    assert store.promoted == [store.profile.id]


# -- PATCH /auth/me -------------------------------------------------------------


class FakeProfileStore:
    """In-memory stand-in implementing only ``update_profile``."""

    def __init__(self) -> None:
        self.profile = SimpleNamespace(
            id=uuid4(),
            email="user@example.com",
            name="Google Name",
            avatar_url=None,
            timezone=None,
        )

    def update_profile(self, user_id: UUID, *, name: str | None = None, timezone: str | None = None):
        if name is not None:
            self.profile.name = name
        if timezone is not None:
            self.profile.timezone = timezone
        return self.profile


def _me_client(store: FakeProfileStore) -> TestClient:
    from interloper_api.dependencies import get_current_user

    app = FastAPI()
    app.include_router(auth_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: store.profile
    return TestClient(app)


def test_update_me_sets_name_and_timezone() -> None:
    store = FakeProfileStore()
    resp = _me_client(store).patch("/auth/me", json={"name": "Custom", "timezone": "Europe/Berlin"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "Custom"
    assert body["timezone"] == "Europe/Berlin"


def test_update_me_omitted_fields_stay_untouched() -> None:
    store = FakeProfileStore()
    resp = _me_client(store).patch("/auth/me", json={"timezone": "UTC"})
    assert resp.status_code == 200
    assert resp.json()["name"] == "Google Name"
    assert store.profile.timezone == "UTC"


def test_update_me_rejects_unknown_timezone() -> None:
    store = FakeProfileStore()
    resp = _me_client(store).patch("/auth/me", json={"timezone": "Mars/Olympus_Mons"})
    assert resp.status_code == 422
    assert store.profile.timezone is None


def test_update_me_rejects_empty_name() -> None:
    store = FakeProfileStore()
    resp = _me_client(store).patch("/auth/me", json={"name": ""})
    assert resp.status_code == 422
    assert store.profile.name == "Google Name"
