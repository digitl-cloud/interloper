"""Tests for ``interloper_api.routes.auth`` — super-admin bootstrap on login.

The Google OAuth exchange is faked at the httpx layer; a lightweight fake store
records the promotion calls. The property under test: a user whose email is in
``auth_config.super_admin_emails`` is promoted on login, everyone else is not,
and the promotion is promote-only (an existing super-admin is left alone).
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

    def __init__(self, *, is_super_admin: bool = False) -> None:
        self.profile = SimpleNamespace(id=uuid4(), is_super_admin=is_super_admin)
        self.promoted: list[UUID] = []

    def upsert_profile(self, **kwargs) -> SimpleNamespace:
        return self.profile

    def set_super_admin(self, user_id: UUID) -> SimpleNamespace:
        self.promoted.append(user_id)
        self.profile.is_super_admin = True
        return self.profile

    def create_session(self, user_id: UUID) -> str:
        return "token"


def _auth_config(super_admin_emails: list[str]) -> SimpleNamespace:
    return SimpleNamespace(
        google_client_id="client-id",
        google_client_secret="client-secret",
        google_redirect_uri="http://localhost/api/auth/google/callback",
        cookie_secure=False,
        session_expiry_days=1,
        super_admin_emails=super_admin_emails,
    )


def _client(store: FakeStore, super_admin_emails: list[str]) -> TestClient:
    app = FastAPI()
    app.include_router(auth_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_auth_config] = lambda: _auth_config(super_admin_emails)
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
