"""Tests for ``interloper_api.routes.oauth`` (connector connect flow).

Providers come from the core registry (``interloper.oauth``); connector
OAuth *app* credentials are read at the route level from provider-scoped
environment variables (``<PROVIDER>_CLIENT_ID`` / ``_CLIENT_SECRET`` /
``_REDIRECT_URI``).  These tests cover that resolution, the public
``/providers`` endpoint, and the spec-driven generic token exchange.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.oauth import provider, providers

from interloper_api.dependencies import get_current_user, get_store
from interloper_api.routes import oauth as oauth_module

_SUFFIXES = ("CLIENT_ID", "CLIENT_SECRET", "REDIRECT_URI")


@pytest.fixture(autouse=True)
def _clean_oauth_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate tests from ambient / .env connector OAuth variables."""
    for key in providers():
        for suffix in _SUFFIXES:
            monkeypatch.delenv(f"{key.upper()}_{suffix}", raising=False)


def _configure(monkeypatch: pytest.MonkeyPatch, provider: str, **vals: str) -> None:
    for suffix, val in vals.items():
        monkeypatch.setenv(f"{provider.upper()}_{suffix.upper()}", val)


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(oauth_module.router)
    # Stand in for an authenticated user; auth itself is covered separately.
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=uuid4())
    return TestClient(app)


def test_provider_config_reads_bare_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(monkeypatch, "amazon", client_id="id", client_secret="secret", redirect_uri="uri")

    cfg = oauth_module._ProviderConfig("amazon")

    assert (cfg.client_id, cfg.client_secret, cfg.redirect_uri) == ("id", "secret", "uri")
    assert cfg.configured is True


def test_load_providers_marks_only_fully_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(monkeypatch, "amazon", client_id="id", client_secret="secret", redirect_uri="uri")
    _configure(monkeypatch, "criteo", client_id="id-only")  # incomplete -> not configured

    providers = oauth_module._load_providers()

    assert providers["amazon"].configured is True
    assert providers["criteo"].configured is False
    assert providers["tiktok"].configured is False


def test_list_providers_returns_configured_only_with_registry_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(
        monkeypatch,
        "amazon",
        client_id="amzn-id",
        client_secret="amzn-secret",
        redirect_uri="https://app.example.com/auth/amazon",
    )

    resp = _client().get("/oauth/providers")

    assert resp.status_code == 200
    body = resp.json()
    assert [p["key"] for p in body] == ["amazon"]
    amazon = body[0]
    assert amazon["client_id"] == "amzn-id"
    assert amazon["redirect_uri"] == "https://app.example.com/auth/amazon"
    assert amazon["auth_url"] == "https://www.amazon.com/ap/oa"
    assert amazon["label"] == "Amazon"
    # secrets must never be exposed by the public metadata endpoint
    assert "client_secret" not in amazon


def test_exchange_unknown_provider_is_rejected() -> None:
    resp = _client().post("/oauth/nope", json={"code": "x"})
    assert resp.status_code == 400


def test_exchange_unconfigured_provider_is_rejected() -> None:
    resp = _client().post("/oauth/amazon", json={"code": "x"})
    assert resp.status_code == 400


def test_exchange_requires_authentication(monkeypatch: pytest.MonkeyPatch) -> None:
    # Fully configured provider, but no session: the token exchange must not
    # be reachable anonymously (it spends the in-house app credentials).
    _configure(monkeypatch, "amazon", client_id="id", client_secret="secret", redirect_uri="uri")
    app = FastAPI()
    app.include_router(oauth_module.router)
    app.dependency_overrides[get_store] = lambda: SimpleNamespace()
    resp = TestClient(app).post("/oauth/amazon", json={"code": "x"})
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Generic exchange request shaping
# ---------------------------------------------------------------------------


def _capture_client(captured: list[httpx.Request]) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"refresh_token": "rt"})

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _cfg(monkeypatch: pytest.MonkeyPatch, provider: str) -> oauth_module._ProviderConfig:
    _configure(monkeypatch, provider, client_id="cid", client_secret="cs", redirect_uri="https://r")
    return oauth_module._ProviderConfig(provider)


def test_exchange_post_json(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[httpx.Request] = []
    spec = provider("amazon")

    async def run() -> dict:
        async with _capture_client(captured) as client:
            return await oauth_module._exchange(client, spec, _cfg(monkeypatch, "amazon"), "the-code")

    result = asyncio.run(run())

    req = captured[0]
    assert req.method == "POST"
    assert str(req.url) == spec.token_url
    assert json.loads(req.content) == {
        "grant_type": "authorization_code",
        "code": "the-code",
        "redirect_uri": "https://r",
        "client_id": "cid",
        "client_secret": "cs",
    }
    # The app secret is never returned: only the provider's token response.
    assert result == {"refresh_token": "rt"}


def test_exchange_post_form(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[httpx.Request] = []
    spec = provider("linkedin")

    async def run() -> None:
        async with _capture_client(captured) as client:
            await oauth_module._exchange(client, spec, _cfg(monkeypatch, "linkedin"), "the-code")

    asyncio.run(run())

    req = captured[0]
    assert req.method == "POST"
    assert req.headers["Content-Type"] == "application/x-www-form-urlencoded"
    assert "grant_type=authorization_code" in req.content.decode()
    assert "code=the-code" in req.content.decode()


def test_exchange_get_omits_grant_type(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[httpx.Request] = []
    spec = provider("facebook")

    async def run() -> None:
        async with _capture_client(captured) as client:
            await oauth_module._exchange(client, spec, _cfg(monkeypatch, "facebook"), "the-code")

    asyncio.run(run())

    req = captured[0]
    assert req.method == "GET"
    assert req.url.params["code"] == "the-code"
    assert "grant_type" not in req.url.params


def test_exchange_basic_auth_header(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[httpx.Request] = []
    spec = provider("pinterest")

    async def run() -> None:
        async with _capture_client(captured) as client:
            await oauth_module._exchange(client, spec, _cfg(monkeypatch, "pinterest"), "the-code")

    asyncio.run(run())

    assert captured[0].headers["Authorization"].startswith("Basic ")


def test_exchange_renamed_params(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[httpx.Request] = []
    spec = provider("tiktok")

    async def run() -> None:
        async with _capture_client(captured) as client:
            await oauth_module._exchange(client, spec, _cfg(monkeypatch, "tiktok"), "the-code")

    asyncio.run(run())

    body = json.loads(captured[0].content)
    assert body == {"auth_code": "the-code", "app_id": "cid", "secret": "cs"}
