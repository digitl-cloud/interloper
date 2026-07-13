"""Tests for ``interloper_api.routes.oauth`` (connector connect flow).

Providers come from the core registry (``interloper.oauth``); connector
OAuth *app* credentials are read at the route level from provider-scoped
environment variables (``INTERLOPER_<PROVIDER>_CLIENT_ID`` / ``_CLIENT_SECRET`` /
``_REDIRECT_URI``).  These tests cover that resolution, the public
``/providers`` endpoint, and the spec-driven generic token exchange.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.oauth import PROVIDERS, OAuthAppCredentials

from interloper_api.dependencies import get_current_user, get_store
from interloper_api.routes import oauth as oauth_module

_SUFFIXES = ("CLIENT_ID", "CLIENT_SECRET", "REDIRECT_URI")


@pytest.fixture(autouse=True)
def _clean_oauth_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate tests from ambient / .env connector OAuth variables."""
    for key in dict(PROVIDERS.items()):
        for suffix in _SUFFIXES:
            monkeypatch.delenv(f"INTERLOPER_{key.upper()}_{suffix}", raising=False)


def _configure(monkeypatch: pytest.MonkeyPatch, provider: str, **vals: str) -> None:
    for suffix, val in vals.items():
        monkeypatch.setenv(f"INTERLOPER_{provider.upper()}_{suffix.upper()}", val)


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(oauth_module.router)
    # Stand in for an authenticated user; auth itself is covered separately.
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=uuid4())
    return TestClient(app)


def test_credentials_read_from_provider_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(monkeypatch, "amazon", client_id="id", client_secret="secret", redirect_uri="uri")

    creds = OAuthAppCredentials.from_env("amazon")

    assert creds == OAuthAppCredentials(client_id="id", client_secret="secret", redirect_uri="uri")


def test_partially_configured_provider_resolves_to_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(monkeypatch, "amazon", client_id="id", client_secret="secret", redirect_uri="uri")
    _configure(monkeypatch, "criteo", client_id="id-only")  # incomplete -> not configured

    assert OAuthAppCredentials.from_env("amazon") is not None
    assert OAuthAppCredentials.from_env("criteo") is None
    assert OAuthAppCredentials.from_env("tiktok") is None


def test_startup_status_warns_on_partial_trio(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _configure(monkeypatch, "amazon", client_id="id", client_secret="secret", redirect_uri="uri")
    _configure(monkeypatch, "criteo", client_id="id-only")

    with caplog.at_level("INFO", logger=oauth_module.logger.name):
        oauth_module.log_provider_status()

    warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    assert "criteo" in warnings[0] and "INTERLOPER_CRITEO_CLIENT_SECRET" in warnings[0]
    assert any("amazon" in r.message for r in caplog.records if r.levelname == "INFO")


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
    # be reachable anonymously (it spends the in-house OAuth credentials).
    _configure(monkeypatch, "amazon", client_id="id", client_secret="secret", redirect_uri="uri")
    app = FastAPI()
    app.include_router(oauth_module.router)
    app.dependency_overrides[get_store] = lambda: SimpleNamespace()
    resp = TestClient(app).post("/oauth/amazon", json={"code": "x"})
    assert resp.status_code == 401


# -- Generic exchange request shaping ------------------------------------------


def _capture_client(captured: list[httpx.Request]) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"refresh_token": "rt"})

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _cfg(monkeypatch: pytest.MonkeyPatch, provider: str) -> OAuthAppCredentials:
    _configure(monkeypatch, provider, client_id="cid", client_secret="cs", redirect_uri="https://r")
    creds = OAuthAppCredentials.from_env(provider)
    assert creds is not None
    return creds


async def test_exchange_post_json(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[httpx.Request] = []
    spec = PROVIDERS["amazon"]

    async with _capture_client(captured) as client:
        result = await oauth_module._exchange(client, spec, _cfg(monkeypatch, "amazon"), "the-code")

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


async def test_exchange_post_form(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[httpx.Request] = []
    spec = PROVIDERS["linkedin"]

    async with _capture_client(captured) as client:
        await oauth_module._exchange(client, spec, _cfg(monkeypatch, "linkedin"), "the-code")

    req = captured[0]
    assert req.method == "POST"
    assert req.headers["Content-Type"] == "application/x-www-form-urlencoded"
    assert "grant_type=authorization_code" in req.content.decode()
    assert "code=the-code" in req.content.decode()


async def test_exchange_get_omits_grant_type(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[httpx.Request] = []
    spec = PROVIDERS["facebook"]

    async with _capture_client(captured) as client:
        await oauth_module._exchange(client, spec, _cfg(monkeypatch, "facebook"), "the-code")

    req = captured[0]
    assert req.method == "GET"
    assert req.url.params["code"] == "the-code"
    assert "grant_type" not in req.url.params


async def test_exchange_basic_auth_header(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[httpx.Request] = []
    spec = PROVIDERS["pinterest"]

    async with _capture_client(captured) as client:
        await oauth_module._exchange(client, spec, _cfg(monkeypatch, "pinterest"), "the-code")

    assert captured[0].headers["Authorization"].startswith("Basic ")


async def test_exchange_follows_trailing_slash_redirect(monkeypatch: pytest.MonkeyPatch) -> None:
    # Providers such as TikTok 3xx-redirect to the trailing-slash URL; httpx
    # must follow it rather than surfacing the redirect page as an error.
    spec = PROVIDERS["amazon"]

    def handler(request: httpx.Request) -> httpx.Response:
        if not request.url.path.endswith("/token/"):
            return httpx.Response(307, headers={"Location": f"{request.url}/"})
        return httpx.Response(200, json={"refresh_token": "rt"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), follow_redirects=True
    ) as client:
        result = await oauth_module._exchange(client, spec, _cfg(monkeypatch, "amazon"), "the-code")

    assert result == {"refresh_token": "rt"}


async def test_exchange_renamed_params(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[httpx.Request] = []
    spec = PROVIDERS["tiktok"]

    async with _capture_client(captured) as client:
        await oauth_module._exchange(client, spec, _cfg(monkeypatch, "tiktok"), "the-code")

    body = json.loads(captured[0].content)
    assert body == {"auth_code": "the-code", "app_id": "cid", "secret": "cs"}
