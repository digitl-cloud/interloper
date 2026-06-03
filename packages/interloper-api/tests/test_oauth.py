"""Tests for ``interloper_api.routes.oauth`` (connector connect flow).

Connector OAuth *app* credentials are read at the route implementation level
from provider-scoped environment variables (``<PROVIDER>_CLIENT_ID`` /
``_CLIENT_SECRET`` / ``_REDIRECT_URI``). These tests cover that resolution and
that the public ``/providers`` endpoint only exposes fully-configured providers.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from interloper_api.dependencies import get_catalog
from interloper_api.routes import oauth as oauth_module

_SUFFIXES = ("CLIENT_ID", "CLIENT_SECRET", "REDIRECT_URI")


@pytest.fixture(autouse=True)
def _clean_oauth_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate tests from ambient / .env connector OAuth variables."""
    for key in oauth_module._PROVIDER_KEYS:
        for suffix in _SUFFIXES:
            monkeypatch.delenv(f"{key.upper()}_{suffix}", raising=False)


def _configure(monkeypatch: pytest.MonkeyPatch, provider: str, **vals: str) -> None:
    for suffix, val in vals.items():
        monkeypatch.setenv(f"{provider.upper()}_{suffix.upper()}", val)


def _client(catalog: dict | None = None) -> TestClient:
    app = FastAPI()
    app.include_router(oauth_module.router)
    app.dependency_overrides[get_catalog] = lambda: (catalog or {})
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


def test_list_providers_returns_configured_only_with_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(
        monkeypatch,
        "amazon",
        client_id="amzn-id",
        client_secret="amzn-secret",
        redirect_uri="https://app.example.com/auth/amazon",
    )
    catalog = {
        "amazon_ads": {
            "config_schema": {
                "x-oauth": {
                    "provider": "amazon",
                    "auth_url": "https://www.amazon.com/ap/oa",
                    "label": "Amazon",
                    "icon": "icon:amazon",
                }
            }
        }
    }

    resp = _client(catalog).get("/oauth/providers")

    assert resp.status_code == 200
    body = resp.json()
    assert [p["key"] for p in body] == ["amazon"]
    amazon = body[0]
    assert amazon["client_id"] == "amzn-id"
    assert amazon["redirect_uri"] == "https://app.example.com/auth/amazon"
    assert amazon["auth_url"] == "https://www.amazon.com/ap/oa"
    # secrets must never be exposed by the public metadata endpoint
    assert "client_secret" not in amazon


def test_exchange_unknown_provider_is_rejected() -> None:
    resp = _client().post("/oauth/nope", json={"code": "x"})
    assert resp.status_code == 400


def test_exchange_unconfigured_provider_is_rejected() -> None:
    resp = _client().post("/oauth/amazon", json={"code": "x"})
    assert resp.status_code == 400
