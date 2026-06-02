"""Tests for ``interloper_api.routes.oauth`` and connector OAuth settings.

Covers that per-connector OAuth *app* credentials resolve from
:class:`~interloper.settings.OAuthSettings` (env + YAML, mirroring the
``auth``/``smtp`` pattern), and that the connect-flow endpoints only expose
providers whose credentials are fully configured.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.settings import AppSettings, OAuthSettings

from interloper_api.dependencies import get_catalog
from interloper_api.routes import oauth as oauth_module

ActivateOAuth = Callable[..., AppSettings]

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def activate_oauth() -> Iterator[ActivateOAuth]:
    """Activate AppSettings whose ``oauth`` section is set inline, then clear it.

    Passing ``oauth=`` as an init arg makes it win over env/YAML, keeping the
    test hermetic regardless of the runner's cwd or environment.
    """

    def _activate(**oauth_kwargs: str) -> AppSettings:
        settings = AppSettings(oauth=OAuthSettings(**oauth_kwargs))  # type: ignore[call-arg]
        AppSettings.activate(settings)
        return settings

    try:
        yield _activate
    finally:
        AppSettings.clear_active()


def _client(catalog: dict | None = None) -> TestClient:
    app = FastAPI()
    app.include_router(oauth_module.router)
    app.dependency_overrides[get_catalog] = lambda: (catalog or {})
    return TestClient(app)


# ---------------------------------------------------------------------------
# Settings resolution
# ---------------------------------------------------------------------------


def test_oauth_settings_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INTERLOPER_OAUTH_TIKTOK_CLIENT_ID", "id")
    monkeypatch.setenv("INTERLOPER_OAUTH_TIKTOK_CLIENT_SECRET", "secret")
    monkeypatch.setenv("INTERLOPER_OAUTH_TIKTOK_REDIRECT_URI", "uri")

    settings = OAuthSettings()

    assert settings.tiktok_client_id == "id"
    assert settings.tiktok_client_secret == "secret"
    assert settings.tiktok_redirect_uri == "uri"


def test_app_settings_merges_yaml_config_and_env_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The production split: client_id/redirect_uri from YAML (ConfigMap),
    client_secret from env (Secret), merged onto AppSettings.oauth."""
    (tmp_path / "interloper.yaml").write_text(
        "oauth:\n"
        "  amazon_client_id: amzn-id\n"
        "  amazon_redirect_uri: https://app.example.com/auth/amazon\n"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INTERLOPER_OAUTH_AMAZON_CLIENT_SECRET", "amzn-secret")

    oauth = AppSettings.from_sources().oauth

    assert oauth.amazon_client_id == "amzn-id"
    assert oauth.amazon_redirect_uri == "https://app.example.com/auth/amazon"
    assert oauth.amazon_client_secret == "amzn-secret"


# ---------------------------------------------------------------------------
# Provider loading + endpoint
# ---------------------------------------------------------------------------


def test_load_providers_marks_only_fully_configured(activate_oauth: ActivateOAuth) -> None:
    activate_oauth(
        amazon_client_id="id",
        amazon_client_secret="secret",
        amazon_redirect_uri="uri",
        criteo_client_id="id-only",  # incomplete -> not configured
    )

    providers = oauth_module._load_providers()

    assert providers["amazon"].configured is True
    assert providers["criteo"].configured is False
    assert providers["tiktok"].configured is False


def test_list_providers_returns_configured_only_with_metadata(activate_oauth: ActivateOAuth) -> None:
    activate_oauth(
        amazon_client_id="amzn-id",
        amazon_client_secret="amzn-secret",
        amazon_redirect_uri="https://app.example.com/auth/amazon",
    )
    catalog = {
        "amazon_ads": {
            "config_schema": {
                "x-oauth": {
                    "provider": "amazon",
                    "auth_url": "https://www.amazon.com/ap/oa",
                    "label": "Amazon",
                    "icon": "logos:amazon",
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


def test_exchange_unknown_provider_is_rejected(activate_oauth: ActivateOAuth) -> None:
    activate_oauth()
    resp = _client().post("/oauth/nope", json={"code": "x"})
    assert resp.status_code == 400


def test_exchange_unconfigured_provider_is_rejected(activate_oauth: ActivateOAuth) -> None:
    activate_oauth()  # nothing configured
    resp = _client().post("/oauth/amazon", json={"code": "x"})
    assert resp.status_code == 400
