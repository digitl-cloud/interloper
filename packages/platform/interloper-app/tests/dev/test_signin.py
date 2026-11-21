"""Tests for ``interloper_app.dev.signin``."""

from __future__ import annotations

from types import SimpleNamespace
from unittest import mock
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper_api.dependencies import get_settings, get_store
from interloper_api.settings import AuthSettings

from interloper_app.dev.signin import SignIn

DEVELOPER = SimpleNamespace(id=uuid4())


@pytest.fixture
def store() -> SimpleNamespace:
    tokens = mock.Mock()
    tokens.create_session.return_value = "the-session-token"
    organisations = mock.Mock()
    organisations.ensure.return_value = SimpleNamespace(key="weather-pipelines")
    return SimpleNamespace(
        tokens=tokens, organisations=organisations, profiles=SimpleNamespace(local_developer=lambda: DEVELOPER)
    )


def _client(sign_in: SignIn, store: SimpleNamespace) -> TestClient:
    app = FastAPI()
    app.include_router(sign_in.router())
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_settings] = lambda: SimpleNamespace(auth=AuthSettings(cookie_secure=False))
    return TestClient(app, follow_redirects=False)


def test_each_sign_in_draws_its_own_code():
    sign_in = SignIn.generate("Weather pipelines")

    assert len(sign_in.code) >= 40
    assert SignIn.generate("Weather pipelines").code != sign_in.code


def test_the_link_names_the_loopback_address_not_localhost():
    sign_in = SignIn.generate("Weather pipelines")

    assert sign_in.link(3100) == f"http://127.0.0.1:3100/api/dev/sign-in?code={sign_in.code}"


@pytest.mark.parametrize(
    ("extension", "landing"), [("acme", "/weather-pipelines/ext/acme"), (None, "/weather-pipelines")]
)
def test_the_code_signs_in_the_local_developer_where_dev_works(store, extension: str | None, landing: str):
    sign_in = SignIn.generate("Weather pipelines", extension)

    response = _client(sign_in, store).get("/api/dev/sign-in", params={"code": sign_in.code})

    assert response.status_code == 302
    assert response.headers["location"] == landing
    cookie = response.headers["set-cookie"]
    assert cookie.startswith("session_token=the-session-token;")
    assert "HttpOnly" in cookie
    store.tokens.create_session.assert_called_once_with(DEVELOPER.id)
    store.organisations.ensure.assert_called_once_with(
        "Weather pipelines", key="weather-pipelines", admin_id=DEVELOPER.id
    )


@pytest.mark.parametrize("code", ["wrong", ""])
def test_any_other_code_is_refused(store, code: str):
    response = _client(SignIn.generate("Weather pipelines"), store).get("/api/dev/sign-in", params={"code": code})

    assert response.status_code == 403
    store.tokens.create_session.assert_not_called()


def test_the_route_stays_out_of_the_api_schema(store):
    client = _client(SignIn.generate("Weather pipelines"), store)

    assert "/api/dev/sign-in" not in client.get("/openapi.json").json()["paths"]
