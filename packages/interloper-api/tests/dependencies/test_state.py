"""Tests for ``interloper_api.dependencies.state``."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Annotated, Any

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from interloper_api.dependencies import (
    State,
    get_agent,
    get_authorization_server,
    get_catalog,
    get_google_login,
    get_settings,
    get_store,
)


def _client(state: State) -> TestClient:
    """Mount one probe per dependency on an app carrying *state*.

    Args:
        state: The state the app holds.

    Returns:
        A client for the probe app.
    """
    app = FastAPI()
    app.state.interloper = state

    @app.get("/store")
    def store(store: Annotated[Any, Depends(get_store)]) -> dict[str, Any]:
        return {"name": store.name}

    @app.get("/catalog")
    def catalog(catalog: Annotated[Any, Depends(get_catalog)]) -> dict[str, Any]:
        return {"name": catalog.name}

    @app.get("/settings")
    def settings(settings: Annotated[Any, Depends(get_settings)]) -> dict[str, Any]:
        return {"name": settings.name}

    @app.get("/agent")
    def agent(agent: Annotated[Any, Depends(get_agent)]) -> dict[str, Any]:
        return {"name": agent.name}

    @app.get("/authorization-server")
    def authorization_server(server: Annotated[Any, Depends(get_authorization_server)]) -> dict[str, Any]:
        return {"name": server.name}

    @app.get("/google-login")
    def google_login(login: Annotated[Any, Depends(get_google_login)]) -> dict[str, Any]:
        return {"name": login.name}

    return TestClient(app, raise_server_exceptions=False)


def _state(**slots: Any) -> State:
    return State(**{name: SimpleNamespace(name=name) for name in slots if slots[name]})  # ty: ignore[invalid-argument-type]


class TestInstalledSlots:
    """A slot the app was built with is what its dependency returns."""

    @pytest.mark.parametrize("slot", ["store", "catalog", "settings", "agent"])
    def test_a_slot_reads_back(self, slot: str) -> None:
        response = _client(_state(**{slot: True})).get(f"/{slot}")

        assert response.json() == {"name": slot}

    def test_the_authorization_server_reads_back(self) -> None:
        response = _client(_state(authorization_server=True)).get("/authorization-server")

        assert response.json() == {"name": "authorization_server"}

    def test_the_google_login_reads_back(self) -> None:
        response = _client(_state(google_login=True)).get("/google-login")

        assert response.json() == {"name": "google_login"}

    def test_two_apps_hold_their_own_state(self) -> None:
        first = _client(_state(store=True))
        second = _client(State(store=SimpleNamespace(name="other")))  # ty: ignore[invalid-argument-type]

        assert first.get("/store").json() == {"name": "store"}
        assert second.get("/store").json() == {"name": "other"}


class TestMissingSlots:
    """A slot the app was built without is a loud failure, not a silent ``None``."""

    @pytest.mark.parametrize("slot", ["store", "catalog", "settings", "agent"])
    def test_a_missing_slot_is_a_programming_error(self, slot: str) -> None:
        response = _client(State()).get(f"/{slot}")

        assert response.status_code == 500

    def test_a_missing_authorization_server_is_a_503(self) -> None:
        response = _client(State()).get("/authorization-server")

        assert response.status_code == 503
        assert response.json()["detail"] == "OAuth authorization server not configured"

    def test_a_missing_google_login_is_a_503(self) -> None:
        response = _client(State()).get("/google-login")

        assert response.status_code == 503
        assert response.json()["detail"] == "Google login not configured"

    def test_features_default_to_none_enabled(self) -> None:
        assert State().features == {}
