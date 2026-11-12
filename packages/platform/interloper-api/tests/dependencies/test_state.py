"""Tests for ``interloper_api.dependencies.state``."""

from __future__ import annotations

import dataclasses
from typing import Annotated, Any

import pytest
import sqlalchemy
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from interloper_db import Store

from interloper_api.dependencies import (
    State,
    get_agent,
    get_authorization_server,
    get_catalog,
    get_features,
    get_google_login,
    get_settings,
    get_store,
)


def _client(state: State) -> TestClient:
    """Mount one probe per dependency on an app carrying *state*.

    Each probe answers whether the dependency returned the very object the
    state holds in that slot.

    Args:
        state: The state the app holds.

    Returns:
        A client for the probe app.
    """
    app = FastAPI()
    app.state.interloper = state

    @app.get("/store")
    def store(store: Annotated[Any, Depends(get_store)]) -> dict[str, Any]:
        return {"same": store is state.store}

    @app.get("/catalog")
    def catalog(catalog: Annotated[Any, Depends(get_catalog)]) -> dict[str, Any]:
        return {"same": catalog is state.catalog}

    @app.get("/settings")
    def settings(settings: Annotated[Any, Depends(get_settings)]) -> dict[str, Any]:
        return {"same": settings is state.settings}

    @app.get("/features")
    def features(features: Annotated[Any, Depends(get_features)]) -> dict[str, Any]:
        return {"same": features is state.features}

    @app.get("/agent")
    def agent(agent: Annotated[Any, Depends(get_agent)]) -> dict[str, Any]:
        return {"same": agent is state.agent}

    @app.get("/authorization-server")
    def authorization_server(server: Annotated[Any, Depends(get_authorization_server)]) -> dict[str, Any]:
        return {"same": server is state.authorization_server}

    @app.get("/google-login")
    def google_login(login: Annotated[Any, Depends(get_google_login)]) -> dict[str, Any]:
        return {"same": login is state.google_login}

    return TestClient(app, raise_server_exceptions=False)


class TestInstalledSlots:
    """A slot the app was built with is what its dependency returns."""

    @pytest.mark.parametrize("slot", ["store", "catalog", "settings"])
    def test_a_required_slot_reads_back(self, app_state: State, slot: str) -> None:
        assert _client(app_state).get(f"/{slot}").json() == {"same": True}

    @pytest.mark.parametrize(
        ("slot", "path"),
        [("agent", "/agent"), ("authorization_server", "/authorization-server"), ("google_login", "/google-login")],
    )
    def test_an_optional_slot_reads_back(self, app_state: State, slot: str, path: str) -> None:
        state = dataclasses.replace(app_state, **{slot: object()})

        assert _client(state).get(path).json() == {"same": True}

    def test_the_features_read_back(self, app_state: State) -> None:
        state = dataclasses.replace(app_state, features={"agent": True})

        assert _client(state).get("/features").json() == {"same": True}

    def test_two_apps_hold_their_own_state(self, app_state: State) -> None:
        other = Store(app_state.catalog, engine=sqlalchemy.create_engine("sqlite://"))
        first, second = app_state, dataclasses.replace(app_state, store=other)

        assert _client(first).get("/store").json() == {"same": True}
        assert _client(second).get("/store").json() == {"same": True}
        assert first.store is not second.store


class TestMissingSlots:
    """An optional slot the app was built without is a loud failure, not a silent ``None``."""

    def test_a_missing_agent_is_a_programming_error(self, app_state: State) -> None:
        assert _client(app_state).get("/agent").status_code == 500

    def test_a_missing_authorization_server_is_a_503(self, app_state: State) -> None:
        response = _client(app_state).get("/authorization-server")

        assert response.status_code == 503
        assert response.json()["detail"] == "OAuth authorization server not configured"

    def test_a_missing_google_login_is_a_503(self, app_state: State) -> None:
        response = _client(app_state).get("/google-login")

        assert response.status_code == 503
        assert response.json()["detail"] == "Google login not configured"

    def test_features_default_to_none_enabled(self, app_state: State) -> None:
        assert app_state.features == {}
