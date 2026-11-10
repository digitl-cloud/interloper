"""Application state: what ``create_app`` installs once, read back per request via ``Depends``.

The state lives on the application (``app.state.interloper``), so two apps
in one process never share it, and every request-scoped dependency reads it
from its own connection rather than from an ambient global.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any

from fastapi import Depends, HTTPException
from interloper.catalog import Catalog
from interloper_db import Store
from starlette.requests import HTTPConnection

from interloper_api.authorization import AuthorizationServer
from interloper_api.login import GoogleLogin


@dataclass(frozen=True)
class State:
    """What the application holds for its whole life.

    Attributes:
        store: The persistence layer every route reads and writes through.
        catalog: The component definitions the deployment ships.
        settings: The full ``AppSettings``; ``None`` on an app built without
            them, whose routes then cannot read a configuration.
        agent: The built assistant, when the agent routes are mounted.
        features: Optional-feature availability, by feature name.
        admin_config: The secrets-redacted configuration snapshot the
            super-admin surface shows.
        authorization_server: The OAuth authorization server, or ``None`` on
            a deployment that cannot run one.
        google_login: The Google login, or ``None`` on a deployment that
            cannot run one.
    """

    store: Store | None = None
    catalog: Catalog | None = None
    settings: Any | None = None
    agent: Any | None = None
    features: dict[str, bool] = field(default_factory=dict)
    admin_config: Any | None = None
    authorization_server: AuthorizationServer | None = None
    google_login: GoogleLogin | None = None


def get_state(connection: HTTPConnection) -> State:
    """The state of the application serving the connection.

    Args:
        connection: The request or websocket being served.

    Returns:
        The application's state; an app that installed none (a bare router
        under test) holds every slot empty.
    """
    return getattr(connection.app.state, "interloper", None) or State()


StateDep = Annotated[State, Depends(get_state)]


def get_store(state: StateDep) -> Store:
    """The store.

    Args:
        state: The application's state.

    Returns:
        The store.

    Raises:
        RuntimeError: If the app was built without one.
    """
    if state.store is None:
        raise RuntimeError("Store not initialized: create_app() was given none")
    return state.store


def get_catalog(state: StateDep) -> Catalog:
    """The catalog; routes that need the serialized form call ``.dump()`` themselves.

    Args:
        state: The application's state.

    Returns:
        The catalog.

    Raises:
        RuntimeError: If the app was built without one.
    """
    if state.catalog is None:
        raise RuntimeError("Catalog not initialized: create_app() was given none")
    return state.catalog


def get_settings(state: StateDep) -> Any:
    """The full ``AppSettings``.

    Args:
        state: The application's state.

    Returns:
        The settings.

    Raises:
        RuntimeError: If the app was built without them.
    """
    if state.settings is None:
        raise RuntimeError("Settings not initialized: create_app() was given none")
    return state.settings


def get_agent(state: StateDep) -> Any:
    """The assistant the chat route runs.

    Args:
        state: The application's state.

    Returns:
        The agent.

    Raises:
        RuntimeError: If the agent routes are not mounted.
    """
    if state.agent is None:
        raise RuntimeError("Agent not initialized: the agent routes are not mounted")
    return state.agent


def get_authorization_server(state: StateDep) -> AuthorizationServer:
    """The OAuth authorization server.

    Args:
        state: The application's state.

    Returns:
        The server.

    Raises:
        HTTPException: 503 when the deployment runs none (no encryption key).
    """
    if state.authorization_server is None:
        raise HTTPException(status_code=503, detail="OAuth authorization server not configured")
    return state.authorization_server


def get_google_login(state: StateDep) -> GoogleLogin:
    """The Google login.

    Args:
        state: The application's state.

    Returns:
        The login.

    Raises:
        HTTPException: 503 when the deployment runs none (no Google client or
            no encryption key).
    """
    if state.google_login is None:
        raise HTTPException(status_code=503, detail="Google login not configured")
    return state.google_login


StoreDep = Annotated[Store, Depends(get_store)]
CatalogDep = Annotated[Catalog, Depends(get_catalog)]
SettingsDep = Annotated[Any, Depends(get_settings)]
AgentDep = Annotated[Any, Depends(get_agent)]
AuthorizationServerDep = Annotated[AuthorizationServer, Depends(get_authorization_server)]
GoogleLoginDep = Annotated[GoogleLogin, Depends(get_google_login)]
