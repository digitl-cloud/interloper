"""Application state: what ``create_app`` installs once, read back per request via ``Depends``.

The state lives on the application (``app.state.interloper``), so two apps
in one process never share it, and every request-scoped dependency reads it
from its own connection rather than from an ambient global.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Annotated, Any

from fastapi import Depends, HTTPException
from interloper.catalog import Catalog
from interloper_db import Store
from starlette.requests import HTTPConnection

from interloper_api.authorization import AuthorizationServer
from interloper_api.login import Login, LoginProvider
from interloper_api.settings import ApiSettings

if TYPE_CHECKING:
    from interloper_api.routes.admin import AdminConfigResponse


@dataclass(frozen=True)
class State:
    """What the application holds for its whole life.

    Attributes:
        store: The persistence layer every route reads and writes through.
        catalog: The component definitions the deployment ships.
        settings: Every section the API reads.
        admin_config: The secrets-redacted configuration snapshot the
            super-admin surface shows.
        agent: The built assistant, when the agent routes are mounted.
        features: Optional-feature availability, by feature name.
        authorization_server: The OAuth authorization server, or ``None`` on
            a deployment that cannot run one.
        login: The sign-in providers offered, or ``None`` on a deployment
            that offers none.
    """

    store: Store
    catalog: Catalog
    settings: ApiSettings
    admin_config: AdminConfigResponse
    agent: Any | None = None
    features: dict[str, bool] = field(default_factory=dict)
    authorization_server: AuthorizationServer | None = None
    login: Login | None = None


def get_state(connection: HTTPConnection) -> State:
    """The state of the application serving the connection.

    Args:
        connection: The request or websocket being served.

    Returns:
        The state ``create_app`` installed on the application.
    """
    return connection.app.state.interloper


StateDep = Annotated[State, Depends(get_state)]


def get_store(state: StateDep) -> Store:
    """The store.

    Args:
        state: The application's state.

    Returns:
        The store.
    """
    return state.store


def get_catalog(state: StateDep) -> Catalog:
    """The catalog; routes that need the serialized form call ``.dump()`` themselves.

    Args:
        state: The application's state.

    Returns:
        The catalog.
    """
    return state.catalog


def get_settings(state: StateDep) -> ApiSettings:
    """Every section the API reads, as the command serving it read them.

    Args:
        state: The application's state.

    Returns:
        The settings.
    """
    return state.settings


def get_features(state: StateDep) -> dict[str, bool]:
    """Optional-feature availability, by feature name.

    Args:
        state: The application's state.

    Returns:
        Whether each optional feature is available.
    """
    return state.features


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


def get_login_provider(provider: str, state: StateDep) -> LoginProvider:
    """The sign-in provider the path names.

    Args:
        provider: The provider's name, from the path.
        state: The application's state.

    Returns:
        The provider.

    Raises:
        HTTPException: 404 when the deployment does not offer it (no client
            configured for it, or no encryption key).
    """
    if state.login is None or provider not in state.login.providers:
        raise HTTPException(status_code=404, detail=f"Unknown sign-in provider {provider!r}")
    return state.login.providers[provider]


StoreDep = Annotated[Store, Depends(get_store)]
CatalogDep = Annotated[Catalog, Depends(get_catalog)]
SettingsDep = Annotated[ApiSettings, Depends(get_settings)]
FeaturesDep = Annotated[dict[str, bool], Depends(get_features)]
AgentDep = Annotated[Any, Depends(get_agent)]
AuthorizationServerDep = Annotated[AuthorizationServer, Depends(get_authorization_server)]
LoginProviderDep = Annotated[LoginProvider, Depends(get_login_provider)]
