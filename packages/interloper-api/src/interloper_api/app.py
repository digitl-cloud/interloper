"""FastAPI application factory for the interloper API."""

from __future__ import annotations

import logging
from types import ModuleType
from typing import Any

from fastapi import APIRouter, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from interloper.catalog.base import Catalog
from interloper.errors import ComponentDriftError, NotFoundError, QuotaExceededError
from interloper_db import Store

from interloper_api.dependencies import (
    set_admin_config,
    set_auth_config,
    set_catalog,
    set_features,
    set_quota_defaults,
    set_smtp_config,
    set_store,
)
from interloper_api.routes import (
    admin,
    auth,
    backfills,
    components,
    health,
    oauth,
    organisations,
    runs,
    tokens,
    websocket,
)
from interloper_api.routes import catalog as catalog_routes

logger = logging.getLogger(__name__)

#: Routers mounted under ``/api`` for every deployment. The agent router is
#: absent by design: it is optional and mounted by :func:`_mount_agent`.
_ROUTE_MODULES: tuple[ModuleType, ...] = (
    auth,
    organisations,
    admin,
    catalog_routes,
    components,
    runs,
    backfills,
    oauth,
    tokens,
    websocket,
    health,
)


# -- Error handling ------------------------------------------------------------


async def _not_found(_request: Request, exception: NotFoundError) -> JSONResponse:
    """Render a missing store target as a plain 404.

    Args:
        _request: The incoming request, unused.
        exception: The raised :class:`NotFoundError`.

    Returns:
        A 404 response carrying the exception message as ``detail``.
    """
    return JSONResponse(status_code=404, content={"detail": str(exception)})


async def _component_drift(_request: Request, exception: ComponentDriftError) -> JSONResponse:
    """Render catalog drift as a conflict rather than a 500.

    Hydrating or running a drifted source/asset cannot succeed until the user
    resolves the drift, so it surfaces as a clean 409 the UI can act on.

    Args:
        _request: The incoming request, unused.
        exception: The raised :class:`ComponentDriftError`.

    Returns:
        A 409 response carrying the exception message as ``detail``.
    """
    return JSONResponse(status_code=409, content={"detail": str(exception)})


async def _quota_exceeded(_request: Request, exception: QuotaExceededError) -> JSONResponse:
    """Render store-level quota enforcement as a 429 with structured context.

    Args:
        _request: The incoming request, unused.
        exception: The raised :class:`QuotaExceededError`.

    Returns:
        A 429 response whose ``detail`` carries the message plus the quota
        name, its limit, and the amount already used.
    """
    return JSONResponse(
        status_code=429,
        content={
            "detail": {
                "message": str(exception),
                "quota": exception.quota,
                "limit": exception.limit,
                "used": exception.used,
            }
        },
    )


#: Framework errors that have an HTTP meaning, and the response each becomes.
#: Anything absent here is a bug and stays a 500.
_ERROR_HANDLERS: dict[type[Exception], Any] = {
    NotFoundError: _not_found,
    ComponentDriftError: _component_drift,
    QuotaExceededError: _quota_exceeded,
}


# -- Application factory -------------------------------------------------------


def create_app(
    store: Store | None = None,
    catalog: Catalog | None = None,
    settings: Any | None = None,
    cors_origins: list[str] | None = None,
    **kwargs: Any,
) -> FastAPI:
    """Create the FastAPI application with all routes.

    Args:
        store: The ``Store`` instance for persistence.
        catalog: Catalog instance.
        settings: Full ``AppSettings``; the factory slices what it needs
            (auth, smtp, agent) and builds the secrets-redacted snapshot for
            the super-admin ``/admin/config`` view. The agent routes mount
            only when enabled (or with no settings) and the ``agent`` extra
            is installed.
        cors_origins: Allowed CORS origins. Only needed in dev mode for direct
            WebSocket connections that bypass the Vite proxy.
        **kwargs: Additional kwargs forwarded to ``FastAPI()``.

    Returns:
        The configured FastAPI application.
    """
    app = FastAPI(title="Interloper API", lifespan=websocket.realtime_lifespan, **kwargs)

    for error_type, handler in _ERROR_HANDLERS.items():
        app.add_exception_handler(error_type, handler)

    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    api = APIRouter(prefix="/api")
    for module in _ROUTE_MODULES:
        api.include_router(module.router)
    agent_available = _mount_agent(api, settings)
    app.include_router(api)

    # After mounting, so the feature flag and the config snapshot can record
    # whether the agent actually mounted. Routes read state per request, never
    # at mount time, so the order is safe.
    _install_state(store, catalog, settings, agent_available=agent_available)

    oauth.log_provider_status()

    return app


# -- Internals -----------------------------------------------------------------


def _mount_agent(api: APIRouter, settings: Any | None) -> bool:
    """Mount the optional agent routes, reporting whether they are available.

    Args:
        api: The ``/api`` router to mount onto.
        settings: Full ``AppSettings``, or ``None`` to mount whenever the
            extra is installed.

    Returns:
        True when the agent routes are mounted.
    """
    if settings is not None and not settings.agent.enabled:
        logger.info("Agent routes not mounted: disabled via settings.")
        return False

    try:
        from interloper_api.routes import agent as agent_routes
    except ImportError:
        logger.warning(
            "Agent routes not mounted: the 'agent' extra is not installed "
            "(install interloper-api[agent]); /agent endpoints will return 404."
        )
        return False

    api.include_router(agent_routes.router)
    return True


def _install_state(
    store: Store | None,
    catalog: Catalog | None,
    settings: Any | None,
    *,
    agent_available: bool,
) -> None:
    """Install the process-wide state the request dependencies read back.

    Args:
        store: The ``Store`` instance, or ``None`` to leave it unset.
        catalog: Catalog instance, or ``None`` to leave it unset.
        settings: Full ``AppSettings``, or ``None`` to install neither the
            settings slices nor the admin config snapshot.
        agent_available: Whether the agent routes mounted, recorded as a
            feature flag and in the admin config snapshot.
    """
    if store:
        set_store(store)
    if catalog:
        set_catalog(catalog)

    set_features({"agent": agent_available})

    if settings:
        set_auth_config(settings.auth)
        set_smtp_config(settings.smtp)
        set_quota_defaults(settings.quota)
        set_admin_config(
            admin.AdminConfigResponse.from_settings(settings, features={"agent": agent_available}, catalog=catalog)
        )
