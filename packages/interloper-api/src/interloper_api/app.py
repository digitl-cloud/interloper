"""FastAPI application factory for the interloper API."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from interloper.catalog.base import Catalog
from interloper.errors import ComponentDriftError, NotFoundError
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
    oauth,
    organisations,
    runs,
    tokens,
    ws,
)
from interloper_api.routes import catalog as catalog_routes

logger = logging.getLogger(__name__)


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
    app = FastAPI(title="Interloper API", lifespan=ws.realtime_lifespan, **kwargs)

    @app.exception_handler(NotFoundError)
    async def _not_found_handler(_request: Request, exc: NotFoundError) -> JSONResponse:
        """Store mutations raise NotFoundError on missing targets — a plain 404."""
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(ComponentDriftError)
    async def _component_drift_handler(_request: Request, exc: ComponentDriftError) -> JSONResponse:
        """A stored component whose catalog key has drifted is a conflict, not a 500.

        Hydrating or running a drifted source/asset can't succeed until the
        user resolves the drift, so surface it as a clean 409 the UI can act on.
        """
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    if store:
        set_store(store)
    if catalog:
        set_catalog(catalog)
    if settings:
        set_auth_config(settings.auth)
        set_smtp_config(settings.smtp)
        set_quota_defaults(settings.quota)

    api = APIRouter(prefix="/api")
    api.include_router(auth.router)
    api.include_router(organisations.router)
    api.include_router(admin.router)
    api.include_router(catalog_routes.router)
    api.include_router(components.router)
    api.include_router(runs.router)
    api.include_router(backfills.router)
    api.include_router(oauth.router)
    api.include_router(tokens.router)
    api.include_router(ws.router)

    agent_available = False
    if settings is None or settings.agent.enabled:
        try:
            from interloper_api.routes import agent as agent_routes

            api.include_router(agent_routes.router)
            agent_available = True
        except ImportError:
            logger.warning(
                "Agent routes not mounted: the 'agent' extra is not installed "
                "(install interloper-api[agent]); /agent endpoints will return 404."
            )
    else:
        logger.info("Agent routes not mounted: disabled via settings.")
    set_features({"agent": agent_available})

    if settings:
        set_admin_config(admin.build_config_snapshot(settings, features={"agent": agent_available}, catalog=catalog))

    @api.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    app.include_router(api)

    oauth.log_provider_status()

    return app
