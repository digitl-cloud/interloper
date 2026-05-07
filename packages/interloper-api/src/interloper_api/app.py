"""FastAPI application factory for the interloper API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from interloper.catalog.base import Catalog
from interloper_db import Store

from interloper_api.dependencies import set_auth_config, set_catalog, set_smtp_config, set_store
from interloper_api.routes import (
    assets,
    auth,
    backfills,
    destinations,
    external,
    jobs,
    oauth,
    organisations,
    resources,
    runs,
    sources,
    ws,
)
from interloper_api.routes import catalog as catalog_routes


def create_app(
    store: Store | None = None,
    catalog: Catalog | None = None,
    auth_config: Any | None = None,
    smtp_config: Any | None = None,
    cors_origins: list[str] | None = None,
    **kwargs: Any,
) -> FastAPI:
    """Create the FastAPI application with all routes.

    Args:
        store: The ``Store`` instance for persistence.
        catalog: Catalog instance.
        auth_config: ``AuthConfig`` instance for authentication settings.
        smtp_config: ``SmtpConfig`` instance for sending invitation emails.
        cors_origins: Allowed CORS origins. Only needed in dev mode for direct
            WebSocket connections that bypass the Vite proxy.
        **kwargs: Additional kwargs forwarded to ``FastAPI()``.

    Returns:
        The configured FastAPI application.
    """
    app = FastAPI(title="Interloper API", lifespan=ws.realtime_lifespan, **kwargs)

    if cors_origins:
        app.add_middleware(
            CORSMiddleware,  # type: ignore[arg-type]
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    if store:
        set_store(store)
    if catalog:
        set_catalog(catalog)
    if auth_config:
        set_auth_config(auth_config)
    if smtp_config:
        set_smtp_config(smtp_config)

    api = APIRouter(prefix="/api")
    api.include_router(auth.router, tags=["auth"])
    api.include_router(organisations.router, tags=["organisations"])
    api.include_router(catalog_routes.router, prefix="/catalog", tags=["catalog"])
    api.include_router(resources.router, prefix="/resources", tags=["resources"])
    api.include_router(sources.router, prefix="/sources", tags=["sources"])
    api.include_router(destinations.router, prefix="/destinations", tags=["destinations"])
    api.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
    api.include_router(runs.router, prefix="/runs", tags=["runs"])
    api.include_router(backfills.router, prefix="/backfills", tags=["backfills"])
    api.include_router(assets.router, prefix="/assets", tags=["assets"])
    api.include_router(oauth.router, tags=["oauth"])
    api.include_router(external.router, prefix="/external", tags=["external"])
    api.include_router(ws.router, tags=["ws"])

    try:
        from interloper_api.routes import agent as agent_routes

        api.include_router(agent_routes.router, prefix="/agent", tags=["agent"])
    except ImportError:
        pass

    @api.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    app.include_router(api)

    return app
