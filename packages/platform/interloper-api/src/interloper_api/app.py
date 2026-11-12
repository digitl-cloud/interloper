"""FastAPI application factory for the interloper API."""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import APIRouter, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from interloper.catalog import Catalog
from interloper.errors import ConfigError
from interloper_db import (
    ComponentDriftError,
    ConflictError,
    HydrationError,
    InUseError,
    NotFoundError,
    QuotaExceededError,
    Store,
)
from starlette.concurrency import run_in_threadpool

from interloper_api.authorization import AuthorizationServer, CrossOriginMiddleware
from interloper_api.dependencies import State
from interloper_api.login import GoogleLogin
from interloper_api.routes import (
    admin,
    apps,
    auth,
    authorization,
    backfills,
    components,
    executions,
    health,
    oauth,
    organisations,
    overview,
    relations,
    runs,
    tokens,
    websocket,
)
from interloper_api.routes import catalog as catalog_routes
from interloper_api.settings import ApiSettings

logger = logging.getLogger(__name__)

#: Routers mounted under ``/api`` for every deployment: the caller's own, the
#: organisation management and super-admin ones, a run's ingest, and the probes.
_ROUTERS: tuple[APIRouter, ...] = (
    auth.router,
    authorization.router,
    organisations.router,
    admin.router,
    oauth.router,
    tokens.router,
    apps.router,
    runs.router,
    health.router,
)

#: Routers acting in one organisation, mounted under ``/api/organisations/{org_id}``.
#: The agent router is absent by design: it is optional and mounted by
#: :func:`_mount_agent`.
_ORG_ROUTERS: tuple[APIRouter, ...] = (
    organisations.org_router,
    catalog_routes.org_router,
    components.org_router,
    relations.org_router,
    runs.org_router,
    executions.org_router,
    backfills.org_router,
    overview.org_router,
    websocket.org_router,
)

ORG_PREFIX = "/organisations/{org_id}"


# -- Error handling ------------------------------------------------------------


#: Framework errors that have an HTTP meaning, by the status each answers.
#: Drift and an unhydratable record are 409s: the record stays broken until a
#: user repoints, re-keys or rewrites it, so the reason is surfaced instead of
#: an opaque 500. Anything absent here is a bug and stays a 500.
_ERROR_STATUS: dict[type[Exception], int] = {
    ConfigError: 400,
    NotFoundError: 404,
    ConflictError: 409,
    ComponentDriftError: 409,
    HydrationError: 409,
}


async def _plain_error(_request: Request, exception: Exception) -> JSONResponse:
    """Render a framework error as its mapped status, the message as ``detail``.

    Args:
        _request: The incoming request, unused.
        exception: The raised error, an instance of exactly one key of
            :data:`_ERROR_STATUS` (none of the keys subclasses another).

    Returns:
        The response, at the status mapped to the error's class.
    """
    status = next(status for error_type, status in _ERROR_STATUS.items() if isinstance(exception, error_type))
    return JSONResponse(status_code=status, content={"detail": str(exception)})


async def _in_use(_request: Request, exception: InUseError) -> JSONResponse:
    """Render a refused deletion as a 409 naming what still depends on the record.

    Args:
        _request: The incoming request, unused.
        exception: The raised :class:`InUseError`.

    Returns:
        A 409 response whose ``detail`` carries the message and the referrers
        as ``used_by``.
    """
    return JSONResponse(
        status_code=409, content={"detail": {"message": str(exception), "used_by": exception.referrers}}
    )


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


#: Errors whose response carries more than the message.
_STRUCTURED_ERRORS: dict[type[Exception], Any] = {
    InUseError: _in_use,
    QuotaExceededError: _quota_exceeded,
}


def install_error_handlers(app: FastAPI) -> None:
    """Answer every framework error that has an HTTP meaning with its status.

    Routes raise the store's errors as they come; this one mapping turns them
    into responses, so no handler translates an exception by hand.

    Args:
        app: The application to install the handlers on.
    """
    for error_type in _ERROR_STATUS:
        app.add_exception_handler(error_type, _plain_error)
    for error_type, handler in _STRUCTURED_ERRORS.items():
        app.add_exception_handler(error_type, handler)


# -- Application factory -------------------------------------------------------


def create_app(
    store: Store,
    catalog: Catalog,
    settings: ApiSettings,
    cors_origins: list[str] | None = None,
    **kwargs: Any,
) -> FastAPI:
    """Create the FastAPI application with all routes.

    On startup the application promotes the configured super-admins and
    starts the realtime listener.

    Args:
        store: The ``Store`` instance for persistence.
        catalog: Catalog instance.
        settings: Every section the API reads; the factory also builds the
            secrets-redacted snapshot of every declared section for the
            super-admin ``/admin/config`` view. The agent routes
            mount only when they enable the agent and the ``agent`` extra
            is installed.
        cors_origins: Allowed CORS origins. Only needed in dev mode for direct
            WebSocket connections that bypass the Vite proxy.
        **kwargs: Additional kwargs forwarded to ``FastAPI()``.

    Returns:
        The configured FastAPI application.
    """
    app = FastAPI(title="Interloper API", lifespan=_lifespan, **kwargs)

    install_error_handlers(app)

    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    # Added last, so it answers a client-facing endpoint's preflight before the policy above can refuse it.
    app.add_middleware(CrossOriginMiddleware)
    google_login = _google_login(settings)
    if google_login is not None:
        google_login.install(app)

    api = APIRouter(prefix="/api")
    for router in _ROUTERS:
        api.include_router(router)
    for org_router in _ORG_ROUTERS:
        api.include_router(org_router, prefix=ORG_PREFIX)
    agent_model = _mount_agent(api, settings)
    app.include_router(api)
    app.include_router(authorization.well_known_router)

    app.state.interloper = _state(store, catalog, settings, agent_model=agent_model, google_login=google_login)

    oauth.log_provider_status()

    return app


# -- Internals -----------------------------------------------------------------


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Promote the configured super-admins, then serve with the realtime listener running.

    ``auth.super_admin_emails`` is applied here as well as at login, so a
    deployment shipping a new list promotes the profiles that already exist
    without waiting for them to sign in again.

    Args:
        app: The application being started; its state carries the store and
            the settings.

    Yields:
        None, while the application serves.
    """
    state: State = app.state.interloper
    emails = state.settings.auth.super_admin_emails
    promoted = await run_in_threadpool(state.store.profiles.promote_super_admins, emails)
    for profile in promoted:
        logger.info("Promoted profile %s to super-admin.", profile.id)
    async with websocket.realtime_lifespan(app):
        yield


def _mount_agent(api: APIRouter, settings: ApiSettings) -> str | None:
    """Mount the optional agent routes when the settings enable the agent.

    The conversation routes go under the organisation prefix, the tool list outside it.

    Args:
        api: The ``/api`` router to mount onto.
        settings: The API's settings; ``agent`` is ``None`` without the agent package.

    Returns:
        The model the mounted agent runs on, or ``None`` when the routes are not mounted.
    """
    if settings.agent is None or not settings.agent.enabled:
        logger.info("Agent routes not mounted: the agent is not enabled (agent.enabled).")
        return None

    try:
        from interloper_api.routes import agent as agent_routes
    except ImportError:
        logger.warning(
            "Agent routes not mounted: the 'agent' extra is not installed "
            "(install interloper-api[agent]); /agent endpoints will return 404."
        )
        return None

    api.include_router(agent_routes.router)
    api.include_router(agent_routes.org_router, prefix=ORG_PREFIX)
    return settings.agent.model


def _state(
    store: Store,
    catalog: Catalog,
    settings: ApiSettings,
    *,
    agent_model: str | None,
    google_login: GoogleLogin | None,
) -> State:
    """Build the state the request dependencies read back.

    Args:
        store: The ``Store`` instance.
        catalog: Catalog instance.
        settings: The API's settings.
        agent_model: The model the mounted agent runs on, or ``None`` when
            the agent routes are not mounted; recorded as a feature flag and
            in the admin config snapshot.
        google_login: The Google login, already installed on the app, or
            ``None``.

    Returns:
        The state.
    """
    features = {"agent": agent_model is not None}
    agent = None
    if agent_model is not None:
        from interloper_agent import build_agent

        agent = build_agent(agent_model)
    return State(
        store=store,
        catalog=catalog,
        settings=settings,
        admin_config=admin.AdminConfigResponse.from_declared(features=features, catalog=catalog),
        agent=agent,
        features=features,
        authorization_server=_authorization_server(store, settings),
        google_login=google_login,
    )


def _google_login(settings: ApiSettings) -> GoogleLogin | None:
    """Build the Google login, when the deployment can run one.

    Args:
        settings: The API's settings.

    Returns:
        The login, or ``None`` without a Google client or without an
        encryption key (which signs the cookie the login keeps between
        Google's two legs).
    """
    google_login = GoogleLogin.from_settings(settings)
    if google_login is None and settings.auth.google_client_id and not settings.encryption.key:
        logger.warning("Google login unavailable: INTERLOPER_ENCRYPTION_KEY is not configured.")
    return google_login


def _authorization_server(store: Store, settings: ApiSettings) -> AuthorizationServer | None:
    """Build the OAuth authorization server, when the deployment can run one.

    Args:
        store: The ``Store`` instance.
        settings: The API's settings.

    Returns:
        The server, or ``None`` without an encryption key (which seals the
        authorization request while the user decides), or when the issuer is
        refused (plain http on a non-loopback host).
    """
    if not settings.encryption.key:
        logger.warning("OAuth authorization server not mounted: INTERLOPER_ENCRYPTION_KEY is not configured.")
        return None
    try:
        return AuthorizationServer.from_settings(store, settings)
    except ValueError as error:
        logger.warning("OAuth authorization server not mounted: %s (server.external_url).", error)
        return None
