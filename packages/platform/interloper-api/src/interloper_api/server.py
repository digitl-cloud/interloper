"""The API as a supervised service: a uvicorn server over the FastAPI app."""

from __future__ import annotations

from typing import TYPE_CHECKING

import uvicorn
from interloper.telemetry import instrument_fastapi

from interloper_api.app import create_app

if TYPE_CHECKING:
    from fastapi import FastAPI
    from interloper.catalog import Catalog
    from interloper_db import Store

    from interloper_api.settings import ApiSettings


class ApiServer:
    """The HTTP API, served by uvicorn until stopped.

    Attributes:
        app: The FastAPI application; a composing process may add middleware to it before ``start``.
    """

    def __init__(self, app: FastAPI, server: uvicorn.Server) -> None:
        """Hold the application and its server.

        Args:
            app: The FastAPI application.
            server: The uvicorn server bound to it.
        """
        self.app = app
        self._server = server

    @classmethod
    def from_settings(
        cls,
        settings: ApiSettings,
        *,
        store: Store,
        catalog: Catalog,
        port: int,
        cors_origins: list[str] | None = None,
    ) -> ApiServer:
        """Build the API and its server from settings.

        Args:
            settings: The API's settings; ``server.host`` is the bind host.
            store: The store the routes read and write through.
            catalog: The catalog the routes describe.
            port: The port to bind.
            cors_origins: Origins allowed cross-origin requests; ``None`` allows none
                (dev mode passes the Nuxt dev server's origin).

        Returns:
            The server, not yet started.
        """
        app = create_app(store=store, catalog=catalog, settings=settings, cors_origins=cors_origins)
        instrument_fastapi(app)
        config = uvicorn.Config(
            app, host=settings.server.host, port=port, log_level="info", log_config=None, ws="wsproto"
        )
        return cls(app, uvicorn.Server(config))

    def start(self) -> None:
        """Serve until stopped."""
        self._server.run()

    def stop(self) -> None:
        """Ask the server to finish its requests and return."""
        self._server.should_exit = True
