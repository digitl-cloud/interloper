"""One short line per request: method, route pattern, status, duration; an error line for a 5xx, debug otherwise.

The route pattern (``/api/organisations/{org_id}/runs``), not the URL asked
for, so a line never carries an id or a query string, and a page polling a
route reads as the same line. Only a 5xx is news at the console's default
level; ``-v`` shows every request.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from fastapi.routing import iter_route_contexts
from starlette.routing import Match

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = logging.getLogger(__name__)


class RequestLog:
    """ASGI middleware logging each HTTP request once it is answered, or once it raised.

    Attributes:
        app: The application it wraps.
    """

    def __init__(self, app: ASGIApp) -> None:
        """Wrap the application.

        Args:
            app: The application whose requests are logged.
        """
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Serve the request, then log its line; a websocket or lifespan scope passes through unlogged.

        Args:
            scope: The connection's scope, which routing fills with the matched route.
            receive: The channel the request arrives on.
            send: The channel the response leaves on.
        """
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = time.perf_counter()
        status = 500

        async def record_status(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, record_status)
        finally:
            level = logging.ERROR if status >= 500 else logging.DEBUG
            if logger.isEnabledFor(level):
                milliseconds = round((time.perf_counter() - started) * 1000)
                logger.log(level, "%s %s %d (%d ms)", scope["method"], self._route(scope), status, milliseconds)

    @staticmethod
    def _route(scope: Scope) -> str:
        """Name the route a request matched by its pattern, prefixes included.

        An included router's route knows only its own path, so the request is
        matched again against the application's routes as FastAPI flattens
        them, which only a line about to be printed pays for.

        Args:
            scope: The request's scope.

        Returns:
            The pattern (``/api/organisations/{org_id}/runs``), or the path
            asked for when no route matched it.
        """
        for route in iter_route_contexts(scope["app"].routes):
            match, _ = route.matches(scope)
            if match is Match.FULL:
                return route.path or scope["path"]
        return scope["path"]
