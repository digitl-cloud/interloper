"""The SPA's built output, its Nuxt source checkout, and the fallback that serves it next to the API."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from pathlib import Path

from starlette.responses import FileResponse, HTMLResponse
from starlette.staticfiles import StaticFiles
from starlette.types import ASGIApp, Receive, Scope, Send

_PACKAGE_DIR = Path(__file__).parent

API_PATHS = ("/api", "/.well-known/oauth-authorization-server")

_IMPORT_MAP = re.compile(r'<script type="importmap">(.*?)</script>', re.DOTALL)


def static_dir() -> Path:
    """Return the path to the built SPA static files.

    Returns:
        The directory holding the built ``index.html`` and its assets.

    Raises:
        FileNotFoundError: If the app hasn't been built yet.
    """
    d = _PACKAGE_DIR / "static"
    if not d.exists() or not (d / "index.html").exists():
        msg = "interloper-app static files not found. Run 'make build-app' to build the frontend first."
        raise FileNotFoundError(msg)
    return d


def source_dir() -> Path:
    """Return the path to the Nuxt source directory (for dev mode).

    Returns:
        The Nuxt project directory the dev server runs from.

    Raises:
        FileNotFoundError: If the Nuxt source directory is not available.
    """
    d = _PACKAGE_DIR.parent.parent / "app"
    if not d.exists():
        msg = "Nuxt source directory not found — dev mode requires a source checkout."
        raise FileNotFoundError(msg)
    return d


class SpaFallback:
    """Serve the built SPA for every request the API does not own.

    Each of :data:`API_PATHS` and everything under it (``/api``, and the
    authorization server's metadata at the host root) goes to the wrapped
    application, so an unknown API route keeps its 404 rather than becoming a
    page. Any other path is a file of the build when one exists, else
    ``index.html`` for the client-side router. Non-HTTP traffic (websockets)
    passes through.

    ``interloper dev`` hands it the origins of the extension UIs' Vite dev
    servers. Such a server rewrites every module the app shares to
    ``<origin>/@id/<name>``, so the page's import map gains those URLs, each
    mapped where the map already sends its name: the dev UI then runs on the
    app's own Vue, router and stores.
    """

    def __init__(self, app: ASGIApp, directory: Path, dev_origins: Sequence[str] = ()) -> None:
        """Wrap the application.

        Args:
            app: The application to hand API and non-HTTP traffic to.
            directory: The built SPA's root, holding ``index.html``.
            dev_origins: The origins of the dev servers the page imports
                extension UIs from; none outside ``interloper dev``.
        """
        self.app = app
        self._directory = Path(directory)
        self._files = StaticFiles(directory=self._directory)
        self._index = self._directory / "index.html"
        self._page = self._with_dev_origins(self._index.read_text(), dev_origins) if dev_origins else None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Serve a file of the build, ``index.html``, or delegate to the application.

        Args:
            scope: The ASGI connection scope.
            receive: The ASGI receive callable.
            send: The ASGI send callable.
        """
        path = scope.get("path", "")
        if scope["type"] != "http" or any(path == prefix or path.startswith(f"{prefix}/") for prefix in API_PATHS):
            await self.app(scope, receive, send)
            return
        relative = path.lstrip("/")
        if relative and relative != self._index.name and (self._directory / relative).is_file():
            await self._files(scope, receive, send)
            return
        if self._page is not None:
            await HTMLResponse(self._page)(scope, receive, send)
            return
        await FileResponse(self._index, media_type="text/html")(scope, receive, send)

    @staticmethod
    def _with_dev_origins(page: str, origins: Sequence[str]) -> str:
        """Add each dev server's spelling of every name the page's import map holds.

        Args:
            page: The built ``index.html``.
            origins: The dev servers' origins (``http://127.0.0.1:5173``).

        Returns:
            The page, its import map extended; unchanged when it has none.
        """

        def extend(match: re.Match[str]) -> str:
            import_map = json.loads(match.group(1))
            imports: dict[str, str] = import_map.get("imports", {})
            spellings = {f"{origin}/@id/{name}": url for origin in origins for name, url in imports.items()}
            extended = {**import_map, "imports": {**imports, **spellings}}
            return f'<script type="importmap">{json.dumps(extended)}</script>'

        return _IMPORT_MAP.sub(extend, page, count=1)
