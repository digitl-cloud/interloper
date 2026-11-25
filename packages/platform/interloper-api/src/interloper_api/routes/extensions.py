"""What installed packages bring to the API: their routes, their UI bundles, and the listing of those packages.

A package adds routes by naming an ``APIRouter`` in the ``routes`` section of
its manifest, and a prebuilt UI by naming a `UIBundle` in its ``ui`` section:

```yaml
routes: interloper_acme.routes:router
ui: interloper_acme.ui:UI
```

The router answers under ``/api/organisations/{org_id}/ext/<name>``, where
``<name>`` is the entry's name: the extension's key for a single import
string. The viewer role in the organisation the path names is the floor of
every route; a route that writes raises it with ``EditorDep``. The routes
reuse the API's dependencies (``StoreDep``, ``ViewerDep``, ``EditorDep``,
``load_authorized``), and the contract that keeps one organisation's data
from another is the author's: every row carries ``org_id``, and a fetch by id
goes through ``load_authorized``.

The bundle's files are served under ``/api/ext/<name>/ui/<digest>/`` to anyone,
as the SPA's own are: they hold code, and the data that code reads goes
through the authenticated routes. ``/api`` is the one prefix that reaches the
API in every deployment shape (the dev server's proxy, the single-process app,
nginx). The digest is the hash of the bundle's files, so any change to them
moves every URL and each file can be cached for good, whatever bundler named it.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterable
from typing import Any, Literal

from fastapi import APIRouter
from interloper.extension import MANIFESTS, Extension, ExtensionFailure
from interloper.registry import Registry
from pydantic import BaseModel
from starlette.responses import Response
from starlette.staticfiles import StaticFiles
from starlette.types import Scope

from interloper_api.dependencies import CurrentUserDep
from interloper_api.ui import DevServerUI, UIBundle

_NAME_PATTERN = re.compile(r"[a-z][a-z0-9_]*")

UI_PATH = "/api/ext/{name}/ui/{digest}"
_IMMUTABLE = "public, max-age=31536000, immutable"


# -- UI bundles ----------------------------------------------------------------


class BundleFiles(StaticFiles):
    """Serve one bundle's files, each cached for good: the path they are served under carries the bundle's digest."""

    def __init__(self, bundle: UIBundle) -> None:
        """Serve the bundle's directory.

        Args:
            bundle: The bundle to serve.
        """
        super().__init__(directory=bundle.directory)

    def file_response(
        self, full_path: str | os.PathLike[str], stat_result: os.stat_result, scope: Scope, status_code: int = 200
    ) -> Response:
        """Answer with the file, marked as never changing.

        Args:
            full_path: The file's path on disk.
            stat_result: The file's ``os.stat`` result.
            scope: The request's ASGI scope.
            status_code: The response's status.

        Returns:
            The file response, ``Cache-Control`` set.
        """
        response = super().file_response(full_path, stat_result, scope, status_code)
        response.headers["Cache-Control"] = _IMMUTABLE
        return response


# -- Registries ----------------------------------------------------------------


def _adopt_router(name: str, loaded: Any) -> tuple[str, APIRouter]:
    """Accept a declared router, its name the path segment it answers under.

    Args:
        name: The entry's name.
        loaded: What the entry's import string names.

    Returns:
        The name and the router.

    Raises:
        ValueError: If the name is not a lowercase identifier.
        TypeError: If the entry names something other than an ``APIRouter``.
    """
    if not _NAME_PATTERN.fullmatch(name):
        raise ValueError(f"'{name}' is not a lowercase identifier, which the path segment must be")
    if not isinstance(loaded, APIRouter):
        raise TypeError(f"'{name}' names an instance of {type(loaded).__name__}, not an APIRouter")
    return name, loaded


def _adopt_bundle(name: str, loaded: Any) -> tuple[str, UIBundle]:
    """Accept a declared UI bundle, its name the path segment it is served under.

    Args:
        name: The entry's name.
        loaded: What the entry's import string names.

    Returns:
        The name and the bundle.

    Raises:
        ValueError: If the name is not a lowercase identifier.
        TypeError: If the entry names something other than a `UIBundle`.
    """
    if not _NAME_PATTERN.fullmatch(name):
        raise ValueError(f"'{name}' is not a lowercase identifier, which the path segment must be")
    if not isinstance(loaded, UIBundle):
        raise TypeError(f"'{name}' names an instance of {type(loaded).__name__}, not a UIBundle")
    return name, loaded


EXTENSION_ROUTERS: Registry[APIRouter] = Registry("routes", adopt=_adopt_router)
UI_BUNDLES: Registry[UIBundle | DevServerUI] = Registry("ui", adopt=_adopt_bundle)


# -- Listing -------------------------------------------------------------------

router = APIRouter(tags=["extensions"])


class UIBundleResponse(BaseModel):
    """Where the SPA loads an extension's UI from: its entry module and the stylesheets it needs."""

    entry: str
    css: list[str]

    @classmethod
    def from_bundle(cls, name: str, bundle: UIBundle | DevServerUI) -> UIBundleResponse:
        """Address a bundle's entry and stylesheets where the API serves them, or a dev server's entry.

        Args:
            name: The path segment the bundle is served under.
            bundle: The bundle, or the dev server taking its place.

        Returns:
            The URLs, rooted at the host and carrying the bundle's digest; a
            dev server's entry as it is, with no stylesheet.
        """
        if isinstance(bundle, DevServerUI):
            return cls(entry=bundle.entry, css=[])
        root = UI_PATH.format(name=name, digest=bundle.digest)
        return cls(entry=f"{root}/{bundle.entry}", css=[f"{root}/{path}" for path in bundle.css])


class ExtensionResponse(BaseModel):
    """One installed package that ships a manifest, and whether everything it declares loaded.

    ``status`` is ``failed`` when anything the package declares was skipped:
    its manifest, a section or an entry. The failures themselves are shown
    to super-admins only, on the configuration page. ``ui`` is ``None`` for a
    package that ships no UI, or whose UI failed to load.
    """

    package: str
    key: str | None
    name: str
    description: str
    icon: str
    version: str | None
    status: Literal["ok", "failed"]
    ui: UIBundleResponse | None = None

    @classmethod
    def from_extension(cls, extension: Extension, failures: Iterable[ExtensionFailure]) -> ExtensionResponse:
        """Describe an installed package from its manifest, its UI and what was skipped of it.

        Args:
            extension: The package, as its manifest says what it is.
            failures: Everything skipped so far, every package's.

        Returns:
            The package, ``failed`` when one of the failures is its own.
        """
        failed = any(failure.package == extension.package for failure in failures)
        ui = None
        if extension.key is not None and (bundle := UI_BUNDLES.get(extension.key)) is not None:
            ui = UIBundleResponse.from_bundle(extension.key, bundle)
        return cls(
            package=extension.package,
            key=extension.key,
            name=extension.name,
            description=extension.description,
            icon=extension.icon,
            version=extension.version,
            status="failed" if failed else "ok",
            ui=ui,
        )


@router.get("/extensions")
def list_extensions(user: CurrentUserDep) -> list[ExtensionResponse]:
    """List the installed packages that ship a manifest, the platform's own included.

    Args:
        user: The authenticated caller; any signed-in user may read the listing.

    Returns:
        The packages, sorted by package.
    """
    failures = MANIFESTS.failures
    return [ExtensionResponse.from_extension(extension, failures) for extension in MANIFESTS.extensions()]
