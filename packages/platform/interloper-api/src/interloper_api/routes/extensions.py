"""Extension routes: the HTTP routes installed packages declare, and the listing of those packages.

A package adds routes by naming an ``APIRouter`` in the ``routes`` section of
its manifest:

```yaml
routes: acme_interloper.routes:router
```

The router answers under ``/api/organisations/{org_id}/ext/<name>``, where
``<name>`` is the entry's name: the extension's key for a single import
string. The viewer role in the organisation the path names is the floor of
every route; a route that writes raises it with ``EditorDep``. The routes
reuse the API's dependencies (``StoreDep``, ``ViewerDep``, ``EditorDep``,
``load_authorized``), and the contract that keeps one organisation's data
from another is the author's: every row carries ``org_id``, and a fetch by id
goes through ``load_authorized``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any, Literal

from fastapi import APIRouter
from interloper.extension import MANIFESTS, Extension, ExtensionFailure
from interloper.registry import Registry
from pydantic import BaseModel

from interloper_api.dependencies import CurrentUserDep

_NAME_PATTERN = re.compile(r"[a-z][a-z0-9_]*")


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
        raise TypeError(f"'{name}' names a {type(loaded).__name__}, not an APIRouter")
    return name, loaded


EXTENSION_ROUTERS: Registry[APIRouter] = Registry("routes", adopt=_adopt_router)

router = APIRouter(tags=["extensions"])


class ExtensionResponse(BaseModel):
    """One installed package that ships a manifest, and whether everything it declares loaded.

    ``status`` is ``failed`` when anything the package declares was skipped:
    its manifest, a section or an entry. The failures themselves are shown
    to super-admins only, on the configuration page.
    """

    package: str
    key: str | None
    name: str
    description: str
    icon: str
    version: str | None
    status: Literal["ok", "failed"]

    @classmethod
    def from_extension(cls, extension: Extension, failures: Iterable[ExtensionFailure]) -> ExtensionResponse:
        """Describe an installed package from its manifest and what was skipped of it.

        Args:
            extension: The package, as its manifest says what it is.
            failures: Everything skipped so far, every package's.

        Returns:
            The package, ``failed`` when one of the failures is its own.
        """
        failed = any(failure.package == extension.package for failure in failures)
        return cls(
            package=extension.package,
            key=extension.key,
            name=extension.name,
            description=extension.description,
            icon=extension.icon,
            version=extension.version,
            status="failed" if failed else "ok",
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
