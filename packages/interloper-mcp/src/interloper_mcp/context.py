"""Process globals and per-request toolkit context resolution.

Two authentication paths converge on :func:`get_ctx`:

- **Streamable HTTP**: every request carries a bearer PAT; the SDK's auth
  middleware stores the verified token in a contextvar, read back here via
  ``get_access_token()``.
- **stdio**: single-user by nature; the context is resolved once at startup
  (from a PAT or a direct org scope) and seeded via :func:`set_static_ctx`.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any
from uuid import UUID

from interloper.catalog.base import Catalog
from interloper_db import Store
from interloper_db.toolkit import ToolkitContext
from mcp.server.auth.middleware.auth_context import get_access_token

from interloper_mcp.auth import PatAccessToken

_store: Store | None = None
_catalog_dump: dict[str, Any] | None = None

_static_ctx: ContextVar[ToolkitContext | None] = ContextVar("interloper_mcp_static_ctx", default=None)


def init_context(store: Store, catalog: Catalog) -> None:
    """Set the process-wide store and catalog (once, at startup).

    Args:
        store: The Store every request queries.
        catalog: The catalog; dumped once — definitions don't change at runtime.
    """
    global _store, _catalog_dump  # noqa: PLW0603
    _store = store
    _catalog_dump = catalog.dump()


def set_static_ctx(org_id: UUID) -> None:
    """Seed the static (stdio) context with an organisation scope.

    Args:
        org_id: The organisation all tool calls are scoped to.
    """
    _static_ctx.set(ToolkitContext(store=_require_store(), catalog=_require_catalog(), org_id=org_id))


def get_ctx() -> ToolkitContext:
    """Build the toolkit context for the current tool call.

    Returns:
        The org-scoped context — from the request's verified bearer token
        (HTTP) or the startup-seeded scope (stdio).

    Raises:
        RuntimeError: When no authenticated context exists (a programming
            error: the transport must enforce auth before tools run).
    """
    token = get_access_token()
    if isinstance(token, PatAccessToken):
        return ToolkitContext(store=_require_store(), catalog=_require_catalog(), org_id=UUID(token.org_id))

    ctx = _static_ctx.get()
    if ctx is None:
        raise RuntimeError("No authenticated context for this tool call")
    return ctx


def _require_store() -> Store:
    if _store is None:
        raise RuntimeError("MCP context not initialized. Call init_context() first.")
    return _store


def _require_catalog() -> dict[str, Any]:
    if _catalog_dump is None:
        raise RuntimeError("MCP context not initialized. Call init_context() first.")
    return _catalog_dump
