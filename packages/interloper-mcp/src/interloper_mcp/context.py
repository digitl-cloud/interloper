"""Process globals and per-call toolkit context resolution.

A token names only its user; every tool call names the organisation it acts
in, and :func:`get_ctx` checks the user's live membership there and reads
their role. Two authentication paths supply the user:

- **Streamable HTTP**: every request carries a bearer token; the SDK's auth
  middleware stores the verified token in a contextvar, read back here via
  ``get_access_token()``.
- **stdio**: single-user by nature; the user is resolved once at startup
  from ``INTERLOPER_MCP_TOKEN`` and seeded via :func:`set_static_user`.

A super-admin acts as ``admin`` in any live organisation, as on the API's
organisation routes; leaving the organisation out still counts memberships
only, so a super-admin names any other organisation explicitly.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any
from uuid import UUID

from interloper.catalog import Catalog
from interloper_db import NotFoundError, Organisation, OrganisationQuery, Profile, Store
from interloper_toolkit import ToolError, ToolkitContext
from mcp.server.auth.middleware.auth_context import get_access_token

from interloper_mcp.auth import UserAccessToken

NAME_THE_ORGANISATION = (
    "Name the organisation to act in: pass its id as `organisation` (list_organisations lists yours)."
)

_store: Store | None = None
_catalog_dump: dict[str, Any] | None = None

_static_user: ContextVar[Profile | None] = ContextVar("interloper_mcp_static_user", default=None)


def init_context(store: Store, catalog: Catalog) -> None:
    """Set the process-wide store and catalog (once, at startup).

    Args:
        store: The Store every request queries.
        catalog: The catalog; dumped once — definitions don't change at runtime.
    """
    # One context per process: the MCP server serves a single deployment.
    global _store, _catalog_dump
    _store = store
    _catalog_dump = catalog.dump()


def set_static_user(profile: Profile) -> None:
    """Seed the static (stdio) context with the user every tool call runs as.

    Args:
        profile: The user the stdio token resolved to.
    """
    _static_user.set(profile)


def get_ctx(organisation: str | None = None) -> ToolkitContext | ToolError:
    """Build the toolkit context for the current tool call, or refuse it.

    Args:
        organisation: The id of the organisation the call acts in; ``None``
            when the caller left it out, which is accepted only when they
            belong to exactly one organisation.

    Returns:
        The context scoped to that organisation with the caller's role in it,
        or the refusal: an organisation the caller cannot act in, or an
        omitted one while they belong to none or several, the error listing
        their organisation ids.
    """
    if organisation is None:
        memberships = organisations()
        if len(memberships) != 1:
            return ToolError(error=NAME_THE_ORGANISATION, valid_values=[str(org.id) for org, _ in memberships])
        ((org, role),) = memberships
        return ToolkitContext(store=_require_store(), catalog=_require_catalog(), org_id=org.id, role=role)

    try:
        org_id = UUID(organisation)
    except ValueError:
        return _not_found(organisation)
    role = _role(org_id)
    if role is None:
        return _not_found(organisation)
    return ToolkitContext(store=_require_store(), catalog=_require_catalog(), org_id=org_id, role=role)


def organisations() -> list[tuple[Organisation, str]]:
    """The organisations the caller is a member of, with the role tool calls hold in each.

    Returns:
        Each live organisation the caller belongs to, oldest first, with
        their role in it (``admin`` for a super-admin).
    """
    user = _caller()
    store = _require_store()
    listed = store.organisations.list(OrganisationQuery(user_id=user.id, limit=None)).items
    return [(org, role) for org in listed if (role := store.members.role_of(org.id, user)) is not None]


def _not_found(organisation: str) -> ToolError:
    """The refusal for an organisation the caller cannot act in.

    Missing and not a member read the same, so ids don't act as an existence
    oracle.

    Args:
        organisation: The organisation as the caller named it.

    Returns:
        The error, listing the caller's own organisation ids.
    """
    return ToolError(
        error=f"Organisation {organisation} not found", valid_values=[str(org.id) for org, _ in organisations()]
    )


def _role(org_id: UUID) -> str | None:
    """The role the caller acts with in an organisation, read live.

    Args:
        org_id: The organisation.

    Returns:
        ``admin`` for a super-admin in a live organisation, else the caller's
        membership role; ``None`` when they cannot act in it.
    """
    store = _require_store()
    try:
        store.organisations.get(org_id)
    except NotFoundError:
        return None
    return store.members.role_of(org_id, _caller())


def _caller() -> Profile:
    """The user the current tool call runs as.

    Returns:
        Their profile: from the request's verified bearer token (HTTP), else
        the startup-seeded user (stdio).

    Raises:
        RuntimeError: When no authenticated user exists (a programming
            error: the transport must enforce auth before tools run).
    """
    token = get_access_token()
    if isinstance(token, UserAccessToken):
        return token.profile

    profile = _static_user.get()
    if profile is None:
        raise RuntimeError("No authenticated user for this tool call")
    return profile


def _require_store() -> Store:
    """The initialized store, or a loud failure.

    Returns:
        The process-wide Store.

    Raises:
        RuntimeError: If the context was never initialized.
    """
    if _store is None:
        raise RuntimeError("MCP context not initialized. Call init_context() first.")
    return _store


def _require_catalog() -> dict[str, Any]:
    """The initialized catalog dump, or a loud failure.

    Returns:
        The serialized catalog every tool reads.

    Raises:
        RuntimeError: If the context was never initialized.
    """
    if _catalog_dump is None:
        raise RuntimeError("MCP context not initialized. Call init_context() first.")
    return _catalog_dump
