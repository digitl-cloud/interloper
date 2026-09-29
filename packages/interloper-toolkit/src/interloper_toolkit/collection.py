"""Collection tools: the org's component instances.

Generic over component kinds, mirroring the framework's component
architecture: one lister, one editor and the relation binders for any kind
(sensitive kinds project identity-only and refuse config edits, driven by
``KINDS``), plus the connection operations that are irreducibly
kind-specific. Credentials never transit the model on the normal path:
``request_connection_setup`` hands the user to the app's secure form, and
the browser submits credentials to the API directly.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from uuid import UUID

import httpx
from interloper.component import KINDS
from interloper.connection.base import Connection
from interloper.errors import (
    CatalogKeyError,
    ComponentDriftError,
    ConfigError,
    ConnectionCheckError,
    HydrationError,
    NotFoundError,
)
from interloper.oauth import OAuthAppCredentials
from interloper.utils.concurrency import invoke
from pydantic import ValidationError

from interloper_toolkit.authz import requires_role
from interloper_toolkit.context import ToolkitContext
from interloper_toolkit.models import (
    BindResult,
    ComponentCounts,
    ComponentList,
    ComponentRef,
    ComponentSummary,
    ComponentUpdated,
    ConnectionCheck,
    ConnectionsCreated,
    ConnectionSetup,
    FailedInstance,
    ToolError,
    UnbindResult,
)
from interloper_toolkit.sources import normalized_asset_keys, unresolved_requirements

logger = logging.getLogger(__name__)

#: Upper bound on a live connection check: a tool must never hang on a dead host.
_CHECK_TIMEOUT = 15.0


def list_components(
    ctx: ToolkitContext, kind: str | None = None, q: str | None = None, limit: int = 50, offset: int = 0
) -> ComponentCounts | ComponentList | ToolError:
    """List the components in the organisation's collection, oldest first.

    This answers "what do we have?" — what *could* be added (the catalog of
    definitions) is the Catalog specialist's domain. Sensitive kinds
    (connections, configs, resources) always return identity and metadata
    only — never credential or config values.

    Args:
        kind: Component kind to list — e.g. 'source', 'connection',
            'destination', 'asset'. Omit for per-kind counts only; call again
            with a kind for the entries.
        q: Keep only components whose name or key contains this text,
            case-insensitively.
        limit: Maximum number of components to return (default 50).
        offset: Number of components to skip, for paging past the first page.

    Returns the page of components and the total number matching the filters.
    """
    try:
        if kind is None:
            counts: dict[str, int] = {}
            for c in ctx.store.components.list_all(ctx.org_id, q=q):
                counts[c.kind] = counts.get(c.kind, 0) + 1
            return ComponentCounts(
                component_counts=counts,
                message="Call again with a kind for the entries.",
            )
        if kind not in KINDS:
            return ToolError(error=f"Unknown kind '{kind}'", valid_values=sorted(KINDS.keys()))

        results = []
        total = ctx.store.components.count(ctx.org_id, kinds=[kind], q=q)
        for c in ctx.store.components.list_all(ctx.org_id, kinds=[kind], q=q, limit=limit, offset=offset):
            entry = ComponentSummary(
                id=str(c.id),
                key=c.key,
                name=c.name,
                type_name=(ctx.catalog.get(c.key) or {}).get("name", c.key),
                created_at=c.created_at,
            )
            # Fail closed: a sensitive kind's config is (or wraps) credentials.
            if not KINDS[kind].sensitive:
                entry.config = c.config
            if kind == "source":
                entry.asset_count = len(c.children)
            results.append(entry)

        return ComponentList(kind=kind, count=len(results), total=total, components=results)
    except Exception as e:
        return ToolError(error=str(e))


@requires_role("editor")
def update_component(
    ctx: ToolkitContext,
    component_id: str,
    name: str | None = None,
    config_updates: dict[str, Any] | None = None,
    asset_keys: list[str] | None = None,
) -> ComponentUpdated | ToolError:
    """Edit an existing component in the organisation's collection.

    Works on any kind: rename it, change config values, or change which of a
    source's assets are enabled. Recap exactly what changes (old → new) and
    get the user's explicit confirmation BEFORE calling this.

    Config updates are partial — only the fields passed change, the rest of
    the stored config is kept; pass null to reset a field to its default.
    Connection configs hold credentials and are never edited here: the user
    changes those in the app (renaming a connection is fine). Rebinding
    relations (a source's connection, a job's targets) is not config either:
    use bind_relation and unbind_relation.

    Args:
        component_id: UUID of the component, from list_components.
        name: New display name; omit to keep the current one.
        config_updates: Config fields to change, merged over the stored
            config — e.g. ``{"cron": "0 7 * * *"}`` on a job, or
            ``{"account_id": ...}`` on a source.
        asset_keys: Sources only — the child asset keys to enable, replacing
            the current selection exactly. Omit to leave it unchanged.
    """
    try:
        component = ctx.store.components.get(UUID(component_id), org_id=ctx.org_id)
        if name is None and config_updates is None and not asset_keys:
            return ToolError(error="Nothing to update — pass name, config_updates, or asset_keys")
        if config_updates and KINDS[component.kind].sensitive:
            return ToolError(
                error=(
                    f"The config of a {component.kind} holds credentials and cannot be edited in chat — "
                    "the user changes it in the app, or sets up a new one via the secure form. "
                    "Renaming is allowed."
                )
            )

        config = None
        if config_updates:
            # Sensitive kinds were refused above, so the plain config column
            # is the full stored payload.
            config = dict(component.config or {})
            for field, value in config_updates.items():
                if value is None:
                    config.pop(field, None)
                else:
                    config[field] = value

        children = None
        defn = ctx.catalog.get(component.key)
        if asset_keys:
            if component.kind != "source":
                return ToolError(error=f"Components of kind '{component.kind}' have no assets")
            if defn is None:
                return ToolError(
                    error=f"'{component.key}' is no longer in the catalog — its asset selection cannot change"
                )
            children, error = normalized_asset_keys(defn, component.key, asset_keys)
            if error:
                return error

        try:
            row = ctx.store.components.update(component.id, name=name, config=config, children=children)
        except (ConfigError, CatalogKeyError) as e:
            return ToolError(error=str(e))

        return ComponentUpdated(
            message=f"{component.kind.capitalize()} '{row.name or row.key}' updated",
            component=ComponentRef(id=row.id, kind=row.kind, key=row.key, name=row.name),
            asset_count=len(row.children) if children is not None else None,
            changed_fields=sorted(config_updates) if config_updates else None,
            unresolved_requirements=unresolved_requirements(defn, row) if children is not None and defn else None,
        )
    except Exception as e:
        return ToolError(error=str(e))


# -- Relations (write) ---------------------------------------------------------


@requires_role("editor")
def bind_relation(ctx: ToolkitContext, component_id: str, name: str, dst_id: str) -> BindResult | ToolError:
    """Bind one component to another under a declared relation name.

    Works on any kind: a source's connection, a job's watched assets, a
    destination target, whatever the component's own class declares under
    that name. A ``many`` name accumulates; a single-valued one repoints, so
    rebinding it needs no prior unbind_relation call. Recap what the binding
    changes and get the user's explicit confirmation BEFORE calling this.

    Args:
        component_id: UUID of the component the relation originates from.
        name: Relation name, which the component's class must declare.
        dst_id: UUID of the destination component the relation points at.
            Must belong to the same organisation as the source.
    """
    try:
        src = ctx.store.components.get(UUID(component_id), org_id=ctx.org_id)
        row = ctx.store.relations.add(src.id, name=name, dst_id=UUID(dst_id))
    except (ConfigError, NotFoundError, ValueError) as e:
        return ToolError(error=str(e))
    return BindResult(src_id=str(row.src_id), name=row.name, dst_id=str(row.dst_id), dst_kind=row.dst_kind)


@requires_role("editor")
def unbind_relation(ctx: ToolkitContext, component_id: str, name: str, dst_id: str) -> UnbindResult | ToolError:
    """Detach one component from another under a declared relation name.

    A non-optional relation cannot be emptied, only repointed with
    bind_relation. Recap what the component loses and get the user's
    explicit confirmation BEFORE calling this.

    Args:
        component_id: UUID of the component the relation originates from.
        name: Relation name the edge is filed under.
        dst_id: UUID of the destination the removed edge points at. Removing
            an edge that isn't there is a no-op.
    """
    try:
        src = ctx.store.components.get(UUID(component_id), org_id=ctx.org_id)
        ctx.store.relations.remove(src.id, name=name, dst_id=UUID(dst_id))
    except (ConfigError, NotFoundError, ValueError) as e:
        return ToolError(error=str(e))
    return UnbindResult(src_id=component_id, name=name, dst_id=dst_id)


# -- Connections (kind-specific by nature) ---------------------------------------


def request_connection_setup(
    ctx: ToolkitContext, connection_key: str, name: str | None = None, force_new: bool = False
) -> ConnectionSetup | ToolError:
    """Hand the user to the app's secure connection setup form.

    Call this to let the user create a connection: the app presents the form
    for the given definition (OAuth sign-in when available, manual credential
    entry otherwise) and the credentials go directly to the API. Never ask the
    user to share credentials in the chat instead.

    When the collection already holds connections of this definition, no form
    is presented: ``existing`` lists them so you can ask the user whether to
    reuse one — call again with ``force_new`` only when they want another
    account connected.

    The response notes whether the user can sign in with the provider
    (``oauth_available``) or must enter credentials manually; an unknown key
    fails with the list of valid connection keys.

    Args:
        connection_key: Catalog key of the connection definition — usually
            ``<source_key>_connection`` (e.g. 'facebook_ads_connection').
        name: Optional display name to prefill in the form.
        force_new: Present the form even though fitting connections exist.
    """
    try:
        defn = ctx.catalog.get(connection_key)
        if defn is None or defn.get("kind") != "connection":
            return ToolError(
                error=f"Connection definition '{connection_key}' not found in catalog",
                valid_values=sorted(k for k, d in ctx.catalog.items() if d.get("kind") == "connection"),
            )
        oauth = (defn.get("config_schema") or {}).get("x-oauth")
        setup = ConnectionSetup(
            message=(
                "Setup form presented to the user. Ask them to complete it "
                "(and to say so when done), then verify with list_components."
            ),
            connection_key=connection_key,
            name=name,
            oauth=oauth is not None,
            oauth_available=OAuthAppCredentials.is_configured(oauth["provider"]) if oauth else False,
        )
        if not force_new:
            existing = [
                ComponentRef(id=c.id, kind=c.kind, key=c.key, name=c.name)
                for c in ctx.store.components.list_all(ctx.org_id, kinds=["connection"])
                if c.key == connection_key
            ]
            if existing:
                setup.existing = existing
                setup.message = (
                    "The collection already holds connections of this definition — no form was "
                    "presented. Ask the user whether to reuse one; call again with force_new "
                    "only if they want another account connected."
                )
        return setup
    except Exception as e:
        return ToolError(error=str(e))


@requires_role("editor")
def create_connections(
    ctx: ToolkitContext, connection_key: str, instances: list[dict[str, Any]]
) -> ConnectionsCreated | ToolError:
    """Create connections directly from credential values the user already gave.

    REACT-ONLY. The secure form (request_connection_setup) is the only path
    you ever propose or ask for — never invite the user to paste credentials.
    Use this solely when the user has *already* put the credential values in
    the conversation unprompted: they are in context regardless, so create
    what they asked for instead of dead-ending on a form. Recap and get
    explicit confirmation first, and never repeat a credential value back —
    not in the recap, not in your reply (identity and location only).

    Each config is validated against the connection definition and stored
    encrypted. Instances that fail are reported individually; the rest are
    created. Verify the results with check_connection afterwards.

    Args:
        connection_key: Catalog key of the connection definition
            (e.g. 'amazon_selling_partner_connection').
        instances: One entry per connection, each ``{"name": ...,
            "config": {<field>: <value>, ...}}`` — config carries the
            definition's fields (shared ones like client_id/client_secret
            repeated per instance, plus the per-instance secret).
    """
    try:
        defn = ctx.catalog.get(connection_key)
        if defn is None or defn.get("kind") != "connection":
            return ToolError(
                error=f"Connection definition '{connection_key}' not found in catalog",
                valid_values=sorted(k for k, d in ctx.catalog.items() if d.get("kind") == "connection"),
            )
        cleaned: list[tuple[str, dict[str, Any]]] = [
            (str(i["name"]), i["config"])
            for i in instances
            if isinstance(i, dict) and i.get("name") and isinstance(i.get("config"), dict)
        ]
        if not cleaned:
            return ToolError(error="instances must carry at least one {name, config} entry")

        created, failed = [], []
        for name, config in cleaned:
            try:
                row = ctx.store.components.create(
                    ctx.org_id, kind="connection", key=connection_key, name=name, config=config
                )
            except (ConfigError, CatalogKeyError) as e:
                # str(e) may name required fields but never echoes values.
                failed.append(FailedInstance(name=name, error=str(e)))
                continue
            created.append(ComponentRef(id=row.id, kind=row.kind, key=row.key, name=row.name))

        return ConnectionsCreated(
            message=f"{len(created)} connection(s) created" + (f", {len(failed)} failed" if failed else ""),
            created=created,
            failed=failed,
        )
    except Exception as e:
        return ToolError(error=str(e))


async def check_connection(ctx: ToolkitContext, connection_id: str) -> ConnectionCheck | ToolError:
    """Run a health check on an existing connection.

    Hydrates the stored connection (which validates its config against the
    current catalog and environment) and, when the type supports it, makes a
    lightweight authenticated call to the provider to prove the credentials
    work. Use this to verify a connection after the user sets it up, or when
    data collection fails with authentication-looking errors.

    Args:
        connection_id: UUID of the connection, from list_components.

    Returns ``ok`` plus, on failure, a ``category`` ('config', 'auth',
    'network', 'error') and message. ``live`` is false when the type
    implements no check and only hydration was verified.
    """
    try:
        component = ctx.store.components.get(UUID(connection_id), kind="connection", org_id=ctx.org_id)
        info = ComponentRef(id=component.id, kind=component.kind, key=component.key, name=component.name)
        try:
            conn = ctx.store.components.load(component.id)
        except ComponentDriftError as e:
            return ConnectionCheck(connection=info, ok=False, live=False, category="config", message=str(e))
        except HydrationError as e:
            logger.error("Connection '%s' (%s) failed to hydrate: %s", component.name, component.key, e)
            # Never forward the wrapped message: pydantic errors embed input
            # values, which for connections may be secrets — name fields only.
            if isinstance(e.__cause__, ValidationError):
                fields = ", ".join(
                    ".".join(str(loc) for loc in err["loc"]) or "(root)" for err in e.__cause__.errors()
                )
                message = f"The stored config is no longer valid for this connection type (invalid fields: {fields})."
            else:
                message = "The stored connection could not be reconstructed."
            return ConnectionCheck(connection=info, ok=False, live=False, category="config", message=message)

        if not isinstance(conn, Connection) or not conn.checkable():
            return ConnectionCheck(
                connection=info,
                ok=True,
                live=False,
                message="This connection type implements no live check; the stored config hydrates.",
            )
        try:
            ok = bool(await asyncio.wait_for(invoke(conn.check), timeout=_CHECK_TIMEOUT))
        except Exception as e:
            logger.error("Connection check failed for '%s' (%s): %s", component.name, component.key, e)
            category, message = categorise(e)
            return ConnectionCheck(connection=info, ok=False, live=True, category=category, message=message)
        if not ok:
            return ConnectionCheck(
                connection=info, ok=False, live=True, category="error", message="The connection check failed."
            )
        return ConnectionCheck(connection=info, ok=True, live=True)
    except Exception as e:
        return ToolError(error=str(e))


def categorise(exc: Exception) -> tuple[str, str]:
    """Map a provider call's failure to an LLM-safe ``(category, message)`` pair.

    Written against the categorisation contract documented on
    ``Connection.check()``. Raw provider errors may carry URLs with tokens,
    and a tool's output enters the model context — only curated messages
    leave here; details are logged server-side.

    Args:
        exc: The exception the provider call raised.

    Returns:
        The ``(category, message)`` pair.
    """
    if isinstance(exc, ConnectionCheckError):
        return "error", str(exc)
    if isinstance(exc, httpx.HTTPStatusError):
        if exc.response.status_code in (401, 403):
            return "auth", "The provider rejected the credentials."
        return "error", f"The provider responded with HTTP {exc.response.status_code}."
    if isinstance(exc, (TimeoutError, httpx.TimeoutException)):
        return "network", "The provider did not respond in time."
    if isinstance(exc, httpx.TransportError):
        return "network", "The provider could not be reached."
    return "error", "The connection check failed unexpectedly."
