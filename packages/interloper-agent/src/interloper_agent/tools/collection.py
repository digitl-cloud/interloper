"""Collection tools — the component instances persisted for the organisation.

Generic over component kinds, mirroring the framework's component
architecture: one lister for any kind (sensitive kinds project
identity-only, driven by ``KINDS``), plus the connection and source
operations that are irreducibly kind-specific. Credentials never transit
the model: ``request_connection_setup`` only signals the app to present a
secure setup form, and the browser submits credentials to the API
directly; source setup is conversational because its inputs (accounts,
datasets, asset selections) are not secrets.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from uuid import UUID

import httpx
from google.adk.tools.tool_context import ToolContext
from interloper.component import KINDS
from interloper.connection.base import Connection
from interloper.errors import CatalogKeyError, ComponentDriftError, ConfigError, ConnectionCheckError, HydrationError
from interloper.oauth import is_provider_configured
from interloper.resource.fields import is_fetch_field_provider
from interloper.utils.concurrency import invoke
from pydantic import ValidationError

from interloper_agent.context import get_catalog, get_org_id, get_store, serialize

logger = logging.getLogger(__name__)

#: Upper bound on a live connection check — the agent must never hang on a dead host.
_CHECK_TIMEOUT = 15.0

#: Upper bound on a provider options fetch, and the most options one response carries.
_RESOLVE_TIMEOUT = 30.0
_MAX_OPTIONS = 50


def list_components(kind: str | None = None, tool_context: ToolContext | None = None) -> dict[str, Any]:
    """List the components in the organisation's collection.

    This answers "what do we have?" — what *could* be added (the catalog of
    definitions) is the Catalog specialist's domain. Sensitive kinds
    (connections, configs, resources) always return identity and metadata
    only — never credential or config values.

    Args:
        kind: Component kind to list — e.g. 'source', 'connection',
            'destination'. Omit for per-kind counts only; call again with a
            kind for the entries.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        catalog = get_catalog()

        if kind is None:
            counts: dict[str, int] = {}
            for c in store.list_components(org_id):
                counts[c.kind] = counts.get(c.kind, 0) + 1
            return {
                "status": "success",
                "component_counts": counts,
                "message": "Call again with a kind for the entries.",
            }
        if kind not in KINDS:
            return {"status": "error", "error": f"Unknown kind '{kind}'", "valid_kinds": sorted(KINDS.keys())}

        results = []
        for c in store.list_components(org_id, kinds=[kind]):
            entry: dict[str, Any] = {
                "id": serialize(c.id),
                "key": c.key,
                "name": c.name,
                "type_name": (catalog.get(c.key) or {}).get("name", c.key),
                "created_at": serialize(c.created_at),
            }
            # Fail closed: a sensitive kind's config is (or wraps) credentials.
            if not KINDS[kind].sensitive:
                entry["config"] = serialize(c.config)
            if kind == "source":
                entry["asset_count"] = len(c.children)
            results.append(entry)

        return {"status": "success", "kind": kind, "count": len(results), "components": results}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# -- Connection operations (kind-specific by nature) ------------------------------


def request_connection_setup(
    connection_key: str,
    name: str | None = None,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Present a secure connection setup form to the user in the app.

    Call this to let the user create a connection: the app renders the form
    for the given definition (OAuth sign-in when available, manual
    credential entry otherwise) and the credentials go directly to the API.
    Never ask the user to share credentials in the chat instead.

    The response notes whether the user can sign in with the provider
    (``oauth_available``) or must enter credentials manually; an unknown key
    fails with the list of valid connection keys.

    Args:
        connection_key: Catalog key of the connection definition — usually
            ``<source_key>_connection`` (e.g. 'facebook_ads_connection').
        name: Optional display name to prefill in the form.
    """
    try:
        catalog = get_catalog()
        defn = catalog.get(connection_key)
        if defn is None or defn.get("kind") != "connection":
            valid = sorted(k for k, d in catalog.items() if d.get("kind") == "connection")
            return {
                "status": "error",
                "error": f"Connection definition '{connection_key}' not found in catalog",
                "valid_keys": valid,
            }
        oauth = (defn.get("config_schema") or {}).get("x-oauth")
        return {
            "status": "success",
            "message": (
                "Setup form presented to the user. Ask them to complete it "
                "(and to say so when done), then verify with list_components."
            ),
            "connection_key": connection_key,
            "name": name,
            "oauth": oauth is not None,
            "oauth_available": is_provider_configured(oauth["provider"]) if oauth else False,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def _categorise(exc: Exception) -> tuple[str, str]:
    """Map a ``check()`` failure to an LLM-safe ``(category, message)`` pair.

    Written against the categorisation contract documented on
    ``Connection.check()``. Raw provider errors may carry URLs with tokens,
    and this tool's output enters the model context — only curated messages
    leave here; details are logged server-side.

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


async def check_connection(connection_id: str, tool_context: ToolContext) -> dict[str, Any]:
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
        org_id = get_org_id(tool_context)
        store = get_store()

        component = store.get_component(UUID(connection_id), kind="connection")
        if component.org_id != org_id:
            return {"status": "error", "error": f"Connection '{connection_id}' not found"}
        info = {"id": connection_id, "name": component.name, "key": component.key}

        try:
            conn = store.load(component.id)
        except ComponentDriftError as e:
            return {"status": "success", "connection": info, "ok": False, "live": False,
                    "category": "config", "message": str(e)}
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
            return {"status": "success", "connection": info, "ok": False, "live": False,
                    "category": "config", "message": message}

        if not isinstance(conn, Connection) or not conn.checkable():
            return {"status": "success", "connection": info, "ok": True, "live": False,
                    "message": "This connection type implements no live check; the stored config hydrates."}

        try:
            ok = bool(await asyncio.wait_for(invoke(conn.check), timeout=_CHECK_TIMEOUT))
        except Exception as e:  # noqa: BLE001 — any hook failure is a categorised result, never a raise
            logger.error("Connection check failed for '%s' (%s): %s", component.name, component.key, e)
            category, message = _categorise(e)
            return {"status": "success", "connection": info, "ok": False, "live": True,
                    "category": category, "message": message}
        if not ok:
            return {"status": "success", "connection": info, "ok": False, "live": True,
                    "category": "error", "message": "The connection check failed."}
        return {"status": "success", "connection": info, "ok": True, "live": True}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# -- Source operations (kind-specific by nature) ----------------------------------


async def resolve_source_field_options(
    source_key: str,
    field: str,
    connection_id: str,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """List the live options for a provider-backed source config field.

    Fields marked fetchable in the source definition get their options from
    the provider through a connection in the org's collection — e.g.
    ``facebook_ads.account_id`` lists the ad accounts the connection can
    access. Options are not secret: present them for the user to choose
    from; the chosen option's label makes a good default source name.

    Args:
        source_key: The source definition's catalog key (e.g. 'facebook_ads').
        field: The config field to resolve (must be provider-backed).
        connection_id: UUID of a connection from the org's collection.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        catalog = get_catalog()

        defn = catalog.get(source_key)
        if defn is None or defn.get("kind") != "source":
            return {"status": "error", "error": f"Source '{source_key}' not found in catalog"}
        properties = (defn.get("config_schema") or {}).get("properties", {})
        fetch = (properties.get(field) or {}).get("x-fetch")
        if not fetch:
            fetchable = sorted(k for k, p in properties.items() if p.get("x-fetch"))
            return {
                "status": "error",
                "error": f"Field '{field}' on '{source_key}' is not provider-backed",
                "fetchable_fields": fetchable,
            }
        _, _, method_name = str(fetch.get("provider", "")).partition(".")

        component = store.get_component(UUID(connection_id), kind="connection")
        if component.org_id != org_id:
            return {"status": "error", "error": f"Connection '{connection_id}' not found"}
        try:
            conn = store.load(component.id)
        except (ComponentDriftError, HydrationError) as e:
            logger.error("Connection '%s' (%s) failed to load for resolve: %s", component.name, component.key, e)
            return {"status": "error", "error": "The connection could not be loaded — check it with check_connection."}

        # The @fetch_field_provider marker is the allowlist (same contract as
        # the API's /components/resolve): only opted-in methods are callable.
        fn = getattr(conn, method_name, None)
        if not is_fetch_field_provider(fn):
            return {
                "status": "error",
                "error": f"Connection '{component.key}' does not provide options for {source_key}.{field}",
            }
        assert fn is not None  # narrowed by the guard above

        try:
            items = list(await asyncio.wait_for(invoke(fn), timeout=_RESOLVE_TIMEOUT) or [])
        except Exception as e:  # noqa: BLE001 — a provider failure is a categorised result, never a raise
            logger.error("Resolving %s.%s via '%s' failed: %s", source_key, field, component.name, e)
            category, message = _categorise(e)
            return {"status": "error", "category": category, "error": message}

        label_key, value_key = fetch.get("label_key"), fetch.get("value_key")
        options = [
            {"label": item.get(label_key), "value": item.get(value_key)}
            if isinstance(item, dict)
            else {"label": str(item), "value": item}
            for item in items[:_MAX_OPTIONS]
        ]
        return {
            "status": "success",
            "source_key": source_key,
            "field": field,
            "total": len(items),
            "returned": len(options),
            "options": options,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def create_source(
    source_key: str,
    name: str,
    config: dict[str, Any],
    connection_id: str | None = None,
    asset_keys: list[str] | None = None,
    destination_ids: list[str] | None = None,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Create a source in the organisation's collection.

    Recap the choices — type, name, config, assets, connection, destinations
    — and get the user's explicit confirmation BEFORE calling this.

    Args:
        source_key: The source definition's catalog key (e.g. 'facebook_ads').
        name: Display name — default to the label of the chosen account /
            discriminator option.
        config: Values for the definition's config schema (e.g. account_id).
        connection_id: UUID of the connection to bind — required when the
            definition declares a required connection slot.
        asset_keys: Child asset keys to enable; omit to enable all.
        destination_ids: Destination UUIDs to attach (optional).
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        catalog = get_catalog()

        defn = catalog.get(source_key)
        if defn is None or defn.get("kind") != "source":
            return {"status": "error", "error": f"Source '{source_key}' not found in catalog"}

        # Bind the connection into the definition's matching resource slot.
        slots = (((defn.get("relations") or {}).get("resource") or {}).get("slots")) or {}
        connection = None
        if connection_id is not None:
            connection = store.get_component(UUID(connection_id), kind="connection")
            if connection.org_id != org_id:
                return {"status": "error", "error": f"Connection '{connection_id}' not found"}
        resource_bindings: list[tuple[UUID, str]] = []
        for slot_name, spec in slots.items():
            expected = spec.get("key")
            if connection is not None and (not expected or connection.key == expected):
                resource_bindings.append((connection.id, slot_name))
                connection = None
            elif spec.get("required"):
                return {
                    "status": "error",
                    "error": (
                        f"'{source_key}' requires a '{expected or 'connection'}' in slot '{slot_name}' — "
                        "pick one from the collection or set one up first"
                    ),
                }
        if connection is not None:
            return {
                "status": "error",
                "error": f"Connection '{connection.key}' does not fit any slot of '{source_key}'",
            }

        destination_bindings: list[tuple[UUID, str]] = []
        for dest_id in destination_ids or []:
            dest = store.get_component(UUID(dest_id), kind="destination")
            if dest.org_id != org_id:
                return {"status": "error", "error": f"Destination '{dest_id}' not found"}
            destination_bindings.append((dest.id, ""))

        try:
            row = store.create_component(
                org_id,
                kind="source",
                key=source_key,
                name=name,
                config=config,
                children=asset_keys,
                relations={"resource": resource_bindings, "destination": destination_bindings},
            )
        except (ConfigError, CatalogKeyError) as e:
            return {"status": "error", "error": str(e)}

        # Cross-source requirements are not auto-wired: report them so the
        # agent can point the user at the app for dependency resolution.
        enabled = {a.key for a in row.children}
        unresolved = sorted(
            f"{a['key']}: {', '.join(sorted({**a.get('requires', {}), **a.get('optional_requires', {})}))}"
            for a in defn.get("assets", [])
            if a.get("key") in enabled and (a.get("requires") or a.get("optional_requires"))
        )

        return {
            "status": "success",
            "message": f"Source '{name}' created",
            "source": {
                "id": serialize(row.id),
                "key": row.key,
                "name": row.name,
                "asset_count": len(row.children),
                "connection_bound": bool(resource_bindings),
                "destination_count": len(destination_bindings),
            },
            "unresolved_requirements": unresolved,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
