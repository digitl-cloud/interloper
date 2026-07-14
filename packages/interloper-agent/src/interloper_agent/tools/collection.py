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
    force_new: bool = False,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Present a secure connection setup form to the user in the app.

    Call this to let the user create a connection: the app renders the form
    for the given definition (OAuth sign-in when available, manual
    credential entry otherwise) and the credentials go directly to the API.
    Never ask the user to share credentials in the chat instead.

    When the collection already holds connections of this definition, the
    form is NOT presented: the response lists them so you can ask the user
    whether to reuse one — call again with ``force_new`` only when they
    want another account connected.

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
        catalog = get_catalog()
        defn = catalog.get(connection_key)
        if defn is None or defn.get("kind") != "connection":
            valid = sorted(k for k, d in catalog.items() if d.get("kind") == "connection")
            return {
                "status": "error",
                "error": f"Connection definition '{connection_key}' not found in catalog",
                "valid_keys": valid,
            }

        if not force_new:
            org_id = get_org_id(tool_context)
            existing = [
                {"id": serialize(c.id), "name": c.name}
                for c in get_store().list_components(org_id, kinds=["connection"])
                if c.key == connection_key
            ]
            if existing:
                return {
                    "status": "exists",
                    "message": (
                        "The collection already holds connections of this definition — no form was "
                        "presented. Ask the user whether to reuse one; call again with force_new "
                        "only if they want another account connected."
                    ),
                    "existing": existing,
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
    connection_id: str,
    field: str | None = None,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """List the live options for a source's provider-backed config field.

    Fields marked fetchable in the source definition get their options from
    the provider through a connection in the org's collection — e.g. the ad
    accounts the connection can access. Options are not secret: present them
    for the user to choose from; the chosen option's label makes a good
    default source name.

    Args:
        source_key: The source definition's catalog key (e.g. 'facebook_ads').
        connection_id: UUID of a connection from the org's collection.
        field: The config field to resolve. Omit it — never guess field
            names: most definitions have exactly one fetchable field and it
            is picked automatically (the response names it).
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        catalog = get_catalog()

        defn = catalog.get(source_key)
        if defn is None or defn.get("kind") != "source":
            return {"status": "error", "error": f"Source '{source_key}' not found in catalog"}
        properties = (defn.get("config_schema") or {}).get("properties", {})
        fetchable = sorted(k for k, p in properties.items() if p.get("x-fetch"))
        if field is None:
            if len(fetchable) != 1:
                return {
                    "status": "error",
                    "error": f"'{source_key}' has {len(fetchable)} fetchable fields — pass one explicitly",
                    "fetchable_fields": fetchable,
                }
            field = fetchable[0]
        fetch = (properties.get(field) or {}).get("x-fetch")
        if not fetch:
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


def _normalized_asset_keys(
    defn: dict[str, Any], source_key: str, asset_keys: list[str] | None
) -> tuple[list[str] | None, dict[str, Any] | None]:
    """Normalize an asset selection: empty means default-all, unknown keys error.

    Models pass ``[]`` meaning "default" — and a zero-asset source is useless.

    Returns:
        ``(asset_keys, None)`` or ``(None, error_response)``.
    """
    if not asset_keys:
        return None, None
    valid = {a.get("key") for a in defn.get("assets", [])}
    unknown = sorted(set(asset_keys) - valid)
    if unknown:
        return None, {
            "status": "error",
            "error": f"Unknown asset keys for '{source_key}': {', '.join(unknown)}",
            "valid_asset_keys": sorted(valid),
        }
    return asset_keys, None


def _source_relations(
    store: Any,
    org_id: UUID,
    defn: dict[str, Any],
    source_key: str,
    connection_id: str | None,
    destination_ids: list[str] | None,
) -> tuple[dict[str, list[tuple[UUID, str]]] | None, dict[str, Any] | None]:
    """Bind the connection into the definition's resource slot, plus destinations.

    Returns:
        ``(relations, None)`` or ``(None, error_response)``.
    """
    slots = (((defn.get("relations") or {}).get("resource") or {}).get("slots")) or {}
    connection = None
    if connection_id is not None:
        connection = store.get_component(UUID(connection_id), kind="connection")
        if connection.org_id != org_id:
            return None, {"status": "error", "error": f"Connection '{connection_id}' not found"}
    resource_bindings: list[tuple[UUID, str]] = []
    for slot_name, spec in slots.items():
        expected = spec.get("key")
        if connection is not None and (not expected or connection.key == expected):
            resource_bindings.append((connection.id, slot_name))
            connection = None
        elif spec.get("required"):
            return None, {
                "status": "error",
                "error": (
                    f"'{source_key}' requires a '{expected or 'connection'}' in slot '{slot_name}' — "
                    "pick one from the collection or set one up first"
                ),
            }
    if connection is not None:
        return None, {
            "status": "error",
            "error": f"Connection '{connection.key}' does not fit any slot of '{source_key}'",
        }

    destination_bindings: list[tuple[UUID, str]] = []
    for dest_id in destination_ids or []:
        dest = store.get_component(UUID(dest_id), kind="destination")
        if dest.org_id != org_id:
            return None, {"status": "error", "error": f"Destination '{dest_id}' not found"}
        destination_bindings.append((dest.id, ""))

    return {"resource": resource_bindings, "destination": destination_bindings}, None


def _unresolved_requirements(defn: dict[str, Any], row: Any) -> list[str]:
    """Cross-source requirements of the enabled assets — reported, not auto-wired.

    Returns:
        One ``"asset: params"`` line per affected asset.
    """
    enabled = {a.key for a in row.children}
    return sorted(
        f"{a['key']}: {', '.join(sorted({**a.get('requires', {}), **a.get('optional_requires', {})}))}"
        for a in defn.get("assets", [])
        if a.get("key") in enabled and (a.get("requires") or a.get("optional_requires"))
    )


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

        asset_keys, error = _normalized_asset_keys(defn, source_key, asset_keys)
        if error:
            return error
        relations, error = _source_relations(store, org_id, defn, source_key, connection_id, destination_ids)
        if error:
            return error
        assert relations is not None

        try:
            row = store.create_component(
                org_id,
                kind="source",
                key=source_key,
                name=name,
                config=config,
                children=asset_keys,
                relations=relations,
            )
        except (ConfigError, CatalogKeyError) as e:
            return {"status": "error", "error": str(e)}

        return {
            "status": "success",
            "message": f"Source '{name}' created",
            "source": {
                "id": serialize(row.id),
                "key": row.key,
                "name": row.name,
                "asset_count": len(row.children),
                "connection_bound": bool(relations["resource"]),
                "destination_count": len(relations["destination"]),
            },
            "unresolved_requirements": _unresolved_requirements(defn, row),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def create_sources(
    source_key: str,
    instances: list[dict[str, str]],
    connection_id: str | None = None,
    asset_keys: list[str] | None = None,
    shared_config: dict[str, Any] | None = None,
    field: str | None = None,
    destination_ids: list[str] | None = None,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Create several sources of one definition — one per account/profile value.

    Use this when the user sets up multiple accounts of the same source type
    at once: every instance shares the connection, asset selection, and any
    shared config; its ``value`` fills the definition's account field and its
    ``name`` becomes the source's display name (use the option labels from
    the account selection).

    Recap the choices and get the user's explicit confirmation BEFORE calling
    this. Instances that fail (e.g. an account that already has a source)
    are reported individually; the others are still created.

    Args:
        source_key: The source definition's catalog key (e.g. 'facebook_ads').
        instances: One entry per source, each ``{"name": ..., "value": ...}``.
        connection_id: UUID of the connection every source binds.
        asset_keys: Child asset keys to enable on every source; omit for all.
        shared_config: Config values common to all instances (e.g. dataset).
        field: The config field receiving each value. Omit it — never guess
            field names: the definition's fetchable field is picked
            automatically.
        destination_ids: Destination UUIDs to attach to every source.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        catalog = get_catalog()

        defn = catalog.get(source_key)
        if defn is None or defn.get("kind") != "source":
            return {"status": "error", "error": f"Source '{source_key}' not found in catalog"}
        cleaned = [
            {"name": str(i.get("name") or i.get("value")), "value": str(i.get("value"))}
            for i in instances
            if isinstance(i, dict) and i.get("value") is not None
        ]
        if not cleaned:
            return {"status": "error", "error": "instances must carry at least one {name, value} entry"}

        properties = (defn.get("config_schema") or {}).get("properties", {})
        if field is None:
            fetchable = sorted(k for k, p in properties.items() if p.get("x-fetch"))
            discriminators = sorted(k for k, p in properties.items() if p.get("x-discriminator"))
            candidates = fetchable or discriminators
            if len(candidates) != 1:
                return {
                    "status": "error",
                    "error": f"'{source_key}' has no single account field — pass one explicitly",
                    "candidate_fields": candidates,
                }
            field = candidates[0]
        elif field not in properties:
            return {"status": "error", "error": f"Unknown config field '{field}' for '{source_key}'"}

        asset_keys, error = _normalized_asset_keys(defn, source_key, asset_keys)
        if error:
            return error
        relations, error = _source_relations(store, org_id, defn, source_key, connection_id, destination_ids)
        if error:
            return error
        assert relations is not None

        created, failed = [], []
        unresolved: list[str] = []
        for instance in cleaned:
            try:
                row = store.create_component(
                    org_id,
                    kind="source",
                    key=source_key,
                    name=instance["name"],
                    config={**(shared_config or {}), field: instance["value"]},
                    children=asset_keys,
                    relations=relations,
                )
            except (ConfigError, CatalogKeyError) as e:
                failed.append({"name": instance["name"], "value": instance["value"], "error": str(e)})
                continue
            created.append({"id": serialize(row.id), "name": row.name, "value": instance["value"]})
            unresolved = _unresolved_requirements(defn, row)

        return {
            "status": "success" if created else "error",
            "message": f"{len(created)} source(s) created" + (f", {len(failed)} failed" if failed else ""),
            "field": field,
            "created": created,
            "failed": failed,
            "unresolved_requirements": unresolved,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# -- Job operations (kind-specific by nature) --------------------------------------


def create_job(
    name: str,
    cron: str,
    target_source_ids: list[str],
    partitioned: bool = False,
    backfill_days: int | None = None,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Create a cron job that runs the given sources on a schedule.

    Use this after creating sources to put them on a cadence — one job can
    target several sources (e.g. every account of a source type). Recap the
    name, the schedule in words, and the targets, and get the user's
    explicit confirmation BEFORE calling this.

    Args:
        name: Display name for the job (e.g. 'Facebook Ads daily').
        cron: Standard cron expression (e.g. '0 6 * * *' for daily at 06:00 UTC).
        target_source_ids: UUIDs of the sources the job materializes.
        partitioned: Whether runs are date-partitioned.
        backfill_days: For partitioned jobs, how many days each run covers.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()

        targets: list[tuple[UUID, str]] = []
        for source_id in target_source_ids:
            source = store.get_component(UUID(source_id), kind="source")
            if source.org_id != org_id:
                return {"status": "error", "error": f"Source '{source_id}' not found"}
            targets.append((source.id, ""))
        if not targets:
            return {"status": "error", "error": "target_source_ids must name at least one source"}

        try:
            row = store.create_component(
                org_id,
                kind="job",
                key="cron_job",
                name=name,
                config={
                    "cron": cron,
                    "enabled": True,
                    "tags": [],
                    "partitioned": partitioned,
                    "backfill_days": backfill_days if partitioned else None,
                },
                relations={"target": targets},
            )
        except (ConfigError, CatalogKeyError) as e:
            return {"status": "error", "error": str(e)}

        return {
            "status": "success",
            "message": f"Job '{name}' created",
            "job": {
                "id": serialize(row.id),
                "name": row.name,
                "cron": cron,
                "enabled": True,
                "target_count": len(targets),
            },
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
