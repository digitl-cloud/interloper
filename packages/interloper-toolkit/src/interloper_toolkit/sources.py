"""Source tools: creating sources conversationally.

Source setup is conversational because its inputs (accounts, datasets,
asset selections) are not secrets: the account a source reads comes from
the provider through an existing connection, and a batch of accounts
becomes a batch of sources sharing everything but that one value.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from uuid import UUID

from interloper.errors import CatalogKeyError, ComponentDriftError, ConfigError, HydrationError
from interloper.resource.fields import is_fetch_field_provider
from interloper.utils.concurrency import invoke

from interloper_toolkit.authz import requires_role
from interloper_toolkit.context import ToolkitContext
from interloper_toolkit.models import (
    ComponentRef,
    CreatedInstance,
    FailedInstance,
    FieldOption,
    FieldOptions,
    SourceCreated,
    SourcesCreated,
    ToolError,
)

logger = logging.getLogger(__name__)

#: Upper bound on a provider options fetch, and the most options one response carries.
_RESOLVE_TIMEOUT = 30.0
_MAX_OPTIONS = 100


async def resolve_source_field_options(
    ctx: ToolkitContext, source_key: str, connection_id: str, field: str | None = None
) -> FieldOptions | ToolError:
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
    # Imported here: collection imports this module for the asset helpers.
    from interloper_toolkit.collection import categorise

    try:
        defn = ctx.catalog.get(source_key)
        if defn is None or defn.get("kind") != "source":
            return ToolError(error=f"Source '{source_key}' not found in catalog")
        properties = (defn.get("config_schema") or {}).get("properties", {})
        fetchable = sorted(k for k, p in properties.items() if p.get("x-fetch"))
        if field is None:
            if len(fetchable) != 1:
                return ToolError(
                    error=f"'{source_key}' has {len(fetchable)} fetchable fields — pass one explicitly",
                    valid_values=fetchable,
                )
            field = fetchable[0]
        fetch = (properties.get(field) or {}).get("x-fetch")
        if not fetch:
            return ToolError(error=f"Field '{field}' on '{source_key}' is not provider-backed", valid_values=fetchable)
        _, _, method_name = str(fetch.get("provider", "")).partition(".")

        component = ctx.store.components.get(UUID(connection_id), kind="connection", org_id=ctx.org_id)
        try:
            conn = ctx.store.components.load(component.id)
        except (ComponentDriftError, HydrationError) as e:
            logger.error("Connection '%s' (%s) failed to load for resolve: %s", component.name, component.key, e)
            return ToolError(error="The connection could not be loaded — check it with check_connection.")
        # The @fetch_field_provider marker is the allowlist (same contract as
        # the API's /components/resolve): only opted-in methods are callable.
        fn = getattr(conn, method_name, None)
        if not is_fetch_field_provider(fn):
            return ToolError(error=f"Connection '{component.key}' does not provide options for {source_key}.{field}")
        assert fn is not None  # narrowed by the guard above
        try:
            items = list(await asyncio.wait_for(invoke(fn), timeout=_RESOLVE_TIMEOUT) or [])
        except Exception as e:
            logger.error("Resolving %s.%s via '%s' failed: %s", source_key, field, component.name, e)
            category, message = categorise(e)
            return ToolError(error=message, category=category)

        label_key, value_key = fetch.get("label_key"), fetch.get("value_key")
        options = [
            FieldOption(label=item.get(label_key), value=item.get(value_key))
            if isinstance(item, dict)
            else FieldOption(label=str(item), value=item)
            for item in items[:_MAX_OPTIONS]
        ]
        return FieldOptions(
            source_key=source_key, field=field, total=len(items), returned=len(options), options=options
        )
    except Exception as e:
        return ToolError(error=str(e))


@requires_role("editor")
def create_source(
    ctx: ToolkitContext,
    source_key: str,
    name: str,
    config: dict[str, Any],
    connection_id: str | None = None,
    asset_keys: list[str] | None = None,
    destination_ids: list[str] | None = None,
) -> SourceCreated | ToolError:
    """Create a source in the organisation's collection.

    Recap the choices — type, name, config, assets, connection, destinations
    — and get the user's explicit confirmation BEFORE calling this.

    Args:
        source_key: The source definition's catalog key (e.g. 'facebook_ads').
        name: Display name — default to the label of the chosen account /
            discriminator option.
        config: Values for the definition's config schema (e.g. account_id).
        connection_id: UUID of the connection to bind; required when the
            definition declares a required connection relation.
        asset_keys: Child asset keys to enable; omit to enable all.
        destination_ids: Destination UUIDs to attach (optional).
    """
    try:
        defn = ctx.catalog.get(source_key)
        if defn is None or defn.get("kind") != "source":
            return ToolError(error=f"Source '{source_key}' not found in catalog")

        asset_keys, error = normalized_asset_keys(defn, source_key, asset_keys)
        if error:
            return error
        relations, error = source_relations(ctx, defn, source_key, connection_id, destination_ids)
        if error:
            return error
        assert relations is not None

        try:
            row = ctx.store.components.create(
                ctx.org_id,
                kind="source",
                key=source_key,
                name=name,
                config=config,
                children=asset_keys,
                relations=relations,
            )
        except (ConfigError, CatalogKeyError) as e:
            return ToolError(error=str(e))

        return SourceCreated(
            message=f"Source '{name}' created",
            source=ComponentRef(id=row.id, kind=row.kind, key=row.key, name=row.name),
            asset_count=len(row.children),
            connection_bound=connection_id is not None,
            destination_count=len(relations["destinations"]),
            unresolved_requirements=unresolved_requirements(defn, row),
        )
    except Exception as e:
        return ToolError(error=str(e))


@requires_role("editor")
def create_sources(
    ctx: ToolkitContext,
    source_key: str,
    instances: list[dict[str, str]],
    connection_id: str | None = None,
    asset_keys: list[str] | None = None,
    shared_config: dict[str, Any] | None = None,
    field: str | None = None,
    destination_ids: list[str] | None = None,
) -> SourcesCreated | ToolError:
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
        defn = ctx.catalog.get(source_key)
        if defn is None or defn.get("kind") != "source":
            return ToolError(error=f"Source '{source_key}' not found in catalog")
        cleaned = [
            {"name": str(i.get("name") or i.get("value")), "value": str(i.get("value"))}
            for i in instances
            if isinstance(i, dict) and i.get("value") is not None
        ]
        if not cleaned:
            return ToolError(error="instances must carry at least one {name, value} entry")

        properties = (defn.get("config_schema") or {}).get("properties", {})
        if field is None:
            fetchable = sorted(k for k, p in properties.items() if p.get("x-fetch"))
            discriminators = sorted(k for k, p in properties.items() if p.get("x-discriminator"))
            candidates = fetchable or discriminators
            if len(candidates) != 1:
                return ToolError(
                    error=f"'{source_key}' has no single account field — pass one explicitly", valid_values=candidates
                )
            field = candidates[0]
        elif field not in properties:
            return ToolError(
                error=f"Unknown config field '{field}' for '{source_key}'", valid_values=sorted(properties)
            )

        asset_keys, error = normalized_asset_keys(defn, source_key, asset_keys)
        if error:
            return error
        relations, error = source_relations(ctx, defn, source_key, connection_id, destination_ids)
        if error:
            return error
        assert relations is not None

        created, failed = [], []
        unresolved: list[str] = []
        for instance in cleaned:
            try:
                row = ctx.store.components.create(
                    ctx.org_id,
                    kind="source",
                    key=source_key,
                    name=instance["name"],
                    config={**(shared_config or {}), field: instance["value"]},
                    children=asset_keys,
                    relations=relations,
                )
            except (ConfigError, CatalogKeyError) as e:
                failed.append(FailedInstance(name=instance["name"], value=instance["value"], error=str(e)))
                continue
            created.append(CreatedInstance(id=row.id, name=row.name, value=instance["value"]))
            unresolved = unresolved_requirements(defn, row)

        return SourcesCreated(
            message=f"{len(created)} source(s) created" + (f", {len(failed)} failed" if failed else ""),
            field=field,
            created=created,
            failed=failed,
            unresolved_requirements=unresolved,
        )
    except Exception as e:
        return ToolError(error=str(e))


# -- Helpers -------------------------------------------------------------------


def normalized_asset_keys(
    defn: dict[str, Any], source_key: str, asset_keys: list[str] | None
) -> tuple[list[str] | None, ToolError | None]:
    """Normalize an asset selection: empty means default-all, unknown keys error.

    Models pass ``[]`` meaning "default" — and a zero-asset source is useless.

    Args:
        defn: The source's catalog definition, carrying its assets.
        source_key: The source definition's catalog key, named in the error.
        asset_keys: The requested child asset keys, or nothing.

    Returns:
        ``(asset_keys, None)`` or ``(None, error)``.
    """
    if not asset_keys:
        return None, None
    valid = {a.get("key") for a in defn.get("assets", [])}
    unknown = sorted(set(asset_keys) - valid)
    if unknown:
        return None, ToolError(
            error=f"Unknown asset keys for '{source_key}': {', '.join(unknown)}",
            valid_values=sorted(k for k in valid if k),
        )
    return asset_keys, None


def source_relations(
    ctx: ToolkitContext,
    defn: dict[str, Any],
    source_key: str,
    connection_id: str | None,
    destination_ids: list[str] | None,
) -> tuple[dict[str, list[UUID]] | None, ToolError | None]:
    """Bind the connection into the definition's named relation, plus destinations.

    Finds the relations the definition declares whose ``kind`` includes
    ``"connection"``. A given connection binds under whichever one it fits
    (an empty ``key`` accepts any connection, otherwise the connection's key
    must appear in the relation's key list); every non-optional connection
    relation left unbound afterwards is an error naming the relation and the
    key it expects. Destinations bind under the fixed ``"destinations"`` name
    once each one is confirmed to belong to the organisation.

    Args:
        ctx: The toolkit context, for looking up the connection and
            destination rows in the organisation.
        defn: The source's catalog definition, carrying its declared
            ``relations`` (name to relation dict: ``kind``, ``key``, ``many``,
            ``optional``, ``on_delete``, ``name``).
        source_key: The source definition's catalog key, named in error
            messages.
        connection_id: UUID of the connection to bind, or ``None`` to leave
            every connection relation unbound.
        destination_ids: UUIDs of the destinations to attach, or ``None``.

    Returns:
        ``(relations, None)``, ``relations`` mapping relation name to the
        UUIDs bound under it (ready for ``ComponentStore.create``'s
        ``relations`` argument), or ``(None, error)``.
    """
    relations_defn = defn.get("relations") or {}
    connection_relations = {
        name: relation
        for name, relation in relations_defn.items()
        if "connection" in (relation["kind"] if isinstance(relation["kind"], list) else [relation["kind"]])
    }

    bindings: dict[str, list[UUID]] = {}
    if connection_id is not None:
        connection = ctx.store.components.get(UUID(connection_id), kind="connection", org_id=ctx.org_id)
        name = next(
            (
                relation_name
                for relation_name, relation in connection_relations.items()
                if not relation.get("key")
                or connection.key in ([relation["key"]] if isinstance(relation["key"], str) else relation["key"])
            ),
            None,
        )
        if name is None:
            return None, ToolError(error=f"Connection '{connection.key}' does not fit any relation of '{source_key}'")
        bindings[name] = [connection.id]

    for name, relation in connection_relations.items():
        if not relation.get("optional") and name not in bindings:
            expected = relation.get("key") or "connection"
            return None, ToolError(
                error=(
                    f"'{source_key}' requires a '{expected}' as '{name}'; "
                    "pick one from the collection or set one up first"
                )
            )

    bindings["destinations"] = [
        ctx.store.components.get(UUID(dest_id), kind="destination", org_id=ctx.org_id).id
        for dest_id in destination_ids or []
    ]
    return bindings, None


def unresolved_requirements(defn: dict[str, Any], row: Any) -> list[str]:
    """Cross-source requirements of the enabled assets — reported, not auto-wired.

    Args:
        defn: The source's catalog definition, carrying its assets.
        row: The created or updated source row, with its children loaded.

    Returns:
        One ``"asset: params"`` line per affected asset.
    """
    enabled = {a.key for a in row.children}
    return sorted(
        f"{a['key']}: {', '.join(sorted({**a.get('requires', {}), **a.get('optional_requires', {})}))}"
        for a in defn.get("assets", [])
        if a.get("key") in enabled and (a.get("requires") or a.get("optional_requires"))
    )
