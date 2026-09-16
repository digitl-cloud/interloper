# Components list performance

**Date:** 2026-09-16
**Status:** approved
**Scope:** `interloper-db` (component store), `interloper-api` (components router), `interloper-app` (components store and collection pages)

## Problem

`GET /components` takes several seconds on large organisations, and every app page requests the whole collection regardless of what it renders.

The list query itself is cheap: one `SELECT` plus four `selectinload` batches. The cost sits in the per-row response build and in the shape of the result:

1. **Owned assets are returned twice.** Every asset row has an `org_id`, so it is a top-level row, and it is nested again under its source's `children`. Payload and serialisation double.
2. **One query per top-level asset.** `ComponentStore.status()` needs the owning source's key for an owned asset. Nested children receive it from the caller, but top-level asset rows do not, so each one opens a session and runs a primary-key lookup. That is the N+1, and on Cloud SQL through the proxy it is the bulk of the wall time.
3. **Three decrypts per secret row.** `status()`, `discriminator()` and `public_config()` each decode the payload independently.
4. **Pages load everything.** The five kind pages call `fetchAll()` unfiltered because the client-side delete preview (`deleteImpact`) mirrors the server's referrer rule and needs referrers of every kind, and because relation targets are rendered by looking up their record in the store.

## Principle

Follow the core framework's component model rather than the app's current habits:

- **The owner is the unit.** In the core an owned component never has a spec of its own; it travels inside its owner, and the owner is the unit of hydration. The catalog applies the same rule: it is flat over roots, and owned definitions are reached through their owner. The collection endpoint adopts it too.
- **Ownership, not kind.** The store keys owned behaviour on `parent_id`, never on `kind == "asset"`, so a future owner kind gets the same treatment.
- **Logic has an owner.** The server owns relation acceptance and delete rules; the client stops mirroring them.
- **A row describes itself for display.** A relation ref carries its target's identity, so a page of one kind never needs another kind loaded just to label a badge.

## Design

### 1. Collection = roots

`GET /components` returns rows with `parent_id IS NULL`. Each root carries its owned components under `children`, recursively as the response model already allows (eager-loading stays one level deep, which is the only depth that exists today). `?kind=` filters roots by kind, so `?kind=asset` returns standalone assets only. An owned component is reached through its owner in the list, or directly by id on the detail endpoint.

Store: a new `ComponentStore.list_roots(org_id, *, kinds=None)` alongside the existing flat `list_all`, which the toolkit and agent keep using for row-level questions. `status()` branches on `parent_id is not None` instead of `kind == "asset"`; the owner's key is always in hand when walking children, so the per-row lookup only ever runs for a detail request on an owned component.

This is a breaking API change (`feat!`): the unfiltered list no longer repeats owned assets at top level.

### 2. One reading per row

`ComponentStore.read(row, *, parent_key=None) -> ComponentReading` decodes the payload once and derives everything the response needs from it:

- `status`: catalog resolution, then readability for encrypted rows.
- `config`: the decoded payload, `None` when unreadable or undecodable.
- `public_config`: the schema's `x-public` subset of that payload.
- `discriminator`: the class's discriminator field read off that payload.

`status()`, `decode_config()`, `public_config()` and `discriminator()` stay as thin views over `read()` for existing callers and tests. `ComponentResponse.from_row` uses `read()` and picks `config` or `public_config` by `include_config`.

### 3. Server-side delete impact

`GET /components/delete-impact?id=<uuid>&id=<uuid>` returns:

```json
{ "blocking": [{ "id": "...", "kind": "...", "key": "...", "name": "..." }], "detaching": [ ... ] }
```

Both lists use the `{id, kind, key, name}` shape the 409 `used_by` payload already carries. The store's `_blocking_referrers_into` becomes `_referrers_into(session, target_ids, subtree_ids) -> tuple[blocking, detaching]`; the delete guard keeps using the blocking half. A referrer that blocks through any relation is reported only as blocking. Each requested id is authorised with `load_authorized` (viewer), so a bulk preview costs one lookup per id, which is fine for the handful a table selection holds.

Frontend: `componentsStore.deleteImpact(ids)` becomes `async` and calls the endpoint. `DataTable`'s `deleteImpact` prop becomes a promise-returning function; the collection table and the graph source node `await` it. `_relationDetaches` and the client-side mirror are deleted.

### 4. Relation refs describe their target

`RelationRef` gains `dst_key` and `dst_name`. `ComponentRelation` gets a view-only `dst` relationship, and the component load options add one `selectinload` for it (for the row's relations and the children's). `_relations_of` reads the target's key and name off the loaded row. The org-wide `/components/relations` response is unchanged.

Frontend: `RelationRef` in `types/component.ts` mirrors the new fields. Pages label relation targets from the ref (`componentIcon(ref.dst_key)`, `ref.dst_name ?? ref.dst_key`) instead of `byId(ref.dst_id)`.

### 5. Pages fetch what they render

- `pages/components/[kind].vue`, `hooks.vue`, `jobs.vue`, `destinations.vue`: fetch their own kind only. Relations are fetched only where a page reads `upstreams` or `relations`.
- `pages/components/sources.vue`: fetches `source` and `job` plus relations, because the warning rollup (`useAssetWarnings`) reads job targets and asset upstreams.
- `collection.vue`, `graph.vue`, the command palette: unchanged, they are about the whole graph.
- `timeline.vue`, `executions/runs/[run].vue`, `RunsTable.vue`: keep their kind filters; owned assets arrive inside `source`.

A page that reads more than identity from a relation target (status, config) keeps fetching that kind explicitly. The audit of each page happens during implementation and any such case is noted in the plan.

### 6. Store index

The app store holds roots and derives a flat index:

- `index` (computed `Map<id, record>`) walks roots and their `children` recursively.
- `byId` reads the index. `byKind(kind)` returns every indexed record of that kind, owned or not; `graphModel` already filters `parent_id === null` where it wants standalone assets.
- `search` runs over the index for an unfiltered query and over `byKind` otherwise.
- `_upsert(record)`: a record with `parent_id` is placed inside its parent's `children`; a root replaces its root entry (merged as today).
- `_remove(id)`: removes a root and its children, or a child from its parent's `children`.
- Realtime: a change notification refetches the owner (`record.parent_id ?? record.id`), since the owner's detail carries its children.

## Out of scope

- Pagination of the list endpoint. Roots are two orders of magnitude fewer than rows; if a collection grows past that, pagination is a separate change.
- Changing `list_all` semantics for the toolkit and agent.
- Deeper than one level of eager-loaded ownership.

## Testing

- `interloper-db`: `list_roots` excludes owned rows, honours `kinds`, carries children; `read()` decrypts once per row (count calls on the decrypt callable) and its views agree with the existing `status`/`discriminator`/`public_config` tests; `_referrers_into` splits blocking and detaching with blocking winning; the `dst` relationship loads with the row.
- `interloper-api`: the list route calls `list_roots`; `RelationRef` carries `dst_key`/`dst_name`; `delete-impact` authorises every id, 404s an unknown one and returns both lists; `from_row` reads once per row.
- `interloper-app`: `pnpm run lint` and `pnpm exec nuxt typecheck`; manual check of each collection page, the graph and the timeline against the seeded dev instance.
- Benchmark: a synthetic organisation with a few hundred owned assets, timing `list_roots` plus `from_row` before and after, to confirm the per-asset query is gone and note the speedup in the PR.
