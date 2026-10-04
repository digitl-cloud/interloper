# Store surface and HTTP conventions

Step 2a of the interloper-db / interloper-api simplification. Step 1 (one error mapping, one role vocabulary, shared org/admin models) shipped in #423.

## Problem

The store grouped tables by area, so facets owning two tables named their second entity in every method (`runs.get_backfill`, `runs.list_backfills`, `organisations.create_invitation`, `auth.get_profile`). Listing APIs had a dozen spellings (`list_all`, `list_for_user`, `list_roots`, `list_members`, ...) and three meanings of `count_*`. The 14 run filters were written four times (route, `list_all`, `count`, `_run_filters`). Over HTTP, two of twenty listings paged, totals rode a header, deletes answered four different bodies, and members/invitations existed twice (session-scoped org routes and super-admin routes) with diverging capabilities.

## Decisions

1. **One facet per entity, uniform verbs.** `store.profiles`, `sessions`, `organisations`, `members`, `invitations`, `tokens`, `conversations`, `components`, `relations`, `runs`, `backfills`, `events`, `executions`, `quotas`, `usage`. Each speaks `get` / `list` / `create` / `update` / `delete` over its own rows, plus the domain verbs a row genuinely has (`complete`, `retry`, `cancel`, `revoke`, `accept`, `reissue`). Facets receive the siblings they need in their constructor (`runs` gets `quotas` and `backfills`; `tokens` gets `members`).
2. **Query objects and pages.** A listing takes its scope (`org_id`, a parent id such as `run_id`) and a pydantic query object extending `PageQuery(limit, offset)`, and returns `Page[T]` (`items`, `total`). The API binds the same query objects to query-string parameters (`Annotated[RunQuery, Query()]`), so filters are declared once. `count` methods are gone: the page carries the total.
3. **`limit=None` is in-process only.** `PageQuery.limit` defaults to 50 and is capped at 500. In-process callers that need a whole set (overview, admin quotas, toolkit lineage, seed scripts) pass `limit=None`; a query string cannot express `None`, so HTTP clients stay bounded. A whole-set read skips the COUNT.
4. **HTTP conventions.**
   - No trailing slashes.
   - Every listing returns `{items, total}` and pages with `limit`/`offset`.
   - Deletes answer 204. Creates answer 201 with the resource. Actions return what they produce (`retry` and `resend` answer 201 with the new run/invitation, `accept-invite` the joined organisation).
   - The frontend walks pages with one `fetchAll` helper for views that need a whole set.
5. **Path rules.**
   - A collection that exists only inside its parent is nested under it (`/runs/{id}/events`, `/runs/{id}/executions`, `/organisations/{id}/members`).
   - A query across parents goes to the top-level resource with filters (`/runs?component_id=`, `/executions?latest=true`, `/relations`). Runs belong to a component, a backfill and a retry stack at once, so they are top-level.
   - Reads take no adjective or verb segments. The one named exception is `GET /components/delete-impact`, a preview over several ids.
   - Operations on a component class are catalog routes: `POST /catalog/{key}/check` and `/catalog/{key}/resolve`.
6. **One set of organisation routes.** Members and invitations live under `/organisations/{org_id}/...`, authorized for the org's own members by role or for a super-admin (`authorize_organisation`). `/admin` keeps only platform-wide views (every organisation, every user, quotas, activity, config). Deleting an organisation stays super-admin only.

## Store surface

| Facet | Methods |
|---|---|
| profiles | `get`, `get_by_google_id`, `list(PageQuery)` (organisations loaded), `upsert`, `update`, `set_super_admin(value=)`, `delete` |
| sessions | `create(user_id, org_id=None)`, `resolve(token)`, `switch_org(token, org_id, user_id)`, `delete_all(user_id)` |
| organisations | `get`, `list(OrganisationQuery(user_id, include_deleted))`, `create`, `update(name=)`, `delete`, `activity(id, PageQuery)` |
| members | `role(org_id, user_id)`, `list(org_id, PageQuery)` (profile loaded), `count_by_org`, `add`, `update`, `delete` |
| invitations | `list(org_id, PageQuery)`, `get(id, org_id=)`, `create`, `reissue`, `delete(id, org_id=)`, `accept(token, user_id)`, `has_pending(email)` |
| tokens | `create(user_id, org_id, name=)`, `resolve`, `list(user_id, TokenQuery(org_id))`, `get`, `revoke` |
| conversations | `create`, `get(id, org_id=, user_id=)`, `list(org_id, user_id, PageQuery)`, `update(id, messages=, title=)`, `delete` |
| components | `get`, `list(org_id, ComponentQuery(kind, q, roots_only=True))`, `create`, `update`, `delete`, `delete_impact`, `load`, `read`, `merge_config`, `stamp_state`, partitioning helpers |
| relations | `list(org_id, RelationQuery(name, src_kind, dst_kind))`, `add`, `delete` |
| runs | `get`, `list(org_id, RunQuery)`, `create`, `complete`, `retry`, `latest_by_target`, `parse_partition` |
| backfills | `get`, `list(org_id, BackfillQuery(status))`, `create`, `cancel`, `run_counts`, `failed_partitions` |
| events | `save`, `get`, `list(org_id, EventQuery, run_id=)`, `error_groups` |
| executions | `list(org_id, ExecutionQuery(latest), run_id=)`, `counts`, `partition_coverage`, `coverage_rows` |
| quotas | `effective_limit`, `overrides`, `all_overrides`, `set_overrides`, `check`, `admit_component`, `admit_run`, `try_reserve_run` |
| usage | `list(UsageQuery)`, `current_period`, `reconcile` (returns `UsageDrift`), `successful_runs_by_org`, `sources_by_org`, `max_assets_per_source_by_org` |

Supporting moves: `Component.run_billable()` owns workload billability; backfill advancement (`BackfillStore._advance`) runs inside `runs.complete`'s transaction; the three copies of the "latest attempt per stack" subquery collapse into one `BackfillStore` helper; store parameters always say `org_id` (the four tables whose column is `organisation_id` keep it until a migration renames it).

## Out of scope

- 2b: splitting `store/components.py` into a package by seam; moving catalog-only questions (partitionings, granularity) to the catalog.
- 2c: the run lifecycle (claim, start, cancel, quota cancel, renewal enqueue) owned by `store.runs`, with a `RunStatus` enum; the scheduler stops writing rows through raw sessions.
- 3: insights (overview, coverage, error groups) as one read-model facet.
- Org-admin UI for role changes and renaming: the endpoints accept org admins, the app does not expose controls yet.
