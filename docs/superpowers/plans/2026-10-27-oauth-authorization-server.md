# OAuth Authorization Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every credential stand for a user, put the organisation in the address on every surface, and add an OAuth 2.1 authorization server to `interloper-api` that MCP connectors and (later) the Slack backend use.

**Architecture:** One `tokens` table and one resolver for sessions, PATs and OAuth access tokens, each surface accepting only its kinds. The organisation is named by the app URL (`/<slug>/…`), the API path (`/api/organisations/{org_id}/…`), the MCP `organisation` argument and the agent conversation. The authorization server is Authlib's framework-agnostic core behind a small Starlette adapter, with its clients, codes and refresh tokens in tables of their own.

**Tech Stack:** Python ≥3.10, FastAPI, SQLModel + Alembic, Authlib, MCP Python SDK 2.x, Nuxt 4 + Pinia, Helm.

**Spec:** `docs/superpowers/specs/2026-10-27-oauth-authorization-server-design.md`

## Global Constraints

- One PR per phase, each a valid Conventional Commit set on a `feat/…` or `refactor/…` branch, rebased on `main`, passing `make check`. No adapter or compatibility layer survives a phase.
- The spec file is committed with the Phase 1 PR (no spec-only PR); this plan travels with it.
- Prerequisite: the Google login fix (state nonce, app-relative redirects only) is merged before Phase 4 starts.
- Tokens are opaque, stored as `interloper_db.crypto.hash_token` (SHA-256) of the raw secret; never JWTs.
- Kinds: `session`, `personal`, `access`. Audiences: cookie accepts `session`; API bearer accepts `personal` and `access` with `resource` = API; MCP accepts `personal` and `access` with `resource` = MCP.
- Access token lifetime 60 minutes; refresh token 30 days, rolling with each rotation; PKCE `S256` required on every authorization.
- Roles are read live per organisation (`store.members.role`); a super-admin acts as `admin` in every live organisation.
- No OAuth scopes. Consent is asked on every authorization; no remembered grants.
- Dependencies: `authlib>=1.9,<2` (api), `mcp>=2.3,<3` (mcp).
- Tests mirror the package layout (`src/<pkg>/<module>.py` → `tests/<pkg>/test_<module>.py`), unit tests only.
- Comment sparingly; Google-style docstrings with full sections; ruff line length 120; `ty` clean.
- Text the user reads (UI copy, docs) uses no long dashes.

## Review Focus

1. An organisation whose name slugifies to a reserved word (`Settings`, `API`, `Admin`) gets a suffixed slug in the migration and is refused on rename; expected: never a slug that shadows an app route.
2. A member removed from an organisation while holding a live access token or PAT: the next call into that organisation is refused at once, on API and MCP alike.
3. A token presented on the wrong surface (a session cookie value as `Bearer`, an MCP access token on the API, a PAT as the cookie) is a 401, never a partial success.
4. A super-admin with no memberships calling an MCP tool without `organisation`: a `ToolError` telling them to name one, not a crash or a silent pick.
5. A rotated refresh token presented again (a leaked token or a client retrying): the whole family is revoked and the error is `invalid_grant`.

---

## Phase 0: Spike (throwaway, no PR)

### Task 0: Prove Authlib's core behind Starlette

**Files:** a scratch branch only; nothing merges.

- [ ] **Step 1:** Subclass `authlib.oauth2.rfc6749.AuthorizationServer` with the four framework methods (`create_oauth2_request`, `create_json_request`, `handle_response`, `send_signal`) for Starlette `Request`/`Response`, register `AuthorizationCodeGrant` with `CodeChallenge(required=True)` and `RefreshTokenGrant`, with in-memory client, code and token storage.
- [ ] **Step 2:** Drive authorize → code → token → refresh against a running dev instance with an existing cookie session standing in for consent.
- [ ] **Step 3:** Record in the PR description of Phase 4 what the adapter needed. If it needs more than the four methods plus storage hooks, stop and bring the MCP SDK `mcp.server.auth` alternative back to Guillaume before Phase 4.

---

## Phase 1: MCP SDK 2.x (PR `chore/mcp-sdk-2`)

### Task 1: Bump the MCP SDK

**Files:**
- Modify: `packages/interloper-mcp/pyproject.toml` (`mcp>=2.3,<3`), `uv.lock`
- Modify: `packages/interloper-mcp/src/interloper_mcp/{server,auth,context,tools,main}.py` as the 2.x API requires
- Test: `packages/interloper-mcp/tests/test_auth.py`, `tests/test_tools.py`

**Interfaces:**
- Produces: unchanged module surface (`create_mcp_server`, `PatVerifier`, `get_ctx`, `register_tools`), on 2.x APIs.

- [ ] **Step 1:** Read the SDK's 2.x migration notes; list every import and call the server uses that moved (`TokenVerifier`, `AccessToken`, `get_access_token`, `FastMCP`, `ToolAnnotations`, `AuthSettings`).
- [ ] **Step 2:** Bump and run `uv run pytest packages/interloper-mcp`. Expected: failures only where APIs moved.
- [ ] **Step 3:** Port to 2.x; keep `stateless_http=True` behaviour and the protected-resource metadata path `/.well-known/oauth-protected-resource/mcp`.
- [ ] **Step 4:** Add `test_anonymous_request_advertises_resource_metadata` in `tests/test_auth.py`: an unauthenticated POST to `/mcp` returns 401 with `WWW-Authenticate` containing `resource_metadata="<external_url>/.well-known/oauth-protected-resource/mcp"`.
- [ ] **Step 5:** `uv run pytest packages/interloper-mcp && uv run ty check && uv run ruff check`. Expected: PASS.
- [ ] **Step 6:** Commit `chore(mcp): move to the MCP SDK 2.x` with the spec and this plan.

---

## Phase 2: PATs stand for the user (PR `refactor/user-tokens`)

### Task 2: The `tokens` table and its resolver

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/models/tokens.py` (`PersonalAccessToken` → `Token`, `TokenKind`, `Audience`)
- Create: `packages/interloper-db/src/interloper_db/migrations/versions/015_user_tokens.py`
- Modify: `packages/interloper-db/src/interloper_db/store/tokens.py`, `models/__init__.py`, `interloper_db/__init__.py`
- Test: `packages/interloper-db/tests/store/test_tokens.py`, `tests/models/test_tokens.py` (if model tests exist for the module, fold there)

**Interfaces:**
- Produces:
  - `class TokenKind(str, Enum): SESSION = "session"; PERSONAL = "personal"; ACCESS = "access"`
  - `class Audience(str, Enum): APP = "app"; API = "api"; MCP = "mcp"`; `Audience.kinds` → the kinds it accepts
  - `Token` columns: `id, user_id, kind, client_id: str | None, name: str | None, resource: str | None, token_prefix, token_hash, expires_at, last_used_at, revoked_at, created_at`
  - `TokenStore.create_personal(user_id: UUID, name: str, expires_at: datetime | None) -> tuple[Token, str]`
  - `TokenStore.resolve(raw: str, audience: Audience) -> tuple[Profile, Token] | None`
  - `TokenStore.list(user_id: UUID, query: TokenQuery) -> Page[Token]` (personal only), `get(token_id) -> Token`, `revoke(token_id) -> Token`

- [ ] **Step 1: Write the failing tests** in `tests/store/test_tokens.py`:
  - `test_personal_token_resolves_on_api_and_mcp`: `resolve(raw, Audience.API)` and `resolve(raw, Audience.MCP)` both return `(profile, token)` with `token.kind == TokenKind.PERSONAL`.
  - `test_personal_token_refused_as_cookie`: `resolve(raw, Audience.APP) is None`.
  - `test_expired_revoked_unknown_refused`: each returns `None`.
  - `test_resolve_stamps_last_used_at`.
  - `test_token_carries_no_organisation`: `not hasattr(Token, "organisation_id")`.
- [ ] **Step 2:** Run `uv run pytest packages/interloper-db/tests/store/test_tokens.py`. Expected: FAIL (import errors).
- [ ] **Step 3:** Implement the model, enums and store. `resolve` no longer looks up a role. Migration 015 renames `personal_access_tokens` to `tokens`, drops `organisation_id`, adds `kind varchar NOT NULL DEFAULT 'personal'`, `client_id varchar NULL`, `resource varchar NULL`; existing hashes stay valid.
- [ ] **Step 4:** Add `test_migration_015_keeps_existing_hashes` beside the other migration tests if the package has them; otherwise assert in the store test that a row inserted with the old column set resolves after `upgrade`.
- [ ] **Step 5:** Run the db tests. Expected: PASS.
- [ ] **Step 6:** Commit `refactor(db)!: a personal access token stands for its user`.

### Task 3: `Tool.bind` takes surface parameters after the toolkit's

**Files:**
- Modify: `packages/interloper-toolkit/src/interloper_toolkit/tools.py`
- Modify: `packages/interloper-agent/src/interloper_agent/toolset.py` (call site keeps `RUN_CONTEXT` leading, see below)
- Test: `packages/interloper-toolkit/tests/test_tools.py`

**Interfaces:**
- Produces: `Tool.bind(context: Callable[..., ToolkitContext | ToolError], *leading: inspect.Parameter, trailing: Sequence[inspect.Parameter] = ()) -> types.FunctionType`. `leading` stay positional ahead of the toolkit's parameters (the agent's `run_context`); `trailing` become keyword-only after them and may carry defaults. `context` receives the leading values then the trailing values as keywords; a `ToolError` it returns is returned by the bound tool without calling the function.

- [ ] **Step 1: Write the failing tests:**
  - `test_bind_trailing_parameter_is_keyword_only_with_default`: the bound signature ends with `organisation: str | None = None` of kind `KEYWORD_ONLY`; the toolkit's required parameters keep their order.
  - `test_bind_returns_context_refusal`: a context returning `ToolError(error="x")` makes the bound tool return that error and never call the function.
  - `test_bind_leading_unchanged`: the agent's `run_context` binding still works (existing tests stay green).
- [ ] **Step 2:** Run `uv run pytest packages/interloper-toolkit/tests/test_tools.py`. Expected: FAIL.
- [ ] **Step 3:** Implement in `Tool.bind`; async and sync paths alike.
- [ ] **Step 4:** Run the toolkit and agent tests. Expected: PASS.
- [ ] **Step 5:** Commit `feat(toolkit): a surface binds keyword-only parameters after a tool's own`.

### Task 4: MCP names its organisation per call

**Files:**
- Modify: `packages/interloper-mcp/src/interloper_mcp/{auth,context,tools,server,main}.py`
- Modify: `packages/interloper-core/src/interloper/settings.py` (`McpSettings.org_id` removed)
- Test: `packages/interloper-mcp/tests/{test_auth,test_context,test_tools}.py` (`test_context.py` new, mirroring `context.py`)

**Interfaces:**
- Consumes: `TokenStore.resolve(raw, Audience.MCP)`; `Tool.bind(..., trailing=[ORGANISATION])`.
- Produces:
  - `UserAccessToken(AccessToken)` with `profile_id: str`, `is_super_admin: bool` (replaces `PatAccessToken`; no org, no role)
  - `ORGANISATION = inspect.Parameter("organisation", KEYWORD_ONLY, default=None, annotation=str | None)`
  - `get_ctx(organisation: str | None = None) -> ToolkitContext | ToolError`
  - `list_organisations() -> OrganisationList` (MCP-only tool): items `{id, slug?, name, role}`; `slug` is absent until Phase 3 adds it

- [ ] **Step 1: Write the failing tests** in `tests/test_context.py`:
  - `test_single_membership_used_when_omitted`: one membership, `get_ctx()` → context with that `org_id` and the member's role.
  - `test_several_memberships_require_naming`: `get_ctx()` → `ToolError` whose `valid_values` are the member's organisation ids.
  - `test_non_member_refused`: `get_ctx(str(other_org))` → `ToolError`.
  - `test_super_admin_any_org_as_admin`: super-admin, no membership, `get_ctx(str(org))` → role `admin`.
  - `test_super_admin_without_memberships_must_name`: `get_ctx()` → `ToolError` telling them to name an organisation (Review Focus 4).
  - `test_member_removed_is_refused_next_call` (Review Focus 2).
  - In `tests/test_auth.py`: `test_session_token_refused` and `test_api_access_token_refused` (Review Focus 3; the latter once Phase 4 issues them, written now against a hand-inserted `access` row with `resource="api"`).
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement: the verifier resolves with `Audience.MCP`; `register_tools` binds with `trailing=[ORGANISATION]`; `list_organisations` registered alongside; `INSTRUCTIONS` gains one sentence on naming the organisation. stdio: `_resolve_stdio_scope` becomes `_resolve_stdio_user(store, token) -> Profile` and requires `INTERLOPER_MCP_TOKEN`; the static context holds the user, and `get_ctx` applies the same organisation rule.
- [ ] **Step 4:** Run mcp tests, `ty`, `ruff`. Expected: PASS.
- [ ] **Step 5:** Commit `feat(mcp)!: a tool call names its organisation`.

### Task 5: The user's token routes and settings page

**Files:**
- Modify: `packages/interloper-api/src/interloper_api/routes/tokens.py` (gates on `CurrentUserDep`; org-admin revoke removed)
- Modify: `packages/interloper-app/app/app/pages/settings/authentication.vue` and its token components/types (no organisation column)
- Modify: docs pages that describe PATs or `INTERLOPER_MCP_ORG_ID` (search `docs/` for both)
- Test: `packages/interloper-api/tests/routes/test_tokens.py`

- [ ] **Step 1: Write the failing tests:** `test_create_token_needs_no_organisation`; `test_list_returns_only_own_tokens`; `test_revoke_other_users_token_404s` (even as org admin).
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement; update the settings page copy ("A token acts as you in every organisation you belong to").
- [ ] **Step 4:** `make check`. Expected: PASS.
- [ ] **Step 5:** Commit `refactor(api)!: personal access tokens belong to their user`.

---

## Phase 3: The organisation in the address (PR `refactor/org-in-address`)

### Task 6: Organisation slugs

**Files:**
- Create: `packages/interloper-db/src/interloper_db/slugs.py` (`RESERVED`, `slugify`, `validate_slug`)
- Create: `packages/interloper-db/src/interloper_db/migrations/versions/016_organisation_slug.py`
- Modify: `models/auth.py` (`Organisation.slug: str`, unique index), `store/organisations.py`
- Test: `packages/interloper-db/tests/test_slugs.py`, `tests/store/test_organisations.py`

**Interfaces:**
- Produces: `RESERVED: frozenset[str]` (every top-level page name under `packages/interloper-app/app/app/pages` that stays top level, plus `api`, `mcp`, `oauth`, `.well-known`, `_nuxt`, `authorize`); `slugify(name: str) -> str`; `validate_slug(slug: str) -> None` raising `ConfigError`; `OrganisationStore.get_by_slug(slug: str) -> Organisation`; `create(name, creator_id)` derives a free slug; `update(org_id, *, name: str | None = None, slug: str | None = None)` raising `ConflictError` when taken.

- [ ] **Step 1: Write the failing tests:** `test_slugify_lowercases_and_hyphenates` (`"Swarovski AG"` → `"swarovski-ag"`); `test_reserved_refused` (`validate_slug("settings")` raises); `test_reserved_name_gets_suffix` (`create("Settings")` → slug `"settings-2"`, Review Focus 1); `test_duplicate_name_dedupes`; `test_rename_to_taken_slug_conflicts`; `test_get_by_slug`.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement; migration 016 adds the column, fills it from names with the same `slugify` + de-duplication + reserved suffixing, then sets `NOT NULL` and the unique index.
- [ ] **Step 4:** Run db tests. Expected: PASS.
- [ ] **Step 5:** Commit `feat(db): organisations carry a slug`.

### Task 7: Sessions are tokens; lifetimes from settings

**Files:**
- Delete: `packages/interloper-db/src/interloper_db/store/sessions.py`, `AuthSession` in `models/auth.py`, `tests/store/test_sessions.py`
- Create: `packages/interloper-db/src/interloper_db/migrations/versions/017_sessions_are_tokens.py`
- Modify: `store/tokens.py`, `store/__init__.py` (the `Store` wiring), `packages/interloper-api/src/interloper_api/routes/auth.py`
- Test: `packages/interloper-db/tests/store/test_tokens.py`, `packages/interloper-api/tests/routes/test_auth.py`

**Interfaces:**
- Produces: `TokenStore.create_session(user_id: UUID) -> str`; `TokenStore.revoke_sessions(user_id: UUID) -> None`; `TokenStore(engine, *, session_expiry: timedelta, access_token_expiry: timedelta)` built by `Store.from_settings` from `auth.session_expiry_days` and (Phase 4) `auth.access_token_expiry_minutes`.

- [ ] **Step 1: Write the failing tests:** `test_session_resolves_only_as_cookie` (`Audience.APP` yes, `API`/`MCP` no, Review Focus 3); `test_session_expiry_from_settings` (a store built with `session_expiry=timedelta(days=2)` stamps `expires_at` two days out); `test_migration_017_keeps_logins` (a pre-migration session row resolves as kind `session`).
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement; migration 017 inserts every live `sessions` row into `tokens` as kind `session` with its hash and expiry, then drops `sessions`. `google_callback` and `logout` use the token store; `SESSION_EXPIRY_DAYS` goes.
- [ ] **Step 4:** Run db and api tests. Expected: PASS.
- [ ] **Step 5:** Commit `refactor(db)!: a login session is a token`.

### Task 8: One caller dependency, one organisation dependency

**Files:**
- Modify: `packages/interloper-api/src/interloper_api/dependencies/{auth,rbac,__init__}.py`
- Modify: `packages/interloper-api/src/interloper_api/app.py` (org router mounting), `routes/{agent,backfills,components,executions,overview,relations,runs,organisations,websocket,auth}.py`
- Test: `packages/interloper-api/tests/dependencies/{test_auth,test_rbac}.py`, the matching `tests/routes/test_*.py`

**Interfaces:**
- Consumes: `TokenStore.resolve(raw, Audience.APP | Audience.API)`.
- Produces:
  - `get_caller(store, session_token: str | None = Cookie(None), authorization: str | None = Header(None)) -> Profile`: cookie resolves with `Audience.APP`, `Bearer` with `Audience.API`; `CurrentUserDep` reads it.
  - `@dataclass(frozen=True) class OrgScope: organisation: Organisation; role: str`
  - `get_org_scope(org_id: UUID (path), user: CurrentUserDep, store) -> OrgScope`: member → their role; super-admin → `admin`; else 404 `Organisation {org_id} not found`.
  - `OrgDep = Annotated[OrgScope, Depends(get_org_scope)]`; `ViewerDep`, `EditorDep`, `AdminDep` become gates over `OrgScope` (403 below the minimum).
  - `load_authorized(fetch, entity_id, scope: OrgScope, *, label, minimum="viewer")`: 404 when `entity.org_id != scope.organisation.id`.
  - Org routers mount under `APIRouter(prefix="/organisations/{org_id}")`; `/auth/me` returns memberships with `slug`; `POST /auth/switch-org`, `OrgIdDep`, `get_current_org`, `authorize_org_member`, `authorize_organisation` are deleted.
  - Websocket path `/api/organisations/{org_id}/ws`.
  - The MCP `list_organisations` items gain `slug` (`packages/interloper-mcp/src/interloper_mcp/tools.py`).

- [ ] **Step 1: Write the failing tests:**
  - `test_cookie_and_bearer_both_authenticate`; `test_bearer_session_value_refused` (401); `test_cookie_pat_value_refused` (401).
  - `test_non_member_path_org_404`; `test_role_from_path_org` (editor in A, viewer in B: a write under B is 403); `test_super_admin_any_org`.
  - `test_detail_of_other_org_404` (a run of org A read under `/organisations/B/runs/{id}`).
  - `test_switch_org_gone` (404).
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement and move every org-scoped route; the agent's chat route takes `org_id` from the path and requires `conversation.org_id` to match.
- [ ] **Step 4:** Run `uv run pytest packages/interloper-api`. Expected: PASS.
- [ ] **Step 5:** Commit `refactor(api)!: the request names its organisation`.

### Task 9: Links into the app carry the slug

**Files:**
- Modify: `packages/interloper-scheduler/src/interloper_scheduler/hooks.py` (`_subject_url(self, org_id: UUID, path: str) -> str | None`)
- Modify: `packages/interloper-toolkit/src/interloper_toolkit/collection.py` (`_setup_url(ctx: ToolkitContext, connection_key: str, name: str | None)`)
- Test: `packages/interloper-scheduler/tests/test_hooks.py`, `packages/interloper-toolkit/tests/test_collection.py`

- [ ] **Step 1: Write the failing tests:** `test_hook_url_carries_slug` (`https://app.example/<slug>/executions/runs/<id>`); `test_setup_url_carries_slug` (`…/<slug>/components/connections?new=…`).
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement via `store.organisations.get(org_id).slug`.
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5:** Commit `fix: links into the app name the organisation`.

### Task 10: The app under `/<slug>/…`

**Files:**
- Move: every organisation page under `packages/interloper-app/app/app/pages/` into `pages/[org]/` (`[kind].vue`, `catalog.vue`, `collection.vue`, `components/`, `executions/`, `graph.vue`, `resources/`, `timeline.vue`, `organization.vue`, `agent/`, and the org landing `index.vue`); `settings/`, `admin/`, `login.vue`, `invite/`, `auth/`, `welcome.vue` stay top level.
- Create: `pages/index.vue` (redirect to the last-used organisation's slug, else `/welcome`), `pages/[org].vue` (resolves the slug, renders not-found when unknown or not a member)
- Modify: `composables/api.ts` (`useOrgApi()` prefixing `/organisations/{id}`), `stores/organisation.ts` (active organisation derived from the route; `switchOrg` → `navigateTo`), `composables/realtime.ts`, `components/nav/Organisation.vue`, `composables/commandPalette.ts`, `components/agent/Panel.vue`, every store calling org-scoped endpoints
- Delete: `composables/orgGate.ts`, `components/organization/Gate.vue`, `useOrgScopedRefetch` and `useOrgSwitch` in `composables/orgScope.ts`, the `orgSwitchTarget` route meta, the `interloper-org` BroadcastChannel
- Modify: `pages/[org]/organization.vue` (slug field, admin only, conflict message)
- Modify: `packages/interloper-db/src/interloper_db/slugs.py` `RESERVED` if a top-level page name changed

- [ ] **Step 1:** Move pages and update every internal link builder (`navigateTo`, `NuxtLink :to`) to include the slug.
- [ ] **Step 2:** Replace org-scoped `apiFetch` calls with `useOrgApi()`; delete the switch machinery listed above.
- [ ] **Step 3:** Run `pnpm run lint && pnpm exec nuxt typecheck` in `packages/interloper-app/app`. Expected: PASS.
- [ ] **Step 4:** Verify with the `verify` skill on a seeded instance (`INTERLOPER_SERVER_PORT=3100 make dev-up`): `/` lands on `/dev-org/…`; two tabs on two organisations stay independent; an unknown slug shows not-found; renaming the slug in organisation settings moves the URL.
- [ ] **Step 5:** Commit `refactor(app)!: an organisation's pages live under its slug`.

---

## Phase 4: The authorization server (PR `feat/oauth-authorization-server`)

### Task 11: Clients, codes and refresh tokens

**Files:**
- Create: `packages/interloper-db/src/interloper_db/models/oauth.py` (`OAuthClient`, `AuthorizationCode`, `RefreshToken`)
- Create: `packages/interloper-db/src/interloper_db/store/oauth.py` (`OAuthStore`), migration `018_oauth_authorization_server.py`
- Test: `packages/interloper-db/tests/store/test_oauth.py`

**Interfaces:**
- Produces:
  - `OAuthClient`: `client_id (str, pk)`, `secret_hash: str | None`, `name`, `redirect_uris: list[str]`, `registered_by: Literal["dynamic","operator"]`, `created_at`
  - `AuthorizationCode`: `code_hash`, `client_id`, `user_id`, `redirect_uri`, `code_challenge`, `resource`, `expires_at` (10 minutes), `used_at`
  - `RefreshToken`: `token_hash`, `family_id: UUID`, `client_id`, `user_id`, `resource`, `expires_at`, `rotated_at`, `revoked_at`
  - `OAuthStore`: `register_client(...) -> tuple[OAuthClient, str | None]`, `get_client(client_id) -> OAuthClient | None`, `issue_code(...) -> str`, `consume_code(raw) -> AuthorizationCode | None`, `issue_refresh(user_id, client_id, resource, family_id=None) -> str`, `rotate_refresh(raw, client_id) -> RefreshToken | None` (reuse of a rotated token revokes the family and returns `None`), `revoke_client_for_user(user_id, client_id)`, `authorised_clients(user_id) -> list[OAuthClient]`
  - `TokenStore.create_access(user_id, client_id, resource) -> str` (kind `access`, 60 minutes)

- [ ] **Step 1: Write the failing tests:** `test_code_single_use`; `test_code_expires`; `test_refresh_rotation_issues_new_and_retires_old`; `test_rotated_refresh_reuse_revokes_family` (Review Focus 5); `test_revoke_client_for_user_kills_refresh_and_access`; `test_access_token_bound_to_resource` (`resource="mcp"` resolves for `Audience.MCP`, not `Audience.API`).
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement; settings `AuthSettings.access_token_expiry_minutes = 60`, `refresh_token_expiry_days = 30`.
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5:** Commit `feat(db): store OAuth clients, codes and refresh tokens`.

### Task 12: The Authlib server and its endpoints

**Files:**
- Create: `packages/interloper-api/src/interloper_api/authorization.py` (the Starlette `AuthorizationServer` subclass, grants, storage hooks over `OAuthStore`/`TokenStore`, RFC 8707 `resource` validation)
- Create: `packages/interloper-api/src/interloper_api/routes/authorization.py` (`/auth/oauth/authorize`, `/auth/oauth/consent`, `/auth/oauth/token`, `/auth/oauth/register`, `/auth/oauth/revoke`) and the root route `/.well-known/oauth-authorization-server` in `app.py`
- Modify: `packages/interloper-api/pyproject.toml` (`authlib>=1.9,<2`)
- Test: `packages/interloper-api/tests/test_authorization.py`, `tests/routes/test_authorization.py`

**Interfaces:**
- Consumes: Task 11.
- Produces:
  - `RESOURCES: dict[str, Audience]` mapping `f"{external_url}/api"` → `Audience.API` and the MCP server's `mcp.external_url` → `Audience.MCP`; any other `resource` is `invalid_target`.
  - `GET /api/auth/oauth/authorize`: validates the request, stores it in a short-lived signed value, redirects to the app's `/authorize?request=<value>` (login first when no session).
  - `GET /api/auth/oauth/consent?request=` → `{client_name, redirect_host, resource}`; `POST /api/auth/oauth/consent` `{request, approve: bool}` → `{redirect_to}` with the code or `error=access_denied`.
  - Metadata: `issuer = server.external_url`, the four endpoints, `code_challenge_methods_supported=["S256"]`, `grant_types_supported=["authorization_code","refresh_token"]`, `token_endpoint_auth_methods_supported=["none","client_secret_post","client_secret_basic"]`.

- [ ] **Step 1: Write the failing tests:** `test_metadata`; `test_register_dynamic_client`; `test_authorize_without_pkce_refused`; `test_authorize_unknown_resource_invalid_target`; `test_full_flow_issues_access_and_refresh` (register → authorize → consent approve → token; the access token resolves for its audience); `test_code_bound_to_client_redirect_and_verifier`; `test_consent_deny_returns_access_denied`; `test_refresh_reuse_invalid_grant` (Review Focus 5); `test_revoke`.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement from the spike's adapter (Task 0 notes).
- [ ] **Step 4:** Run `uv run pytest packages/interloper-api`. Expected: PASS.
- [ ] **Step 5:** Commit `feat(api): an OAuth 2.1 authorization server`.

### Task 13: Consent page and authorised apps

**Files:**
- Create: `packages/interloper-app/app/app/pages/authorize.vue` (top level; copy: "<client> wants to act as you in every organisation you belong to, with your role in each. It will return you to <host>.", Approve / Deny)
- Create: `packages/interloper-api/src/interloper_api/routes/apps.py` → `GET /api/auth/apps`, `DELETE /api/auth/apps/{client_id}` (or fold into `routes/tokens.py` if the builder finds it reads better)
- Modify: `pages/settings/authentication.vue` (an "Authorised apps" table beside tokens, revoke via row menu)
- Test: `packages/interloper-api/tests/routes/test_apps.py`

- [ ] **Step 1: Write the failing tests:** `test_lists_clients_with_live_refresh`; `test_revoke_app_kills_its_tokens`.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement API and pages.
- [ ] **Step 4:** `make check`, then `verify` skill: claude.ai-style flow driven with a script against the dev instance on `:3100` (register, authorize with PKCE, approve, token, call `/mcp` with the token naming `organisation`).
- [ ] **Step 5:** Commit `feat(app): consent to an app and revoke it`.

### Task 14: Operator clients, MCP issuer, chart, docs

**Files:**
- Modify: `packages/interloper-api/src/interloper_api/cli.py` (`interloper api clients create --name --redirect-uri … ` printing `client_id` and the secret once; `clients list`, `clients delete`)
- Modify: `packages/interloper-mcp/src/interloper_mcp/server.py` (`AuthSettings.issuer_url` = `server.external_url`, the interloper issuer)
- Modify: `chart/interloper/templates/httproute.yaml`, `chart/interloper/templates/ingress.yaml` (route `/.well-known/oauth-authorization-server` to the api service), chart values docs
- Modify: `docs/` (connecting claude.ai as a custom connector; PATs; authorised apps)
- Test: `packages/interloper-api/tests/test_cli.py`, `packages/interloper-mcp/tests/test_auth.py`, `helm template` render check

- [ ] **Step 1: Write the failing tests:** `test_clients_create_prints_secret_once`; `test_protected_resource_metadata_names_interloper_issuer`.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement; `helm template chart/interloper` renders the new route in both templates.
- [ ] **Step 4:** `make check`. Expected: PASS.
- [ ] **Step 5:** Commit `feat: operator OAuth clients and the authorization server in the chart`.
- [ ] **Step 6:** Create ITLPR follow-ups: Slack app design; CIMD; Google login on Authlib's client; Ingress routes for `/mcp`.
