# OAuth authorization server, user-level credentials, organisation in the address

## Why

interloper is getting a public Slack app: people install it in their own Slack workspace and talk to the
interloper agent there, with exactly the rights they hold on the platform. The Slack backend is treated as a
third party from day one: it never touches the database or the agent, it calls the API with a token per
linked user, like any other client. That needs interloper to issue tokens to clients, which is also what
MCP connectors (claude.ai, ChatGPT) have been waiting for (ITLPR-131).

Building that on today's auth model would pile up special cases: login sessions and personal access tokens
are separate tables with separate lookups, each pinned to one organisation, and a third and fourth kind of
token would join them. This design first makes two rules true everywhere, then adds the authorization server
on top:

1. **A credential identifies a user, and nothing else.**
2. **The thing being acted on names its organisation**: the app URL, the API path, the MCP tool call, the
   agent conversation.

The Slack app itself is a separate, later design. This one delivers everything it builds on.

## Goals

- One credential store and one resolver for everything that authenticates a request: login sessions,
  personal access tokens, OAuth access tokens. Secret in, profile out.
- The organisation is part of the address on every surface, so one user works across organisations side by
  side (two tabs, two Slack threads, one MCP connection).
- An OAuth 2.1 authorization server in `interloper-api`: authorization code with mandatory PKCE, rotating
  refresh tokens, dynamic client registration (RFC 7591), revocation (RFC 7009), server metadata
  (RFC 8414). The consent leg reuses the existing Google login.
- MCP accepts OAuth tokens and becomes multi-organisation; claude.ai custom connectors connect without a
  pasted PAT.
- Users see and revoke the apps they authorised, next to their PATs.

## Non-goals

- The Slack app (install flow, event handling, account linking UI, Block Kit rendering): next design.
- OAuth scopes. A token acts with the user's live role in each organisation, exactly as the app does
  (decided). Revisit only if a client ever needs less than the user's rights.
- Questions that span organisations within one agent conversation. A conversation belongs to one
  organisation.
- Client ID Metadata Documents. claude.ai supports both CIMD and DCR out of the box
  ([Anthropic connector docs](https://www.claude.com/docs/connectors/building/authentication)); DCR ships
  first because Authlib implements it. CIMD can follow if a client needs it.
- Moving Google login onto Authlib's client. Optional cleanup, later.
- Redirects from an organisation's old slug after a rename.

## Anti-goals

- An adapter or compatibility layer left behind between steps. Each step lands whole.
- A hand-rolled OAuth protocol implementation.
- Any path where a credential, rather than the request, decides the organisation.
- A write landing in an organisation the caller did not name.

## Decisions

### Credentials

- Login sessions, personal access tokens and OAuth access tokens become kinds of one row in one table
  (working name `tokens`, model `Token`, replacing `sessions` and `personal_access_tokens`). Shared columns:
  `user_id`, `kind` (`session`, `personal`, `access`), `client_id` (OAuth client, access tokens only),
  `name` (personal tokens), `resource` (access tokens only, below), `token_prefix`, `token_hash`,
  `expires_at`, `last_used_at`, `revoked_at`, `created_at`. No `organisation_id`.
- **Each surface accepts only its kinds.** The MCP spec requires a server to reject tokens issued for
  another audience, so an access token is bound to the resource the client named (RFC 8707 `resource`
  parameter: the API or the MCP server) and only that surface accepts it. The cookie accepts `session`
  only; API bearer accepts `personal` and `access` for the API; MCP accepts `personal` and `access` for
  MCP. The resolver takes the surface and applies that rule. PATs stay valid on both, as today. Authlib
  does not implement RFC 8707, so the authorization server validates and records `resource` itself.
- Tokens stay **opaque**, stored as their SHA-256 hash (`interloper_db.crypto.hash_token`), as today. No
  JWTs: every check hits the database anyway, and opacity gives instant revocation and live roles.
- One store method resolves a raw secret to `(Profile, Token)`, enforcing expiry and revocation and
  stamping `last_used_at`. The API's auth dependency takes the session cookie or `Authorization: Bearer`
  and calls it; the MCP server's `TokenVerifier` calls it too.
- Authorization codes and refresh tokens are not presented to the API, so they are not in this table: they
  live with the authorization server (below).
- Migration: existing session and PAT rows move into `tokens` with their hashes, so nobody is logged out
  and no PAT stops working. A PAT that was created for one organisation now reaches every organisation its
  owner belongs to, with the owner's role in each (accepted). The sessions' organisation is dropped;
  `profiles.last_organisation_id` already records the last one used.
- Invitation tokens stay as they are: single-use, not a credential.
- An organisation admin can no longer revoke a member's PAT (today's `revoke_token` allows it), since the
  token is no longer the organisation's. Cutting a member off is removing their membership, which the live
  role check applies to every token they hold at once.
- Lifetimes are read from settings by the store. Today the session row's expiry is a hard-coded
  `SESSION_EXPIRY_DAYS` in `interloper_db/store/sessions.py` while `auth.session_expiry_days` only sets the
  cookie's `max_age`; both come from the setting.

### Organisations in the address

- `organisations` gets a `slug`: unique, lowercase, URL-safe, editable by an organisation admin when the
  new value is free. The migration derives one from each name, de-duplicated.
- Slugs share the app's top-level URL space with its own routes, so a fixed list of reserved words is
  refused: every top-level app route (`settings`, `admin`, `login`, `invite`, `welcome`, …) plus `api`,
  `mcp`, `oauth`, `.well-known` and the asset paths. The list lives next to the slug validation.
- **App:** every organisation page lives under `/<slug>/…` (`https://app.interloper.dev/swarovski/graph`).
  `/` sends a user to their last-used organisation. Pages that are not about an organisation (`/settings`,
  `/admin`, `/login`, `/invite`, the consent page) stay at the top level. A slug that no longer exists, or
  that the user is not a member of, shows a not-found page.
- **API:** organisation-scoped routes move under `/api/organisations/{org_id}/…` (the house word; today's
  organisation management routes already live there, so there is one prefix, not two), `org_id` being the
  organisation's id. One dependency reads it, checks live membership and returns the organisation and the
  caller's role; every route gate (`ViewerDep`, `EditorDep`, …) reads the role from it, and
  `authorize_organisation` in `dependencies/rbac.py` folds into it. Detail routes live there too and check
  that the resource belongs to that organisation, which replaces today's membership-scoped detail reads.
  Routes about the user (`/api/auth/me`, tokens, authorised apps) and super-admin routes stay outside it.
  The realtime websocket takes the organisation in its path the same way.
- **Super-admins keep access to every organisation** (decided): the shared membership check lets a
  super-admin into any organisation with the `admin` role, on the API routes, the app pages under
  `/<slug>/…` and the MCP tools alike, as `authorize_organisation` does for the organisation routes today.
  `list_organisations` and the omitted-`organisation` rule count memberships only, so a super-admin
  names any other organisation explicitly.
- **Machines use the id, people see the slug.** The app maps the slug in its URL to the id from the
  memberships it already loads; API clients and MCP pass ids. A rename therefore never breaks an API
  client or a stored conversation.
- **Agent:** a conversation keeps its `org_id`; its routes move under `/api/organisations/{org_id}/agent/…`
  and a conversation must belong to the organisation in the path. The model never chooses the
  organisation.
- **MCP:** every tool gets an `organisation` argument (the organisation's id), added by the MCP layer;
  `get_ctx` checks membership, reads the role and builds the usual `ToolkitContext`. The toolkit table and
  its functions do not change, but `Tool.bind` does: today its surface parameters lead the toolkit's own,
  which cannot carry a default ahead of required parameters, and the context callable cannot refuse.
  `bind` learns to take a surface's parameters as keyword-only after the toolkit's, and to return a
  `ToolError` the context callable produces instead of calling the function. Leaving the argument out
  works when the user belongs to exactly one organisation; with several, the call returns a `ToolError`
  whose `valid_values` lists them. Leaving it out never falls back to "last used". An MCP-only
  `list_organisations` tool returns the user's organisations (id, slug, name, role). The server
  instructions say so.
- Links the platform builds into the app carry the slug: the hook's "View in interloper" URL
  (`interloper_scheduler/hooks.py`) and the toolkit's connection setup URL (`_setup_url` in
  `interloper_toolkit/collection.py`).
- What this removes from the app: switching organisation becomes navigation, so the session's active
  organisation, `POST /api/auth/switch-org`, the refetch-on-switch watcher (`useOrgScopedRefetch`), the
  wrong-organisation interstitial (`OrganizationGate`, `useOrgGate`) and `orgSwitchTarget` lose their
  reason to exist. The builder removes what no longer has a job rather than adapting it.

### Authorization server

- Lives in `interloper-api`, which owns login. Built on **Authlib**'s framework-agnostic
  `authlib.oauth2.rfc6749.AuthorizationServer`: Authlib ships server integrations for Flask and Django only,
  so the API gets a small Starlette adapter (the four methods the framework integrations implement:
  `create_oauth2_request`, `create_json_request`, `handle_response`, `send_signal`) plus the storage hooks
  over the store. Grants: authorization code with PKCE required (`S256`), refresh token with rotation.
  Endpoints: client registration (RFC 7591), revocation (RFC 7009), metadata (RFC 8414).
- **A spike comes first** and is thrown away: the adapter plus one authorization-code-with-PKCE flow end
  to end against the real login. If Authlib's core turns out not to fit FastAPI cleanly, the fallback is
  the MCP SDK's `mcp.server.auth` routes, and that choice comes back to Guillaume before step 4 starts.
- Storage, new tables owned by the authorization server: OAuth clients (registered dynamically or by an
  operator: `client_id`, hashed secret for confidential clients, name, redirect URIs, created_at),
  authorization codes (single use, short-lived, hashed, with the PKCE challenge, redirect URI, client and
  user), refresh tokens (hashed, rotated on use, a reused rotated token revokes its whole family).
  Access tokens are `tokens` rows of kind `access`.
- Lifetimes: access token 1 hour, refresh token 30 days and rolling with each rotation. Settings in
  `AuthSettings` next to `session_expiry_days`.
- **Consent:** the authorize endpoint validates the request, then sends the browser to a consent page in
  the app (top level, outside any organisation). Not logged in: the existing Google login runs first and
  returns there. The page shows the client's name, the host it will redirect to, and that it will act as
  the user in every organisation they belong to, with their role in each. Approve issues the code; deny
  returns the OAuth error. Consent is asked on every authorization (no remembered grants), which keeps
  the model free of a grants table.
- Dynamically registered clients are unverified by nature; the consent page shows the redirect host
  prominently for that reason. Signup gating (`auth.allowed_domains`) applies to the login leg as it does
  today.
- The consent page relies on the Google login, which today has two flaws that must be fixed before it
  does (a separate fix, ahead of this work): its `state` carries only the return path, with no nonce
  bound to the browser (login CSRF), and the callback accepts any `state` starting with `/`, so
  `//evil.host` is an open redirect (`routes/auth.py`).
- Operator-registered confidential clients (the Slack backend will be the first) are created through an
  `interloper` CLI command run by an operator; the secret is shown once.
- **Paths.** The existing `/api/oauth/*` routes are interloper acting as a *client* of data providers
  (connector OAuth). The authorization server mounts under `/api/auth/oauth/…` (authorize, token,
  register, revoke) so the two never share a prefix. Metadata is served at
  `/.well-known/oauth-authorization-server` on the app host. The issuer is `server.external_url`.
- **Authorised apps:** the settings authentication page lists, next to PATs, the OAuth clients holding a
  live refresh token for the user. Revoking one revokes its refresh tokens and access tokens.

### MCP

- Bump the MCP SDK to 2.x first, as ITLPR-131 states (spec revision 2026-07-28; the server already runs
  `stateless_http=True`).
- The protected-resource metadata it already publishes names the interloper issuer as its authorization
  server. The verifier resolves both PATs and OAuth access tokens through the shared resolver.
- The stdio transport keeps `INTERLOPER_MCP_TOKEN` and requires it; `INTERLOPER_MCP_ORG_ID` and its
  unauthenticated fallback disappear with organisation-pinned credentials (the `organisation` argument
  covers it). Local development uses a PAT created in the dev instance.

### Deployment

- The chart routes `/.well-known/oauth-authorization-server` on the app hostnames to the api service, in
  both the HTTPRoute and the Ingress (the frontend would otherwise swallow it). Today only the HTTPRoute
  routes `/mcp` and `/.well-known/oauth-protected-resource`; the Ingress has neither, a gap left as a
  follow-up since prod runs on the HTTPRoute.
- No new role, image or process.
- Self-managed instances are their own issuer with no extra configuration.

## Order of work

Each step is its own PR, leaves no adapter behind and passes the full check suite.

Before any of it: the Google login fix above, as its own `fix:` PR.

0. **Spike** (throwaway): Authlib adapter + authorization code with PKCE end to end.
1. **MCP SDK 2.x.**
2. **PATs stand for the user:** `personal_access_tokens` becomes the `tokens` table without
   `organisation_id` (migration keeps hashes), the token routes become the user's (no organisation), the
   shared resolver with per-surface kinds, `Tool.bind`'s keyword-only surface
   parameters, the MCP `organisation` argument and `list_organisations`, stdio changes. Sessions are
   untouched and still carry the app's active organisation.
3. **The organisation in the address:** slug, reserved words, migration; API routes under
   `/api/organisations/{org_id}`; bearer auth on the API (it needs the organisation in the path); the
   app under `/<slug>/…`; agent routes; websocket; link builders;
   sessions fold into `tokens` as kind `session` and lose their organisation; removal of the switch
   machinery; lifetimes from settings.
4. **The authorization server:** clients, codes, refresh tokens, `resource` binding, Authlib adapter,
   consent page, metadata, chart routes, CLI client registration, authorised apps in settings, MCP
   metadata pointing at the issuer.

## Tests

Mirroring the package layout, unit tests only, at least:

- Resolver: each kind resolves; expired, revoked and unknown secrets do not; `last_used_at` is stamped.
- Migration of sessions and PATs into `tokens` keeps their hashes valid.
- API dependency: cookie and bearer both authenticate; a path organisation the user is not a member of is
  refused; the role comes from that organisation; a detail route refuses a resource of another
  organisation.
- Slugs: derivation and de-duplication, reserved words refused, rename refused when taken.
- MCP: `organisation` omitted with one membership works, with several returns the list, a non-member
  organisation is refused, the role gate uses that organisation's role.
- Authorization server: registration; authorize without PKCE refused; code single use and bound to client,
  redirect URI and verifier; refresh rotation; reuse of a rotated refresh token revokes the family;
  revocation; metadata contents; consent deny.
- Frontend: lint and typecheck; routes resolve under a slug.

## Left to the builder

- Table, model and column names beyond those fixed above; the refresh-token family mechanism.
- The slug format limits and the exact reserved list.
- How the app resolves a slug and where it keeps the memberships it maps from.
- The CLI command's name and shape for registering confidential clients.
- Splitting a step into several commits.

## Open follow-ups (Jira ITLPR)

- The Slack app design, building on this.
- Client ID Metadata Documents if a client needs them.
- Google login on Authlib's client.
