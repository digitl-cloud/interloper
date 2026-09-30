# The agent on pydantic-ai

Date: 2026-09-30. Status: approved design, shipped with the implementation PR.

Scope: the in-app agent's runtime (`interloper-agent`), its API (`interloper-api` agent routes), its
persistence (`interloper-db`) and its UI (`interloper-app`). The tool surface itself is the toolkit,
settled by the two preceding specs (investigation tools, MCP write parity); nothing here changes a
tool's behaviour.

---

## 1. Diagnosis

The agent runs on Google's ADK as a router plus five specialists and a duplicated "consultant"
agent. Three things about that are debt rather than design:

- **Sessions are in memory.** `InMemorySessionService` loses every conversation on an API restart
  and cannot be shared by a second replica.
- **The stream is the ADK's internal event shape.** The app parses `author`, `content.parts` with
  `text|thought|functionCall|functionResponse` and `actions.transferToAgent` by hand (307 lines in
  `agentChat.ts`), rebuilds a "trail" of thoughts and steps, and keys three interaction cards on the
  response shapes of three tools.
- **Confirmation is prompt-enforced.** `request_confirmation` returns a "STOP" message and the
  model is asked to wait; nothing mechanical prevents a write without it.

pydantic-ai gives each of these a primitive: message history that serialises
(`ModelMessagesTypeAdapter`), a UI adapter that speaks the Vercel AI SDK stream Nuxt UI's chat
components are built for (`VercelAIAdapter`, approvals as `approval-requested` tool states), and
`requires_approval` / `CallDeferred` for human-in-the-loop tools.

## 2. Decisions

1. **One agent, all toolsets, one instruction.** No router, no specialists, no consultant.
2. **Vercel AI SDK protocol end to end.** `VercelAIAdapter.dispatch_request` on the server, `useChat`
   from `@ai-sdk/vue` in the app, Nuxt UI's `UChatMessages` / `UChatReasoning` / `UChatTool`
   rendering the parts.
3. **Conversations in Postgres**, renamed from "sessions" (which is the auth session here).
4. **Approval on creates and cancels**: `create_source`, `create_sources`, `create_job`,
   `create_connections`, `cancel_backfill`. Every other write runs when called. MCP is unaffected
   (its clients decide from the annotations).
5. **Markdown via `@comark/nuxt`**, replacing `@nuxtjs/mdc`, which only the chat used.

## 3. The agent (`interloper-agent`)

- `agent.py`: `build_agent(model: str) -> Agent[ToolkitContext, str]`. Deps type is `ToolkitContext`
  as it stands. Instructions from `prompts.py` plus a dynamic instruction adding the current UTC
  time. Model settings enable thought summaries for Google models and adaptive thinking for
  Anthropic, keyed on the provider prefix. `UsageLimits(request_limit=…)` bounds a turn.
- `toolset.py`: one `FunctionToolset` from the toolkit functions. `bind(fn)` adapts
  `f(ctx: ToolkitContext, …)` to a pydantic-ai tool: same name and docstring, the signature minus
  `ctx`, forwarding `run_context.deps`. The registration list is a sequence of
  `(function, requires_approval)` pairs, reads first.
- Interaction tools: `request_user_selection` raises `CallDeferred`; `request_connection_setup`
  returns the toolkit result when connections exist, else raises `CallDeferred`; the app supplies
  the outputs. `request_confirmation` is deleted.
- `context.py` (globals, `set_store`, `get_org_id`, `toolkit_ctx`) is deleted; the ADK wrappers
  under `tools/` are deleted.
- Dependencies: `google-adk[extensions]` out, `pydantic-ai-slim[google,anthropic,openai]` in.
  `agent.model` takes `provider:model` (`google-gla:gemini-2.5-flash` default); LiteLLM's
  `provider/model` form is gone.

## 4. Conversations (`interloper-db`) and the API

- Table `conversations`: `id`, `org_id`, `user_id`, `title`, `messages` (JSONB, the
  `ModelMessage` list), `created_at`, `updated_at`; index `(org_id, user_id, updated_at)`.
  Migration 007.
- Facet `store.conversations`: `create`, `get(id, *, org_id, user_id)` (foreign reads as missing),
  `list(org_id, user_id)`, `delete`, `save(id, messages, title=None)` (whole-history replace).
- Routes, replacing `/agent/sessions`:

  | Route | Role |
  |---|---|
  | `POST /agent/conversations` | editor |
  | `GET /agent/conversations` | viewer |
  | `GET /agent/conversations/{id}` (history as `UIMessage[]` via `dump_messages`) | viewer |
  | `DELETE /agent/conversations/{id}` | editor |
  | `POST /agent/conversations/{id}/chat` | editor |

- The chat route is one `VercelAIAdapter.dispatch_request(request, agent=…, sdk_version=6,
  deps=ToolkitContext(...), message_history=stored, conversation_id=…, usage_limits=…,
  on_complete=save)`. Approvals and deferred-tool outputs arrive in the client's next request.
- The agent is built once at API startup and exposed as a dependency.

## 5. The app

- `ai` + `@ai-sdk/vue`; `useChat` with `DefaultChatTransport` on the chat route and
  `sendAutomaticallyWhen: lastAssistantMessageIsCompleteWithToolCalls`.
- Parts: reasoning → `UChatReasoning`; tools → `UChatTool` (Approve/Deny actions on
  `approval-requested`); `tool-request_user_selection` → `SelectCard`; `tool-request_connection_setup`
  → `ConnectCard`, both resolving with `addToolOutput`; text → comark `Markdown`.
- Deleted: `agentChat.ts`, the ADK event types, `ConfirmCard.vue`, `Work.vue`, `Thinking.vue`.
- `stores/agent.ts` becomes the conversations store.

## 6. Tests and rollout

Deterministic agent and route tests over pydantic-ai's `TestModel`/`FunctionModel`; store facet
and migration tests; app lint and typecheck. One PR, commits: db conversations; agent on
pydantic-ai; API routes over the Vercel stream; app over the AI SDK; drop google-adk and document
the model format. Live check (real Gemini, dev instance) before merge: a read, an approved
`create_job`, a selection card, the connect card, a restored conversation after reload.

Out of scope: multi-agent delegation, AG-UI, cost tracking, lifting the `kubernetes` pin.
