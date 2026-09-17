# Retry Phase 3: Surfaces Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A stack reads as one piece of work in the API and the app, so a workload that eventually succeeded looks like a success.

**Architecture:** The runs listing becomes stack-native: one row per stack carrying its latest
attempt, selected with `DISTINCT ON (root_run_id)` and re-ordered in an outer query so the listing
order is unchanged. A `root_run_id` filter lists one stack's attempts. The app's runs table shows an
`n/N` chip and expands to the attempts; the run detail page navigates the stack. The retry policy
itself needs no UI work: it is a config field on a component, so `SchemaForm` already renders it.

**Tech Stack:** FastAPI, SQLModel, pydantic v2, Nuxt 4, Vue 3, @nuxt/ui, TypeScript, pytest, vitest.

Spec: `docs/superpowers/specs/2026-09-17-retry-design.md`, section 9.
Prerequisite: `docs/superpowers/plans/2026-09-17-retry-phase-2-platform.md` must be complete.

## Global Constraints

- Python: line length 120, ruff-formatted, `ty`-checked, Google-style docstrings with every
  applicable section.
- Frontend: run from `packages/interloper-app/app/`; `pnpm run lint` and `pnpm exec nuxt typecheck`.
  Follow that directory's own `AGENTS.md`.
- Tests mirror the package layout one to one.
- Two known `UTable` traps: an inline `:grouping` object literal combined with `v-model:expanded`
  causes an auto-reset loop, and `resolveComponent` returns a bare string inside header and cell
  render functions (import from `#components` in shared helpers instead).
- **Do not commit without Guillaume asking.** The commit step records the intended message.

---

### Task 1: The API exposes the stack

**Files:**
- Modify: `packages/interloper-api/src/interloper_api/routes/runs.py:40-90` (`RunResponse`), `:211-260` (`list_runs`)
- Modify: `packages/interloper-db/src/interloper_db/store/runs.py:140-243` (`list_all`, `count`, `_run_filters`)
- Test: `packages/interloper-db/tests/store/test_runs.py`
- Test: `packages/interloper-api/tests/routes/test_runs.py`

**Interfaces:**
- Consumes: `Run.root_run_id`, `Run.scheduled_for` (phase 2 Task 1).
- Produces: `RunStore.list_all(..., root_run_id: UUID | None = None, stacks: bool = True)` returning
  one row per stack when `stacks` is true and every attempt of one stack when `root_run_id` is given;
  `RunStore.count` matching it; `RunResponse` carrying `root_run_id`, `scheduled_for` and `attempts`.
  Task 2 consumes the response fields.

- [ ] **Step 1: Write the failing store test**

Add to `packages/interloper-db/tests/store/test_runs.py`:

```python
def test_listing_returns_one_row_per_stack(store, org, job_component):
    store.components.merge_config(job_component.id, {"retry": {"max_attempts": 3, "delay": 0}})
    first = store.runs.create(org.id, component_id=job_component.id)
    store.runs.complete(first.id, success=False)

    runs = store.runs.list_all(org.id, component_id=job_component.id)

    assert len(runs) == 1
    assert runs[0].attempt == 2
    assert store.runs.count(org.id, component_id=job_component.id) == 1


def test_a_stack_can_be_listed_in_full(store, org, job_component):
    store.components.merge_config(job_component.id, {"retry": {"max_attempts": 3, "delay": 0}})
    first = store.runs.create(org.id, component_id=job_component.id)
    store.runs.complete(first.id, success=False)

    attempts = store.runs.list_all(org.id, root_run_id=first.root_run_id)

    assert [run.attempt for run in attempts] == [2, 1]


def test_a_status_filter_reads_the_stacks_verdict(store, org, job_component):
    store.components.merge_config(job_component.id, {"retry": {"max_attempts": 3, "delay": 0}})
    first = store.runs.create(org.id, component_id=job_component.id)
    store.runs.complete(first.id, success=False)

    assert store.runs.list_all(org.id, status="failed") == []
    assert len(store.runs.list_all(org.id, status="queued")) == 1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-db/tests/store/test_runs.py -v -k "per_stack or listed_in_full or stacks_verdict"`
Expected: FAIL. Both attempts come back as separate rows.

- [ ] **Step 3: Make the listing stack-native**

In `packages/interloper-db/src/interloper_db/store/runs.py`, add `root_run_id: UUID | None = None`
and `stacks: bool = True` to `list_all` and `count`, pass `root_run_id` through `_run_filters` as a
plain equality filter, and select the latest attempt of each stack when `stacks` is true and no
`root_run_id` was given:

```python
            if stacks and root_run_id is None:
                latest = (
                    select(col(Run.id))
                    .where(*filters)
                    .distinct(col(Run.root_run_id))
                    .order_by(col(Run.root_run_id), col(Run.attempt).desc())
                    .subquery()
                )
                statement = (
                    select(Run)
                    .where(col(Run.id).in_(select(latest.c.id)))
                    .order_by(col(Run.created_at).desc())
                    .offset(offset)
                    .limit(limit)
                    .options(*RUN_LOAD_OPTIONS)
                )
```

`count` applies the same narrowing before counting. `_run_filters` is shared by both, so build the
filter list once and pass it into the subquery, as the existing code already does.

Document the behaviour on both methods: a stack is one piece of work, so a listing shows its latest
attempt and every filter, `status` included, applies to that attempt. Passing `root_run_id` lists one
stack's attempts, newest first.

Order the attempts by `attempt` descending when `root_run_id` is given, so a stack reads newest-first
like the listing does.

- [ ] **Step 4: Run the store tests to verify they pass**

Run: `uv run pytest packages/interloper-db/tests/store/test_runs.py -v`
Expected: PASS.

- [ ] **Step 5: Write the failing API test**

Add to `packages/interloper-api/tests/routes/test_runs.py`, following the module's existing client
and auth fixtures:

```python
def test_a_run_response_carries_its_stack(client, org, job_component, store):
    store.components.merge_config(job_component.id, {"retry": {"max_attempts": 3, "delay": 0}})
    first = store.runs.create(org.id, component_id=job_component.id)
    store.runs.complete(first.id, success=False)

    body = client.get("/runs/").json()

    assert len(body["items"]) == 1
    assert body["items"][0]["root_run_id"] == str(first.root_run_id)
    assert body["items"][0]["attempt"] == 2
    assert body["items"][0]["attempts"] == 2
    assert body["items"][0]["scheduled_for"] is not None


def test_a_stack_is_listed_by_its_root(client, org, job_component, store):
    store.components.merge_config(job_component.id, {"retry": {"max_attempts": 3, "delay": 0}})
    first = store.runs.create(org.id, component_id=job_component.id)
    store.runs.complete(first.id, success=False)

    body = client.get(f"/runs/?root_run_id={first.root_run_id}").json()

    assert [item["attempt"] for item in body["items"]] == [2, 1]
```

Match the listing response's actual envelope key rather than assuming `items`.

- [ ] **Step 6: Expose the fields and the filter**

In `packages/interloper-api/src/interloper_api/routes/runs.py`, add to `RunResponse`:

```python
    root_run_id: UUID
    scheduled_for: str | None = None
    attempts: int = 1
```

`from_run` sets `root_run_id=run.root_run_id`, `scheduled_for=str(run.scheduled_for) if
run.scheduled_for else None` and `attempts=run.attempt`, since the listing returns the latest attempt
and its number is the stack's total. Extend the class docstring with one sentence: a response is one
attempt, and in a stack-native listing it is the stack's latest, whose `attempt` is therefore the
count of attempts made.

Add `root_run_id: UUID | None = None` to `list_runs`' query parameters and pass it to `list_all` and
`count`.

- [ ] **Step 7: Run the API tests to verify they pass**

Run: `uv run pytest packages/interloper-api/tests/routes/test_runs.py -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add packages/interloper-db packages/interloper-api
git commit -m "feat(api)!: list runs by stack

By Digitl"
```

---

### Task 2: The app reads a stack as one run

**Files:**
- Modify: `packages/interloper-app/app/app/types/run.ts`
- Modify: `packages/interloper-app/app/app/stores/runs.ts`
- Modify: `packages/interloper-app/app/app/components/executions/RunsTable.vue`
- Modify: `packages/interloper-app/app/app/pages/executions/runs/[run].vue`

**Interfaces:**
- Consumes: the `root_run_id`, `scheduled_for` and `attempts` fields from Task 1.
- Produces: a runs table whose rows are stacks, and a run detail page that navigates a stack's
  attempts.

- [ ] **Step 1: Extend the type**

In `packages/interloper-app/app/app/types/run.ts`, add to the `Run` interface:

```ts
    /** The stack this attempt belongs to; its own id for a first attempt. */
    root_run_id: string
    /** Earliest instant the queue may claim this run; set on a retry's backoff. */
    scheduled_for: string | null
    /** Attempts made in this stack, as of this row. */
    attempts: number
```

- [ ] **Step 2: Fetch a stack's attempts**

In `packages/interloper-app/app/app/stores/runs.ts`, add an action that fetches one stack, following
the store's existing fetch and state conventions:

```ts
async function fetchStack(rootRunId: string) {
    return await api<Paginated<Run>>('/runs/', { query: { root_run_id: rootRunId } })
}
```

Match the store's real helper names and response envelope rather than the sketch above.

- [ ] **Step 3: Show the stack in the table**

In `RunsTable.vue`, render an `n/N`-style chip in the status cell when `row.attempts > 1`, reading as
"attempt 2 of 2". Reuse the identifier-chip component the tables already share rather than adding a
bespoke badge, and keep the status badge showing the latest attempt's status, which is what the API
now returns.

If the row is made expandable to its attempts, note the two `UTable` traps: bind `grouping` to a
`computed`, never an inline object literal, when `v-model:expanded` is also bound, and import
components from `#components` in any shared cell-render helper instead of calling `resolveComponent`.

- [ ] **Step 4: Navigate the stack on the detail page**

In `pages/executions/runs/[run].vue`, when the loaded run's `attempts > 1` or its `root_run_id`
differs from its `id`, fetch the stack with the store action from step 2 and render the attempts as
links, marking the one being viewed. Show `scheduled_for` on a queued attempt, so a run waiting out
its backoff reads as scheduled rather than stuck.

- [ ] **Step 5: Run the frontend checks**

Run from `packages/interloper-app/app/`: `pnpm run lint && pnpm exec nuxt typecheck`
Expected: clean.

- [ ] **Step 6: Verify it end to end**

Stand up a seeded instance on a non-default port so it does not collide with the developer's own:

Run: `INTERLOPER_SERVER_PORT=3100 make dev-up`

Give a `demo` job a retry policy through the component form, run it against a target that fails, and
confirm: the runs table shows one row that flips to a second attempt, the detail page lists both
attempts, and the timeline shows the retried operation as a success with two attempts.

- [ ] **Step 7: Commit**

```bash
git add packages/interloper-app/app
git commit -m "feat(app): read a run stack as one piece of work

By Digitl"
```

---

### Task 3: Document the feature

**Files:**
- Modify: `docs/guide/` (the page covering runs and scheduling; locate it with the grep in step 1)

**Interfaces:**
- Consumes: everything from phases 1 to 3.
- Produces: user-facing documentation and the deployment knobs.

- [ ] **Step 1: Find the pages that describe running and failure**

Run: `uv run grep -rln "backfill\|run fails\|scheduler" docs/guide docs/extending`
Expected: the pages that need a retry section.

- [ ] **Step 2: Write the documentation**

Cover, in the guide's voice: what a stack is and why a failed attempt is not a verdict; where a policy
is declared and what level each declaration governs (an asset or other operation for its own
execution, a source as the default for its assets, a job for its runs); that an automatic retry always
re-runs only what failed while the manual retry endpoint still takes a scope; and that hooks fire on a
stack's verdict, so a healed workload notifies once, as a success. State plainly that there is no
instance-wide default: nothing retries until a component declares a policy.

Do not document request-level retry. `RESTClient` does not carry a policy yet; that is the third spec,
and describing it here would document a feature that does not exist.

- [ ] **Step 3: Run the full checks**

Run: `uv run ruff check && uv run ty check && uv run pytest`
Expected: clean.

Run from `packages/interloper-app/app/`: `pnpm run lint && pnpm exec nuxt typecheck`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add docs
git commit -m "docs: document the retry system

By Digitl"
```
