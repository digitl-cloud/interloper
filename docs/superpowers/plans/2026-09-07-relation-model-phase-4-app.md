# Relation model, phase 4 (app) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The Nuxt app reads and writes relations by `name`, derives the resource and upstream views from one `relations` map filtered by kind, and handles many-valued relations in the wizard, the graph and the pages.

**Architecture:** Two type files change shape (`types/component.ts`, `types/catalog.ts`); every other file changes only in which helper it calls or which literal it passes. Helpers stay in the type files, as today. Spec: `docs/superpowers/specs/2026-09-07-relation-model-design.md`, section 9 (App).

**Tech Stack:** Nuxt 4, TypeScript, Pinia, @nuxt/ui. Verification is `pnpm run lint`, `pnpm exec nuxt typecheck`, and a headless walk of the wizard and graph against a dev instance (the `verify` skill).

## Global Constraints

- Stacked PR: branch `feat/relation-model-app` from `feat/campaign-matcher` (phase 3) once its final review is clean; PR base is the phase 3 branch, retargeted as the stack merges.
- Conventional Commits ending `By Digitl`; `feat(app):`. Commit only when Guillaume has asked.
- Frontend commands run from `packages/interloper-app/app/`. Follow `packages/interloper-app/app/AGENTS.md`.
- Dev instance on a non-3000 port: `INTERLOPER_SERVER_PORT=3100 make dev-up`.
- No em-dashes anywhere. Comment sparingly; no attribute-level comments.
- `packages/interloper-app/app/app/` is the root of every path below.

---

### Task 1: Types and helpers

**Files:**
- Modify: `types/component.ts:14-30` (`RelationRef { dst_id, dst_kind }`, `Relation { src_id, name, dst_id, dst_kind }`, `RelationInput { dst_id }`), `:83-98` (helpers)
- Modify: `types/catalog.ts:1-35` (`RelationDefinition { kind: string | string[], key: string | string[], many, optional, on_delete, name }`; drop `Dependency`), `:71-95` (helpers)

**Interfaces:**
- Produces in `types/component.ts`:
  ```ts
  export function relationRefs(c: ComponentRecord, name: string): RelationRef[]
  export function relationIds(c: ComponentRecord, name: string): string[]
  export function resourceMap(c: ComponentRecord, defn: ComponentDefinition | undefined): Record<string, string>   // name -> dst_id for relations whose kind is a resource kind
  ```
- Produces in `types/catalog.ts`:
  ```ts
  export const RESOURCE_KINDS = ['connection', 'config', 'resource'] as const
  export function kindsOf(r: RelationDefinition): string[]
  export function keysOf(r: RelationDefinition): string[]
  export function resourceRelations(defn: ComponentDefinition): Record<string, RelationDefinition>   // kind in RESOURCE_KINDS
  export function upstreamRelations(defn: ComponentDefinition): Record<string, RelationDefinition>   // kind includes 'asset'
  export function requiredUpstreams(defn: ComponentDefinition): Record<string, string>              // name -> first key, non-optional only
  export function allowedDestinationKeys(defn: ComponentDefinition): string[]                        // keysOf(relations.destinations)
  ```
- `resourceMap` needs the definition to know which names are resources; callers pass the catalog definition they already hold.

- [ ] **Step 1: Rewrite the two type files** with the shapes above. Keep `parseQualifiedKey`.

- [ ] **Step 2: Typecheck to list every broken call site**

Run: `pnpm exec nuxt typecheck 2>&1 | grep -c "error TS"`
Expected: a non-zero count; the files it names are Tasks 2 to 4's checklist.

- [ ] **Step 3: Commit**

```bash
git add app/types/component.ts app/types/catalog.ts
git commit -m "feat(app): relation types keyed by name; resource and upstream views derived by kind

By Digitl"
```

---

### Task 2: Components store

**Files:**
- Modify: `stores/components.ts:19` (`upstreams` computed: `relations.value.filter(r => r.dst_kind === 'asset' && componentById(r.src_id)?.kind === 'asset')`, or filter on a `src_kind` field if the API adds it; prefer asking the API for `src_kind` in `RelationResponse` if it is not there), `:175-190` (`_relationDetaches`: `const defn = vocabulary?.[r.name]; return defn.on_delete === 'detach' || defn.optional`), `:270-285` (dedup key `${r.src_id}|${r.name}|${r.dst_id}`, delete match on `name`), the `addRelation(componentId, { name, dst_id })` and `removeRelation(componentId, name, dstId)` signatures and their fetch paths (`/components/${id}/relations/${name}/${dstId}`)

- [ ] **Step 1: Apply the changes**, **Step 2: Typecheck** (`pnpm exec nuxt typecheck`), **Step 3: Commit** `feat(app): components store addresses relations by name`.

---

### Task 3: Wizard, asset select, collection composables

**Files:**
- Modify: `components/wizard/DefinitionStepper.vue:24-35` (`RelationStep.name` replaces `type`), `:139-200` (resource steps from `resourceRelations(definition)`: `{ name, resourceKey: keysOf(r)[0] ?? '', optional: r.optional }`), `:245-255` (seed selections by name; `resourceMap(props.component, definition.value)`), `:300-395` (step slots `relation-${name}` and `resource-${name}`; submit builds `relations: { ...Object.fromEntries(relationSteps.map(s => [s.name, ids.map(id => ({ dst_id: id }))])), ...Object.fromEntries(Object.entries(resourceSelections).map(([name, id]) => [name, [{ dst_id: id }]])) }`)
- Modify: `components/sources/Wizard.vue:34` (`relationIds(props.source, 'destinations')`), `:57` (step name `destinations`), `:89` (`addRelation(childId, { name: paramName, dst_id: upstreamId })`), `:133` (unchanged call)
- Modify: `components/sources/AssetSelect.vue:55-80` (`upstreamRelations(assetDefn)`; per relation: `keys = keysOf(r)`, treat each key; a wildcard key `*.x` lists candidates from every source; `many` renders a multi-select and binds every chosen id; `optional` from `r.optional`)
- Modify: `composables/collection.ts:143` (`resourceRelations(sourceDefn)`), `:159` (`relationIds(source, 'destinations')`), `composables/warnings.ts:102` (unchanged helper name, new shape), `:123` (`relationRefs(source, 'destinations')`), `composables/destinationBadge.ts:39`, `composables/partitionGranularity.ts:104` (`'targets'`), `composables/stepperFlow.ts:12` (by name, unchanged)
- Modify: `pages/hooks.vue:60,146,150` (`'watches'`, `'targets'`), `pages/jobs.vue:94,199,206` (`'targets'`), `pages/sources.vue:125` (`'destinations'`), `pages/destinations.vue:66` (`resourceMap(row.original, catalogStore.catalog[row.original.key]).connection`)

- [ ] **Step 1: Apply the changes file by file, typechecking after each**, **Step 2: `pnpm run lint`**, **Step 3: Commit** `feat(app): wizard, asset select and pages bind relations by name; many-valued upstream picker`.

---

### Task 4: Graph

**Files:**
- Modify: `composables/graph.ts:225-240` (pair by `upstreamRelations(spec)`; a candidate satisfies a relation when any of `keysOf(r)` matches its qualified key, with `*.x` matching any source; `paramName` is the relation name), `components/graph/AssetNode.vue:87,128` (`requiredUpstreams`, `removeRelation(d.src_id, d.name, d.dst_id)`), `components/graph/AssetPanel.vue:85,234` (`'destinations'`), `components/graph/GraphCanvas.vue:553` (edge `type: 'upstream'` is a Vue Flow edge type name, unrelated to relations; leave it), the drop handler that calls `addRelation` (`{ name: paramName, dst_id }`)

- [ ] **Step 1: Apply**, **Step 2: Typecheck and lint**, **Step 3: Commit** `feat(app): graph pairs assets by relation name and key list`.

---

### Task 5: Verification and build

- [ ] **Step 1: `pnpm run lint && pnpm exec nuxt typecheck`** clean.
- [ ] **Step 2: Headless walk** (verify skill, port 3100): create a connection, a destination, a source through the wizard (connection step bound by name, destinations step), open the graph, drag a cross-source upstream onto an asset with a many-valued relation and see two edges, delete a destination that a source uses and see the blocking preview, delete a job target's source and see the detaching preview.
- [ ] **Step 3: Retired-name sweep**

Run: `grep -rn "\.slot\b\|slots\b\|slotted\|'upstream'\|'destination'\|'target'\|'watch'\|'resource'" app --include='*.ts' --include='*.vue' | grep -v "handleType\|edge\|type: 'upstream'\|byKind(\|category\|id: 'target'" || echo clean`
Expected: `clean` after reviewing the exclusions (Vue Flow handle types, table column ids and warning categories legitimately use those words).

- [ ] **Step 4: `make build-app`** from the repo root, then open the PR `feat(app): relations by name` when Guillaume asks.
