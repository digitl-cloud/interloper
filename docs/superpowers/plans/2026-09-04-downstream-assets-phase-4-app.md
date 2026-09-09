# Many-valued upstreams, phase 4 (app) Implementation Plan

> **SUPERSEDED 2026-09-07** by `2026-09-07-relation-model-phase-*.md` (design `2026-09-07-relation-model-design.md`). Kept for the record; phase 1 here was executed as PR #321 and is being reworked.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a user see and edit many-valued and wildcard dependency slots in the Nuxt app: pick legs in the source wizard, see persisted bindings in the asset panel, connect legs on the graph, and get nudged when a matching asset is not bound.

**Architecture:** One shared helper `slotAccepts` mirrors core's `AssetIdentity.satisfies`. The wizard's asset step, the graph connection rules, the warnings composable and the panel all switch from exact qualified-key equality to that helper, and from definition-only views to persisted relations read through a new `upstreamMap` accessor. No new pages.

**Tech Stack:** Nuxt 4, Vue 3, @nuxt/ui, TypeScript. No unit test runner in the app; verification is `pnpm run lint`, `pnpm exec nuxt typecheck` and a headless pass on a dev instance (the `verify` project skill). Spec: `docs/superpowers/specs/2026-09-04-downstream-assets-design.md` section 2.6.

## Global Constraints

- Branch `feat/many-upstreams-app` from `main` after phases 1 to 3 merged (the matcher is the verification subject).
- Conventional Commits ending with `By Digitl`; commit only when asked.
- Frontend commands run from `packages/interloper-app/app/`: `pnpm run lint`, `pnpm exec nuxt typecheck`. Both must pass after every task.
- Follow `packages/interloper-app/app/AGENTS.md`. Keep components small; no attribute-level comments.
- Dev instance on a non-3000 port: `INTERLOPER_SERVER_PORT=3100 make dev-up`; never reset the shared dev database.
- No em-dashes anywhere. All paths below are relative to `packages/interloper-app/app/app/`.

---

### Task 1: Types and the `slotAccepts` helper

**Files:**
- Modify: `types/catalog.ts` (`Dependency` interface, new helpers)
- Modify: `types/component.ts` (new `upstreamMap`)

**Interfaces:**
- Produces: `slotAccepts(slotKey: string, upstream: { sourceKey: string; assetKey: string }, ownSourceKey: string): boolean`.
- Produces: `outwardUpstreamSlots(defn): Record<string, Dependency>` (slots whose key names another source or the wildcard).
- Produces: `upstreamMap(c: ComponentRecord): Record<string, string[]>` (slot to upstream ids, from the `upstream` relation).

- [ ] **Step 1: Confirm the `Dependency` interface**

Phase 1 already renamed `RelationSlot` to `Dependency` with `optional` and `many` in `types/catalog.ts`; check it reads:

```ts
/** A declared dependency: a slot on a slotted relation type. Mirrors `interloper.component.base.Dependency`. */
export interface Dependency {
    /** Expected dst component key. `''` accepts any component of the relation's kinds. */
    key: string
    /** Whether the slot may stay unbound. */
    optional: boolean
    /** Whether the slot binds several components (a fan-in dependency). */
    many: boolean
}
```

- [ ] **Step 2: Add the helpers after `requiredUpstreams`**

```ts
export const ANY_SOURCE = '*'

/**
 * Whether an upstream identity satisfies a dependency slot key.
 * Mirrors `AssetIdentity.satisfies` in interloper-core: a bare key names a
 * sibling of `ownSourceKey`, `source.asset` names that source type, and
 * `*.asset` accepts that asset key from any source.
 */
export function slotAccepts(
    slotKey: string,
    upstream: { sourceKey: string; assetKey: string },
    ownSourceKey: string,
): boolean {
    if (!slotKey) return true
    const { sourceKey, assetKey } = parseQualifiedKey(slotKey)
    if (upstream.assetKey !== assetKey) return false
    const expectedSource = sourceKey || ownSourceKey
    return expectedSource === ANY_SOURCE || upstream.sourceKey === expectedSource
}

/** Dependency slots whose key reaches outside the declaring source (qualified or wildcard). */
export function outwardUpstreamSlots(defn: ComponentDefinition, ownSourceKey: string): Record<string, Dependency> {
    return Object.fromEntries(
        Object.entries(upstreamSlots(defn)).filter(([, slot]) => {
            const { sourceKey } = parseQualifiedKey(slot.key)
            return !!sourceKey && sourceKey !== ownSourceKey
        }),
    )
}
```

`parseQualifiedKey` is defined later in the same file; function declarations hoist, so the order is fine.

- [ ] **Step 3: Add `upstreamMap` to `types/component.ts` after `resourceMap`**

```ts
/** Upstream relations as a {slot: upstream ids} map (several ids for a many-valued slot). */
export function upstreamMap(c: ComponentRecord): Record<string, string[]> {
    const map: Record<string, string[]> = {}
    for (const ref of relationRefs(c, 'upstream')) (map[ref.slot] ??= []).push(ref.dst_id)
    return map
}
```

- [ ] **Step 4: Lint and typecheck**

Run: `pnpm run lint && pnpm exec nuxt typecheck`
Expected: clean.

- [ ] **Step 5: Stage (commit only if asked)**

```bash
git add app/types/catalog.ts app/types/component.ts
git commit -m "feat(app): type many-valued dependency slots and add slotAccepts

By Digitl"
```

---

### Task 2: Wizard asset step picks legs for every outward slot

**Files:**
- Modify: `components/sources/AssetSelect.vue`
- Modify: `components/sources/Wizard.vue`

**Interfaces:**
- `resolvedDeps` model changes to `Record<string, string[]>` keyed `assetKey→paramName` (one-element arrays for single slots).

- [ ] **Step 1: Rewrite `getAssetDeps` in `AssetSelect.vue`**

Replace the loop body with:

```ts
function getAssetDeps(assetDefn: AssetDefinition): AssetDep[] {
    const deps: AssetDep[] = []
    for (const [paramName, slot] of Object.entries(outwardUpstreamSlots(assetDefn, props.sourceDefn.key))) {
        const { sourceKey, assetKey } = parseQualifiedKey(slot.key)
        const label = catalogStore.getAssetDefinition(slot.key)?.name
            ?? catalogStore.sourceDefinitions.flatMap(s => s.assets ?? []).find(a => a.key === assetKey)?.name
            ?? slot.key

        const candidates: DepCandidate[] = []
        for (const source of props.allSources) {
            for (const asset of source.children) {
                if (!slotAccepts(slot.key, { sourceKey: source.key, assetKey: asset.key }, props.sourceDefn.key)) continue
                candidates.push({
                    assetId: asset.id,
                    sourceId: source.id,
                    sourceName: source.name ?? source.key,
                    assetName: label,
                    sameSource: false,
                })
            }
        }

        deps.push({
            paramName,
            qualifiedKey: slot.key,
            sourceKey,
            assetKey,
            label,
            isOptional: slot.optional,
            isMany: slot.many,
            isCrossSource: true,
            candidates,
        })
    }
    return deps
}
```

Add `isMany: boolean` to `AssetDep`, import `outwardUpstreamSlots, slotAccepts` from `~/types/catalog`, and change the model to `defineModel<Record<string, string[]>>('resolvedDeps', ...)`.

- [ ] **Step 2: Auto-resolution**

Replace the watcher body: a single slot with exactly one candidate resolves to `[candidate]`; a many slot with no selection yet resolves to every candidate id (bind-all by default).

```ts
watch([() => props.allSources, selectedKeys], () => {
    for (const assetKey of selectedKeys.value) {
        for (const dep of depsByAsset.value.get(assetKey) ?? []) {
            const depKey = depSelectionKey(assetKey, dep.paramName)
            if (resolvedDeps.value[depKey]?.length) continue
            if (dep.isMany && dep.candidates.length > 0) resolvedDeps.value[depKey] = dep.candidates.map(c => c.assetId)
            else if (!dep.isMany && dep.candidates.length === 1) resolvedDeps.value[depKey] = [dep.candidates[0]!.assetId]
        }
    }
}, { immediate: true, deep: true })
```

- [ ] **Step 3: Template**

Replace the three candidate branches (none, single, multiple) with:

```vue
<span v-if="dep.candidates.length === 0" class="text-xs text-warning">
    No matching {{ dep.assetKey }} asset found
</span>
<USelectMenu v-else-if="dep.isMany"
             :model-value="resolvedDeps[depSelectionKey(asset.key, dep.paramName)] ?? []"
             :items="dep.candidates.map(c => ({ label: `${c.sourceName}, ${c.assetName}`, value: c.assetId }))"
             multiple
             placeholder="Select upstreams…"
             size="xs"
             value-key="value"
             class="min-w-[220px]"
             @update:model-value="resolvedDeps[depSelectionKey(asset.key, dep.paramName)] = $event" />
<USelectMenu v-else
             :model-value="resolvedDeps[depSelectionKey(asset.key, dep.paramName)]?.[0] ?? ''"
             :items="dep.candidates.map(c => ({ label: `${c.sourceName}, ${c.assetName}`, value: c.assetId }))"
             placeholder="Select upstream…"
             size="xs"
             value-key="value"
             class="min-w-[220px]"
             @update:model-value="resolvedDeps[depSelectionKey(asset.key, dep.paramName)] = [$event]" />
<UBadge v-if="dep.isMany" color="neutral" size="xs">
    {{ (resolvedDeps[depSelectionKey(asset.key, dep.paramName)] ?? []).length }} of {{ dep.candidates.length }}
</UBadge>
<UBadge v-if="dep.isOptional" color="neutral" size="xs">optional</UBadge>
```

A single slot with one candidate now shows a one-item select instead of italic text, which also makes it changeable. Keep the `git-merge` icon row.

- [ ] **Step 4: Wizard: seed from persisted bindings, replace the set on save, surface errors**

In `Wizard.vue`:

```ts
const resolvedCrossDeps = ref<Record<string, string[]>>(seedCrossDeps(props.source))

function seedCrossDeps(source: ComponentRecord | null): Record<string, string[]> {
    const seeded: Record<string, string[]> = {}
    for (const asset of source?.children ?? []) {
        for (const [slot, ids] of Object.entries(upstreamMap(asset))) seeded[`${asset.key}→${slot}`] = ids
    }
    return seeded
}
```

Import `upstreamMap` from `~/types/component`. Rewrite `wireCrossDeps` to send the whole dependency set per child with one `PUT` (the API replaces the listed relation type):

```ts
async function wireCrossDeps(saved: ComponentRecord) {
    const toast = useToast()
    const byChild = new Map<string, Array<{ dst_id: string; slot: string }>>()
    for (const [key, upstreamIds] of Object.entries(resolvedCrossDeps.value)) {
        const [assetKey, paramName] = key.split('→')
        const child = saved.children.find(a => a.key === assetKey)
        if (!child || !paramName) continue
        for (const dst_id of upstreamIds) (byChild.get(child.id) ?? byChild.set(child.id, []).get(child.id)!).push({ dst_id, slot: paramName })
    }
    const results = await Promise.allSettled(
        [...byChild].map(([childId, upstream]) => componentsStore.update(childId, { relations: { upstream } })),
    )
    const failed = results.find((r): r is PromiseRejectedResult => r.status === 'rejected')
    if (failed) toast.add(errorToast(failed.reason, 'Failed to wire dependencies'))
}
```

Check `componentsStore.update(id, input)` exists with a `ComponentInput` payload (it is what `DriftBanner` uses for `children`); if it is named differently, use that name. Only children that appear in `resolvedCrossDeps` are touched, so assets whose slots were untouched keep their edges; a child whose selection was emptied gets an empty list, which the backend refuses for a required `on_unbind=block` slot and the toast shows why.

- [ ] **Step 5: Lint and typecheck**

Run: `pnpm run lint && pnpm exec nuxt typecheck`
Expected: clean.

- [ ] **Step 6: Stage (commit only if asked)**

```bash
git add app/components/sources/AssetSelect.vue app/components/sources/Wizard.vue
git commit -m "feat(app): pick legs for many-valued and wildcard dependency slots in the source wizard

By Digitl"
```

---

### Task 3: Asset panel shows persisted bindings and the nudge

**Files:**
- Modify: `components/graph/AssetPanel.vue:232-244` (`dependencyRows`) and the "Upstream dependencies" template block (lines 474-506)
- Create: `composables/dependencyBindings.ts`

**Interfaces:**
- Produces: `useDependencyBindings()` returning `bindingsFor(asset: ComponentRecord, assetDefn: AssetDefinition | undefined, source: ComponentRecord | undefined): SlotBinding[]` where

```ts
export interface SlotBinding {
    param: string
    slot: Dependency
    bound: Array<{ assetId: string; label: string; icon: string }>
    unboundMatches: Array<{ assetId: string; label: string; icon: string }>
}
```

- [ ] **Step 1: Write the composable**

```ts
import type { ComponentRecord } from '~/types/component'
import { upstreamMap } from '~/types/component'
import type { AssetDefinition, Dependency } from '~/types/catalog'
import { upstreamSlots, slotAccepts } from '~/types/catalog'

export interface SlotBinding {
    param: string
    slot: Dependency
    bound: Array<{ assetId: string; label: string; icon: string }>
    unboundMatches: Array<{ assetId: string; label: string; icon: string }>
}

/** Declared dependency slots of an asset joined with its persisted bindings and the org's matching assets. */
export function useDependencyBindings() {
    const componentsStore = useComponentsStore()
    const assetDisplayName = useAssetDisplayName()

    function describe(assetId: string) {
        const names = assetDisplayName.value.get(assetId)
        return { assetId, label: names?.label ?? assetId, icon: names?.sourceIcon ?? 'i-lucide-box' }
    }

    function bindingsFor(
        asset: ComponentRecord,
        assetDefn: AssetDefinition | undefined,
        source: ComponentRecord | undefined,
    ): SlotBinding[] {
        if (!assetDefn) return []
        const bound = upstreamMap(asset)
        const ownSourceKey = source?.key ?? ''
        return Object.entries(upstreamSlots(assetDefn)).map(([param, slot]) => {
            const boundIds = new Set(bound[param] ?? [])
            const matches: string[] = []
            for (const candidateSource of componentsStore.byKind('source')) {
                for (const candidate of candidateSource.children) {
                    if (candidate.id === asset.id || boundIds.has(candidate.id)) continue
                    if (slotAccepts(slot.key, { sourceKey: candidateSource.key, assetKey: candidate.key }, ownSourceKey)) matches.push(candidate.id)
                }
            }
            return {
                param,
                slot,
                bound: [...boundIds].map(describe),
                unboundMatches: slot.many ? matches.map(describe) : [],
            }
        })
    }

    return { bindingsFor }
}
```

Bare (sibling) slots of a single kind list their bound sibling too, since `upstreamMap` reads the persisted intra-source edges.

- [ ] **Step 2: Use it in `AssetPanel.vue`**

Replace `dependencyRows` with:

```ts
const { bindingsFor } = useDependencyBindings()
const slotBindings = computed(() => bindingsFor(props.asset, props.assetDefn, props.source))
const unboundMatchCount = computed(() => slotBindings.value.reduce((n, b) => n + b.unboundMatches.length, 0))

async function bindAllMatching(binding: SlotBinding) {
    try {
        await Promise.all(binding.unboundMatches.map(m =>
            componentsStore.addRelation(props.asset.id, { type: 'upstream', dst_id: m.assetId, slot: binding.param }),
        ))
        toast.add({ title: `Bound ${binding.unboundMatches.length} upstream(s)`, color: 'success' })
    }
    catch (e) {
        toast.add(errorToast(e, 'Failed to bind upstreams'))
    }
}
```

The panel already receives `source: ComponentRecord` as a prop (both pages pass it), and `toast` plus `errorToast` are already in scope there.

Template: the collapsible header badge becomes `{{ slotBindings.length }}`; body renders one block per `binding`:

```vue
<div v-for="binding in slotBindings" :key="binding.param" class="flex flex-col gap-1.5">
    <div class="flex items-center gap-2">
        <UBadge color="neutral" size="sm" class="font-mono">{{ binding.param }}</UBadge>
        <span class="text-xs text-muted">{{ binding.slot.key }}</span>
        <UBadge v-if="binding.slot.many" color="neutral" size="xs">{{ binding.bound.length }} bound</UBadge>
        <UBadge v-if="binding.slot.optional" color="neutral" size="xs">optional</UBadge>
        <UBadge v-else-if="binding.bound.length === 0" color="warning" size="xs">unbound</UBadge>
    </div>
    <div v-for="leg in binding.bound" :key="leg.assetId" class="flex items-center gap-2.5 rounded-md bg-muted px-3 py-2">
        <UIcon :name="leg.icon" class="size-4 shrink-0 text-muted" />
        <span class="truncate text-sm">{{ leg.label }}</span>
    </div>
    <div v-if="binding.unboundMatches.length" class="flex items-center justify-between rounded-md border border-dashed border-default px-3 py-2">
        <span class="text-xs text-muted">{{ binding.unboundMatches.length }} matching, not bound</span>
        <UButton size="xs" variant="ghost" label="Bind all" :disabled="graphReadonly" @click="bindAllMatching(binding)" />
    </div>
</div>
```

Show the collapsible when `slotBindings.length > 0` (so optional-only assets get a section too). If `graphReadonly` is not injected in the panel, drop the `:disabled`.

- [ ] **Step 3: Lint and typecheck**

Run: `pnpm run lint && pnpm exec nuxt typecheck`
Expected: clean.

- [ ] **Step 4: Stage (commit only if asked)**

```bash
git add app/composables/dependencyBindings.ts app/components/graph/AssetPanel.vue app/pages/graph.vue app/pages/collection.vue
git commit -m "feat(app): show persisted dependency bindings and a bind-all nudge in the asset panel

By Digitl"
```

---

### Task 4: Graph connection rules and handles

**Files:**
- Modify: `composables/graph.ts:225-237` (`resolveConnectionPairs`)
- Modify: `components/graph/AssetNode.vue:86-96` (`isRunnable`, `showTargetHandle`)

- [ ] **Step 1: Match slots with `slotAccepts`**

In `resolveConnectionPairs`, `upstream.qk` is qualified; parse it and match every slot, allowing a many slot to take further legs:

```ts
for (const downstream of downstreamAssets) {
    const spec = options.getAssetDefinition(downstream.qk)
    if (!spec) continue
    const ownSourceKey = parseQualifiedKey(downstream.qk).sourceKey
    for (const upstream of upstreamAssets) {
        if (upstream.id === downstream.id) continue
        const identity = parseQualifiedKey(upstream.qk)
        const match = Object.entries(upstreamSlots(spec)).find(([, slot]) => slotAccepts(slot.key, identity, ownSourceKey))
        if (!match) continue
        const [paramName] = match
        if (depExists(downstream.id, upstream.id)) continue
        pairs.push({ upstreamAssetId: upstream.id, downstreamAssetId: downstream.id, paramName })
    }
}
```

Import `parseQualifiedKey, slotAccepts` from `~/types/catalog`. Standalone downstreams have an empty `ownSourceKey`, so their bare keys match nothing, which is the core behaviour too.

- [ ] **Step 2: Per-slot runnability and the target handle**

In `AssetNode.vue` replace `requiredDepCount` / `isRunnable` / `showTargetHandle`:

```ts
const declaredSlots = computed(() => (props.assetDefn ? upstreamSlots(props.assetDefn) : {}))
const isRunnable = computed(() => {
    const bound = upstreamMap(props.asset)
    return Object.entries(declaredSlots.value).every(([param, slot]) => slot.optional || (bound[param]?.length ?? 0) > 0)
})
const showTargetHandle = computed(() =>
    hasUpstream.value || Object.keys(declaredSlots.value).length > 0 || isValidTarget.value,
)
```

Import `upstreamSlots` (replacing `requiredUpstreams`) from `~/types/catalog` and `upstreamMap` from `~/types/component`. `props.asset.relations` carries the embedded refs from `GET /components`, so no extra fetch is needed; if the graph's asset records lack `relations`, fall back to `componentsStore.upstreams.filter(d => d.src_id === props.asset.id)` grouped by `slot`.

- [ ] **Step 3: Lint and typecheck**

Run: `pnpm run lint && pnpm exec nuxt typecheck`
Expected: clean.

- [ ] **Step 4: Stage (commit only if asked)**

```bash
git add app/composables/graph.ts app/components/graph/AssetNode.vue
git commit -m "feat(app): connect legs to many-valued and wildcard slots on the graph

By Digitl"
```

---

### Task 5: Warnings per slot and the matching-but-unbound nudge

**Files:**
- Modify: `composables/warnings.ts:99-120`

- [ ] **Step 1: Rewrite the dependency block of `getWarnings`**

```ts
if (defn) {
    const asset = assetById.value.get(assetId)
    const bound = asset ? upstreamMap(asset) : {}
    const ownSourceKey = source?.key ?? ''
    for (const [param, slot] of Object.entries(upstreamSlots(defn))) {
        const boundIds = bound[param] ?? []
        if (!slot.optional && boundIds.length === 0) {
            const depName = getAssetDefinition(slot.key)?.name ?? slot.key
            warnings.push({ category: 'dependency', message: `Missing dependency: ${depName} (${param})` })
        }
        if (!slot.many) continue
        let unbound = 0
        for (const candidateSource of sources.value) {
            for (const candidate of candidateSource.children) {
                if (candidate.id === assetId || boundIds.includes(candidate.id)) continue
                if (slotAccepts(slot.key, { sourceKey: candidateSource.key, assetKey: candidate.key }, ownSourceKey)) unbound += 1
            }
        }
        if (unbound > 0) {
            warnings.push({ category: 'dependency', message: `${unbound} matching asset${unbound > 1 ? 's' : ''} not bound to ${param}` })
        }
    }
}
```

Import `upstreamMap` from `~/types/component` and `upstreamSlots, slotAccepts` from `~/types/catalog` (drop `requiredUpstreams` if now unused). `getAssetDefinition` in this composable already handles bare keys; for a wildcard key it returns `undefined` and the message falls back to the key. The `upstreamsByAssetId` map becomes unused; remove it.

- [ ] **Step 2: Lint and typecheck**

Run: `pnpm run lint && pnpm exec nuxt typecheck`

- [ ] **Step 3: Stage (commit only if asked)**

```bash
git add app/composables/warnings.ts
git commit -m "feat(app): warn per dependency slot and nudge on matching unbound assets

By Digitl"
```

---

### Task 6: Collection tooltip and qualified definition lookup

**Files:**
- Modify: `components/collection/Table.vue:360-361`
- Modify: `composables/collection.ts:65-90`
- Modify: `pages/collection.vue:31-37`

- [ ] **Step 1: Carry the leg label**

In `collection.ts` the map already computes `{ name, icon }`; replace `name = defn.name` with the display label from `useAssetDisplayName()` (instantiate it at the top of the composable): `name = assetDisplayName.value.get(dep.dst_id)?.label ?? defn.name`.

- [ ] **Step 2: Tooltip on the count**

Replace the bare count span in `Table.vue`:

```vue
<UTooltip v-else-if="row.original.dependencies.length > 0" :delay-duration="0">
    <span class="text-muted">{{ row.original.dependencies.length }}</span>
    <template #content>
        <div class="flex flex-col gap-1 text-xs">
            <div v-for="dep in row.original.dependencies" :key="dep.name" class="flex items-center gap-1.5">
                <UIcon :name="dep.icon" class="size-3.5 shrink-0" />
                <span>{{ dep.name }}</span>
            </div>
        </div>
    </template>
</UTooltip>
```

- [ ] **Step 3: Qualified lookup in `pages/collection.vue`**

Replace `getAssetDefinition(key)` with a version that takes the source key:

```ts
function getAssetDefinition(sourceKey: string, key: string): AssetDefinition | undefined {
    return catalogStore.getSourceDefinition(sourceKey)?.assets?.find(a => a.key === key)
}
```

and update its call in `onViewAsset` to `getAssetDefinition(source.key, asset.key)`.

- [ ] **Step 4: Lint and typecheck; stage (commit only if asked)**

```bash
git add app/components/collection/Table.vue app/composables/collection.ts app/pages/collection.vue
git commit -m "fix(app): list dependency legs in the collection tooltip and resolve definitions by source

By Digitl"
```

---

### Task 7: Headless verification

Follow the `verify` project skill. Dev instance on `:3100`, session reused from `:3000`, never reset the shared database, delete every component you create.

- [ ] **Step 1: Seed the scenario**

Create two provider sources (`snapchat_ads`, `tiktok_ads`) with placeholder connections through the wizard or the API, then open "New source", pick Campaign Matcher.

- [ ] **Step 2: Wizard**

On the Assets step, `campaign_matches` shows one dependency row `campaigns` with a multi-select preselecting both `campaigns` legs and a `2 of 2` badge. Deselect one, save. `GET /components/relations?type=upstream` shows one edge. Reopen the source in edit mode: the multi-select shows the one bound leg. Screenshot both states.

- [ ] **Step 3: Panel and nudge**

Open the matcher asset on the collection page and on the graph page. The "Upstream dependencies" section shows `campaigns`, `1 bound`, the bound leg labelled "Source name, Campaigns", and "1 matching, not bound" with "Bind all". Click it; the section shows `2 bound`; the warning badge on the graph node disappears. Screenshot before and after.

- [ ] **Step 4: Graph drag**

Remove one leg with the edge context menu, then drag from the freed `campaigns` node to the matcher asset: the connection is accepted (the handle highlights) and the edge reappears. Screenshot.

- [ ] **Step 5: Collection column**

Hover the matcher's dependency count in the collection table: the tooltip lists both legs.

- [ ] **Step 6: Clean up and open the PR**

Delete the matcher, then the provider sources and connections. Title `feat(app): many-valued dependency slots in the wizard, panel, graph and warnings`. Attach the screenshots, link the spec, end with `By Digitl`.
