<script setup lang="ts">
/**
 * Checkbox multi-select for choosing which assets to enable on a source.
 *
 * For assets with cross-source dependencies (qualified or wildcard keys on
 * the definition's upstream relations), shows a picker for the upstream asset
 * instance. Auto-resolves when only one candidate exists; a many-valued
 * relation takes several.
 */
import type { AssetDefinition, SourceDefinition } from '~/types/catalog'
import { ANY_SOURCE, keysOf, parseQualifiedKey, upstreamRelations } from '~/types/catalog'
import type { ComponentRecord } from '~/types/component'

const selectedKeys = defineModel<string[]>('selectedKeys', { default: () => [] })

/**
 * Cross-source dependency selections.
 * Keyed as `assetKey→relationName` → chosen upstream asset instance ids.
 */
const resolvedDeps = defineModel<Record<string, string[]>>('resolvedDeps', { default: () => ({}) })

const props = defineProps<{
    /** The source definition whose assets to display. */
    sourceDefn: SourceDefinition
    /** All existing sources in the system (for candidate discovery). */
    allSources: ComponentRecord[]
}>()

// ── Helpers ──────────────────────────────────────────────────────────

const catalogStore = useCatalogStore()
const appConfig = useAppConfig()

interface DepCandidate {
    assetId: string
    sourceName: string
}

interface AssetDep {
    /** The relation's name, which is also the binding's name on the wire. */
    name: string
    /** The cross-source keys the relation accepts, as declared. */
    keys: string[]
    label: string
    isOptional: boolean
    many: boolean
    candidates: DepCandidate[]
}

/** The instances of `assetKey` a declared key reaches, given its source part. */
function keyCandidates(sourceKey: string, assetKey: string): DepCandidate[] {
    const candidates: DepCandidate[] = []
    for (const source of props.allSources) {
        if (sourceKey !== ANY_SOURCE && source.key !== sourceKey) continue
        const asset = source.children.find(a => a.key === assetKey)
        if (!asset) continue
        candidates.push({ assetId: asset.id, sourceName: source.name ?? source.key })
    }
    return candidates
}

/**
 * The cross-source dependencies a given asset definition declares.
 *
 * One entry per relation, not per key: a relation's keys are alternatives,
 * so whichever of them a candidate matches, it fills the same binding.
 */
function getAssetDeps(assetDefn: AssetDefinition): AssetDep[] {
    const deps: AssetDep[] = []
    for (const [name, relation] of Object.entries(upstreamRelations(assetDefn))) {
        const keys: string[] = []
        const candidates: DepCandidate[] = []
        let label = ''
        for (const declared of keysOf(relation)) {
            const { sourceKey, assetKey } = parseQualifiedKey(declared)
            // A bare key, or one naming this very source, is a sibling the
            // framework binds on its own.
            if (!sourceKey || sourceKey === props.sourceDefn.key) continue
            const upstreamAssetDefn = sourceKey === ANY_SOURCE
                ? catalogStore.sourceDefinitions.flatMap(s => s.assets ?? []).find(a => a.key === assetKey)
                : catalogStore.sourceDefinitions.find(s => s.key === sourceKey)?.assets?.find(a => a.key === assetKey)
            const keyLabel = upstreamAssetDefn?.name ?? declared
            if (!label) label = keyLabel
            keys.push(declared)
            // Overlapping keys ('shop.orders' and '*.orders') can reach the
            // same instance; it is one candidate, listed once.
            for (const candidate of keyCandidates(sourceKey, assetKey)) {
                if (!candidates.some(c => c.assetId === candidate.assetId)) candidates.push(candidate)
            }
        }
        if (!keys.length) continue
        deps.push({ name, keys, label, isOptional: relation.optional, many: relation.many, candidates })
    }

    return deps
}

/** All deps grouped by asset key. */
const depsByAsset = computed(() => {
    const map = new Map<string, AssetDep[]>()
    for (const assetDefn of props.sourceDefn.assets) {
        const deps = getAssetDeps(assetDefn)
        if (deps.length > 0) map.set(assetDefn.key, deps)
    }
    return map
})

// ── Auto-resolution ─────────────────────────────────────────────────

/** Auto-resolve single-candidate deps. */
watch([() => props.allSources, selectedKeys], () => {
    for (const assetKey of selectedKeys.value) {
        const deps = depsByAsset.value.get(assetKey)
        if (!deps) continue
        for (const dep of deps) {
            const depKey = `${assetKey}→${dep.name}`
            if (resolvedDeps.value[depKey]?.length) continue // Already resolved
            if (dep.candidates.length === 1) {
                resolvedDeps.value[depKey] = [dep.candidates[0]!.assetId]
            }
        }
    }
}, { immediate: true, deep: true })

// ── Filters ─────────────────────────────────────────────────────────

const tagFilter = ref('all')
const search = ref('')

const tagTabs = computed(() => [
    { label: 'All', value: 'all' },
    ...[...new Set(props.sourceDefn.assets.flatMap(a => a.tags))].map(tag => ({ label: tag, value: tag })),
])

/** Assets left by the tag tabs and the display-name/key search. */
const filteredAssets = computed(() => {
    const query = search.value.trim().toLowerCase()
    return props.sourceDefn.assets.filter(asset =>
        (tagFilter.value === 'all' || asset.tags.includes(tagFilter.value))
        && (!query
            || asset.key.toLowerCase().includes(query)
            || asset.name.toLowerCase().includes(query)),
    )
})

// ── Selection ───────────────────────────────────────────────────────

function toggle(key: string) {
    const idx = selectedKeys.value.indexOf(key)
    if (idx >= 0) {
        selectedKeys.value.splice(idx, 1)
        // Clean up resolved deps for deselected asset
        const cleaned = Object.fromEntries(
            Object.entries(resolvedDeps.value).filter(([k]) => !k.startsWith(`${key}→`)),
        )
        resolvedDeps.value = cleaned
    }
    else {
        selectedKeys.value.push(key)
    }
}

/** Select/deselect operate on the visible (filtered) assets only. */
function selectAll() {
    selectedKeys.value = [...new Set([...selectedKeys.value, ...filteredAssets.value.map(a => a.key)])]
}

function deselectAll() {
    const visible = new Set(filteredAssets.value.map(a => a.key))
    selectedKeys.value = selectedKeys.value.filter(k => !visible.has(k))
    resolvedDeps.value = Object.fromEntries(
        Object.entries(resolvedDeps.value).filter(([k]) => !visible.has(k.split('→')[0]!)),
    )
}

function depSelectionKey(assetKey: string, name: string): string {
    return `${assetKey}→${name}`
}

function dependencyCount(count: number): string {
    return `${count} ${count === 1 ? 'dependency' : 'dependencies'}`
}

function depItems(dep: AssetDep) {
    return dep.candidates.map(c => ({ label: c.sourceName, value: c.assetId }))
}

/** Record a picker's choice; a single-valued relation still stores a one-id list. */
function setDep(assetKey: string, dep: AssetDep, value: string | string[]) {
    resolvedDeps.value[depSelectionKey(assetKey, dep.name)] = Array.isArray(value) ? value : [value]
}
</script>

<template>
    <div class="flex flex-col gap-3">
        <div class="flex items-center gap-2">
            <UTabs v-if="tagTabs.length > 2"
                   v-model="tagFilter"
                   :items="tagTabs"
                   variant="pill"
                   size="xs"
                   :content="false"
                   class="w-fit" />
            <UInput v-model="search"
                    placeholder="Search assets…"
                    :icon="appConfig.ui.icons.search"
                    class="min-w-0 flex-1" />
        </div>

        <div class="flex items-center justify-between">
            <span class="text-sm text-muted tabular-nums">
                {{ selectedKeys.length }} of {{ sourceDefn.assets.length }} assets selected
            </span>
            <div class="flex gap-2">
                <UButton size="sm"
                         color="neutral"
                         variant="ghost"
                         label="Select all"
                         @click="selectAll()" />
                <UButton size="sm"
                         color="neutral"
                         variant="ghost"
                         label="Deselect all"
                         @click="deselectAll()" />
            </div>
        </div>

        <UEmpty v-if="filteredAssets.length === 0"
                variant="naked"
                size="sm"
                :icon="appConfig.ui.icons.search"
                title="No assets match these filters" />

        <div v-else
             class="flex flex-col gap-2">
            <SelectionCard v-for="asset in filteredAssets"
                           :key="asset.key"
                           as="div"
                           :selected="selectedKeys.includes(asset.key)"
                           :label="asset.name || asset.key"
                           :description="asset.description"
                           @select="toggle(asset.key)">
                <template #leading>
                    <UCheckbox :model-value="selectedKeys.includes(asset.key)"
                               @click.stop
                               @update:model-value="toggle(asset.key)" />
                </template>
                <template v-if="asset.tags.length"
                          #details>
                    <div class="mt-1.5 flex flex-wrap gap-1">
                        <UBadge v-for="tag in asset.tags"
                                :key="tag"
                                color="neutral"
                                size="sm"
                                :label="tag" />
                    </div>
                </template>
                <template v-if="asset.partitioning || depsByAsset.get(asset.key)?.length"
                          #trailing>
                    <UTooltip v-if="asset.partitioning"
                              text="This asset is partitioned"
                              :delay-duration="0">
                        <UIcon name="i-lucide-layers"
                               class="size-4 shrink-0 text-muted" />
                    </UTooltip>
                    <UBadge v-if="depsByAsset.get(asset.key)?.length"
                            color="neutral"
                            size="sm"
                            :label="dependencyCount(depsByAsset.get(asset.key)!.length)" />
                </template>

                <!-- Cross-source dependency selectors (shown when asset is selected); clicks here don't toggle the asset -->
                <div v-if="selectedKeys.includes(asset.key) && depsByAsset.get(asset.key)?.length"
                     class="flex cursor-default flex-col gap-2 border-t border-default px-4 py-3"
                     @click.stop>
                    <div v-for="dep in depsByAsset.get(asset.key)"
                         :key="dep.name"
                         class="flex items-center gap-2 pl-8">
                        <UIcon name="i-lucide-git-merge"
                               class="size-3.5 shrink-0"
                               :class="dep.candidates.length === 0 ? 'text-warning' : 'text-muted'" />
                        <span class="text-xs whitespace-nowrap text-muted">{{ dep.label }}</span>

                        <!-- No candidates -->
                        <span v-if="dep.candidates.length === 0"
                              class="text-xs text-warning">
                            No source found for {{ dep.keys.join(' or ') }}
                        </span>

                        <!-- Single candidate: auto-resolved -->
                        <span v-else-if="dep.candidates.length === 1"
                              class="text-xs text-muted">
                            {{ dep.candidates[0]!.sourceName }}
                        </span>

                        <!-- Several candidates: a picker, multiple when the relation fans in -->
                        <USelectMenu v-else-if="dep.many"
                                     :model-value="resolvedDeps[depSelectionKey(asset.key, dep.name)] ?? []"
                                     :items="depItems(dep)"
                                     multiple
                                     placeholder="Select sources…"
                                     size="xs"
                                     value-key="value"
                                     class="min-w-48"
                                     @update:model-value="setDep(asset.key, dep, $event)" />

                        <USelectMenu v-else
                                     :model-value="resolvedDeps[depSelectionKey(asset.key, dep.name)]?.[0] ?? ''"
                                     :items="depItems(dep)"
                                     placeholder="Select source…"
                                     size="xs"
                                     value-key="value"
                                     class="min-w-48"
                                     @update:model-value="setDep(asset.key, dep, $event)" />

                        <UBadge v-if="dep.isOptional"
                                color="neutral"
                                size="xs"
                                label="Optional" />
                    </div>
                </div>
            </SelectionCard>
        </div>
    </div>
</template>
