<script setup lang="ts">
/**
 * Checkbox multi-select for choosing which assets to enable on a source.
 *
 * For assets with cross-source dependencies (qualified keys in the
 * definition's `dependency` relation slots), shows a dropdown to select the
 * upstream asset instance. Auto-resolves when only one candidate exists;
 * prioritises assets within the same source.
 */
import type { AssetDefinition, SourceDefinition } from '~/types/catalog'
import { dependencySlots, parseQualifiedKey } from '~/types/catalog'
import type { ComponentRecord } from '~/types/component'

const selectedKeys = defineModel<string[]>('selectedKeys', { default: () => [] })

/**
 * Cross-source dependency selections.
 * Keyed as `assetKey→paramName` → upstream asset instance id.
 */
const resolvedDeps = defineModel<Record<string, string>>('resolvedDeps', { default: () => ({}) })

const props = defineProps<{
    /** The source definition whose assets to display. */
    sourceDefn: SourceDefinition
    /** All existing sources in the system (for candidate discovery). */
    allSources: ComponentRecord[]
}>()

// ── Helpers ──────────────────────────────────────────────────────────

const catalogStore = useCatalogStore()

interface DepCandidate {
    assetId: string
    sourceId: string
    sourceName: string
    assetName: string
    sameSource: boolean
}

interface AssetDep {
    paramName: string
    qualifiedKey: string
    sourceKey: string
    assetKey: string
    label: string
    isOptional: boolean
    isCrossSource: boolean
    candidates: DepCandidate[]
}

/** Compute dependency info for a given asset definition. */
function getAssetDeps(assetDefn: AssetDefinition): AssetDep[] {
    const deps: AssetDep[] = []
    for (const [paramName, slot] of Object.entries(dependencySlots(assetDefn))) {
        const qk = slot.key
        const isOptional = !slot.required
        const { sourceKey, assetKey } = parseQualifiedKey(qk)
        const isCrossSource = !!sourceKey && sourceKey !== props.sourceDefn.key

        if (!isCrossSource) continue // Intra-source: auto-resolved by framework

        // Find the spec for the upstream asset
        const upstreamSourceDefn = catalogStore.sourceDefinitions.find(s => s.key === sourceKey)
        const upstreamAssetDefn = upstreamSourceDefn?.assets?.find(a => a.key === assetKey)
        const label = upstreamAssetDefn?.name ?? qk

        // Find candidate instances: all sources with matching key that have matching asset
        const candidates: DepCandidate[] = []
        for (const source of props.allSources) {
            if (source.key !== sourceKey) continue
            const asset = source.children.find(a => a.key === assetKey)
            if (!asset) continue
            candidates.push({
                assetId: asset.id,
                sourceId: source.id,
                sourceName: source.name ?? source.key,
                assetName: label,
                sameSource: false, // cross-source by definition
            })
        }

        deps.push({ paramName, qualifiedKey: qk, sourceKey, assetKey, label, isOptional, isCrossSource, candidates })
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
            const depKey = `${assetKey}→${dep.paramName}`
            if (resolvedDeps.value[depKey]) continue // Already resolved
            if (dep.candidates.length === 1) {
                resolvedDeps.value[depKey] = dep.candidates[0]!.assetId
            }
        }
    }
}, { immediate: true, deep: true })

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

function selectAll() {
    selectedKeys.value = props.sourceDefn.assets.map(a => a.key)
}

function deselectAll() {
    selectedKeys.value = []
    resolvedDeps.value = {}
}

function depSelectionKey(assetKey: string, paramName: string): string {
    return `${assetKey}→${paramName}`
}
</script>

<template>
    <div class="flex flex-col gap-3">
        <div class="flex items-center justify-between">
            <span class="text-sm text-muted">
                {{ selectedKeys.length }} of {{ sourceDefn.assets.length }} assets selected
            </span>
            <div class="flex gap-2">
                <UButton size="xs"
                         variant="ghost"
                         label="Select all"
                         @click="selectAll()" />
                <UButton size="xs"
                         variant="ghost"
                         label="Deselect all"
                         @click="deselectAll()" />
            </div>
        </div>

        <div class="flex flex-col gap-2.5">
            <SelectionCard v-for="asset in sourceDefn.assets"
                           :key="asset.key"
                           as="div"
                           :selected="selectedKeys.includes(asset.key)"
                           class="flex flex-col">
                <!-- Asset row -->
                <div class="flex items-center gap-3 px-4 py-3 cursor-pointer"
                     @click="toggle(asset.key)">
                    <UCheckbox :model-value="selectedKeys.includes(asset.key)"
                               @click.stop
                               @update:model-value="toggle(asset.key)" />
                    <div class="flex flex-col min-w-0">
                        <span class="text-sm font-medium">{{ asset.name || asset.key }}</span>
                        <span v-if="asset.description"
                              class="text-xs text-muted truncate">{{ asset.description }}</span>
                    </div>
                    <div class="ml-auto flex items-center gap-2">
                        <UBadge v-if="asset.partitioning"
                                color="neutral"
                                variant="subtle"
                                size="sm">
                            Partitioned
                        </UBadge>
                        <UBadge v-if="depsByAsset.get(asset.key)?.length"
                                color="info"
                                variant="subtle"
                                size="sm">
                            {{ depsByAsset.get(asset.key)!.length }} dep{{ depsByAsset.get(asset.key)!.length > 1 ? 's' : '' }}
                        </UBadge>
                    </div>
                </div>

                <!-- Cross-source dependency selectors (shown when asset is selected) -->
                <div v-if="selectedKeys.includes(asset.key) && depsByAsset.get(asset.key)?.length"
                     class="px-3 pb-3 pt-0 flex flex-col gap-2 border-t border-default/50 mt-0">
                    <div v-for="dep in depsByAsset.get(asset.key)"
                         :key="dep.paramName"
                         class="flex items-center gap-2 pl-8">
                        <UIcon name="i-lucide-git-merge"
                               class="size-3.5 shrink-0"
                               :class="dep.candidates.length === 0 ? 'text-warning' : 'text-muted'" />
                        <span class="text-xs text-muted whitespace-nowrap">{{ dep.label }}</span>

                        <!-- No candidates -->
                        <span v-if="dep.candidates.length === 0"
                              class="text-xs text-warning">
                            No {{ dep.sourceKey }} source found
                        </span>

                        <!-- Single candidate: auto-resolved -->
                        <span v-else-if="dep.candidates.length === 1"
                              class="text-xs text-muted italic">
                            {{ dep.candidates[0]!.sourceName }}
                        </span>

                        <!-- Multiple candidates: dropdown -->
                        <USelectMenu v-else
                                     :model-value="resolvedDeps[depSelectionKey(asset.key, dep.paramName)] ?? ''"
                                     :items="dep.candidates.map(c => ({ label: c.sourceName, value: c.assetId }))"
                                     placeholder="Select source…"
                                     size="xs"
                                     value-key="value"
                                     class="min-w-[180px]"
                                     @update:model-value="resolvedDeps[depSelectionKey(asset.key, dep.paramName)] = $event" />

                        <UBadge v-if="dep.isOptional"
                                color="neutral"
                                variant="outline"
                                size="xs">
                            optional
                        </UBadge>
                    </div>
                </div>
            </SelectionCard>
        </div>
    </div>
</template>
