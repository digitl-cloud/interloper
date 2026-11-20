<script setup lang="ts">
import type { Connection } from '@vue-flow/core'
import type { DependencyPair } from '~/composables/graph'
import { qualifiedKey } from '~/types/catalog'
import type { ComponentRecord } from '~/types/component'

/**
 * Collection adapter: binds the store-backed collection model + dependency-editing
 * rules to the presentational <GraphCanvas>. Keeps the original props/emits
 * so the Assets graph is unaffected.
 */
const props = withDefaults(defineProps<{
    sourceIds?: string[]
    readonly?: boolean
    groupBy?: GroupBy
    statusFilter?: StatusFilter
    /** The assets a search matched, and the sources holding them; null shows every asset. */
    assetIds?: Set<string> | null
    showNewSourceButton?: boolean
    /** Asset id whose panel is open — the only highlighted node. */
    selectedId?: string | null
}>(), {
    sourceIds: undefined,
    readonly: false,
    groupBy: 'type',
    statusFilter: 'all',
    assetIds: null,
    showNewSourceButton: true,
    selectedId: null,
})

const emit = defineEmits<{
    'add-source': []
    'edit-source': [sourceId: string]
    'asset-click': [asset: ComponentRecord, assetDefn: AssetDefinition | undefined, source: ComponentRecord | null]
    'pane-click': []
    'delete-source': [sourceId: string]
    'create-dependencies': [pairs: DependencyPair[]]
    'delete-dependency': [payload: { upstreamAssetId: string; downstreamAssetId: string; name?: string }]
}>()

const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const { loading, upstreams: assetDependencies } = storeToRefs(componentsStore)

const { model } = useCollectionGraph({ sourceIds: () => props.sourceIds })
const { sourceStatus } = useNodeStatus()

// ── Maps for connection validation/creation (collection editing only) ──
const sources = computed(() => model.value.sources.map(e => e.source))

const assetToSource = computed(() => {
    const map = new Map<string, string | null>()
    for (const e of model.value.assets) map.set(e.asset.id, e.source?.id ?? null)
    return map
})

const qualifiedKeyById = computed(() => {
    const map = new Map<string, string>()
    for (const e of model.value.assets) {
        map.set(e.asset.id, e.source ? qualifiedKey(e.source.key, e.asset.key) : e.asset.key)
    }
    return map
})

function getAssetDefinition(qk: string): AssetDefinition | undefined {
    return catalogStore.getAssetDefinition(qk)
}

// Filtered model for display: the status filter narrows which sources render, a search which assets.
const displayModel = computed<GraphModel>(() => {
    const m = model.value
    const { statusFilter, assetIds } = props
    if (statusFilter === 'all' && !assetIds) return m

    const statusKept = new Set(m.sources.filter(s => matchesStatusFilter(sourceStatus(s.source).state, statusFilter)).map(s => s.source.id))
    // Standalone assets (no source) pass the status filter; a search keeps only the matches.
    const assets = m.assets.filter(a => (a.source === null || statusKept.has(a.source.id)) && (!assetIds || assetIds.has(a.asset.id)))
    const shownSources = new Set(assets.map(a => a.source?.id))

    return {
        sources: m.sources.filter(s => statusKept.has(s.source.id) && (!assetIds || shownSources.has(s.source.id))),
        assets,
        dependencies: m.dependencies,
    }
})

const { resolveConnectionPairs, isValidConnection } = useGraphConnectionRules({
    sources,
    assetDependencies,
    assetToSource,
    qualifiedKeyById,
    getAssetDefinition,
})

function onConnect(connection: Connection) {
    if (props.readonly) return
    emit('create-dependencies', resolveConnectionPairs(connection))
}
</script>

<template>
    <GraphCanvas :model="displayModel"
                 :editable="!readonly"
                 :loading="loading"
                 :group-by="groupBy"
                 :show-new-source-button="showNewSourceButton"
                 :selected-id="selectedId"
                 :is-valid-connection="isValidConnection"
                 @add-source="emit('add-source')"
                 @edit-source="emit('edit-source', $event)"
                 @asset-click="(a, d, s) => emit('asset-click', a, d, s)"
                 @delete-source="emit('delete-source', $event)"
                 @connect="onConnect"
                 @delete-dependency="emit('delete-dependency', $event)"
                 @pane-click="emit('pane-click')" />
</template>
