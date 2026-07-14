<script setup lang="ts">
import type { Connection } from '@vue-flow/core'
import { qualifiedKey } from '~/types/catalog'
import type { ComponentRecord } from '~/types/component'

/**
 * Collection adapter: binds the store-backed collection model + dependency-editing
 * rules to the presentational <GraphCanvas>. Keeps the original props/emits
 * so the graph page is unaffected.
 */
const props = withDefaults(defineProps<{
    sourceIds?: string[]
    readonly?: boolean
    expandMode?: ExpandMode
    viewMode?: ViewMode
    statusFilter?: StatusFilter
    showNewSourceButton?: boolean
    /** Asset id whose panel is open — the only highlighted node. */
    selectedId?: string | null
}>(), {
    sourceIds: undefined,
    readonly: false,
    expandMode: 'nodes',
    viewMode: 'topology',
    statusFilter: 'all',
    showNewSourceButton: true,
    selectedId: null,
})

const emit = defineEmits<{
    'add-source': []
    'edit-source': [sourceId: string]
    'asset-click': [asset: ComponentRecord, assetDefn: AssetDefinition | undefined, source: ComponentRecord | null]
    'pane-click': []
    'delete-source': [sourceId: string]
    'create-dependencies': [pairs: Array<{ upstreamAssetId: string; downstreamAssetId: string; paramName: string }>]
    'delete-dependency': [payload: { upstreamAssetId: string; downstreamAssetId: string }]
}>()

const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const { loading, dependencies: assetDependencies } = storeToRefs(componentsStore)

const { model } = useCollectionGraph({ sourceIds: () => props.sourceIds })

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

// Filtered model for display (the status filter narrows which sources render).
const displayModel = computed<GraphModel>(() => {
    const m = model.value
    if (props.statusFilter === 'all') return m

    const keep = new Set(
        m.sources
            .filter((s) => {
                const state = s.status?.state ?? 'idle'
                if (props.statusFilter === 'healthy') return state === 'idle'
                if (props.statusFilter === 'attention') return state === 'attention'
                if (props.statusFilter === 'paused') return state === 'paused'
                return true
            })
            .map(s => s.source.id),
    )

    return {
        sources: m.sources.filter(s => keep.has(s.source.id)),
        // Keep standalone assets (no source) regardless of the source filter.
        assets: m.assets.filter(a => a.source === null || keep.has(a.source.id)),
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
                 :expand-mode="expandMode"
                 :view-mode="viewMode"
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
