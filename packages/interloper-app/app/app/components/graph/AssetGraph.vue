<script setup lang="ts">
import type { Connection } from '@vue-flow/core'
import { qualifiedKey } from '~/types/catalog'

/**
 * Catalog adapter: binds the store-backed catalog model + dependency-editing
 * rules to the presentational <GraphCanvas>. Keeps the original props/emits
 * so the graph page is unaffected.
 */
const props = withDefaults(defineProps<{
    sourceIds?: string[]
    readonly?: boolean
    expandMode?: ExpandMode
    viewMode?: ViewMode
}>(), {
    sourceIds: undefined,
    readonly: false,
    expandMode: 'nodes',
    viewMode: 'topology',
})

const emit = defineEmits<{
    'add-source': []
    'edit-source': [sourceId: string]
    'asset-click': [asset: SourceAsset | Asset, assetDefn: AssetDefinition | undefined, source: Source | null]
    'pane-click': []
    'delete-source': [sourceId: string]
    'create-dependencies': [pairs: Array<{ upstreamAssetId: string; downstreamAssetId: string; paramName: string }>]
    'delete-dependency': [payload: { upstreamAssetId: string; downstreamAssetId: string }]
}>()

const assetsStore = useAssetsStore()
const catalogStore = useCatalogStore()
const sourcesStore = useSourcesStore()
const { loading } = storeToRefs(sourcesStore)
const { dependencies: assetDependencies } = storeToRefs(assetsStore)

const { model } = useCatalogGraph({ sourceIds: () => props.sourceIds })

// ── Maps for connection validation/creation (catalog editing only) ──
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
    <GraphCanvas :model="model"
                 :editable="!readonly"
                 :loading="loading"
                 :expand-mode="expandMode"
                 :view-mode="viewMode"
                 :is-valid-connection="isValidConnection"
                 @add-source="emit('add-source')"
                 @edit-source="emit('edit-source', $event)"
                 @asset-click="(a, d, s) => emit('asset-click', a, d, s)"
                 @delete-source="emit('delete-source', $event)"
                 @connect="onConnect"
                 @delete-dependency="emit('delete-dependency', $event)"
                 @pane-click="emit('pane-click')" />
</template>
