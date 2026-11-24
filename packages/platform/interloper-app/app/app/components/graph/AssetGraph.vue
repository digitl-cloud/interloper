<script setup lang="ts">
import type { ComponentRecord } from '~/types/component'

/**
 * Collection adapter: binds the store-backed collection model to the
 * presentational <GraphCanvas>. Upstream edges are the ones in effect, read
 * only: an asset follows every matching upstream of the organisation.
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
}>()

const componentsStore = useComponentsStore()
const { loading } = storeToRefs(componentsStore)

const { model } = useCollectionGraph({ sourceIds: () => props.sourceIds })
const { sourceStatus } = useNodeStatus()

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
</script>

<template>
    <GraphCanvas :model="displayModel"
                 :editable="!readonly"
                 :loading="loading"
                 :group-by="groupBy"
                 :show-new-source-button="showNewSourceButton"
                 :selected-id="selectedId"
                 @add-source="emit('add-source')"
                 @edit-source="emit('edit-source', $event)"
                 @asset-click="(a, d, s) => emit('asset-click', a, d, s)"
                 @delete-source="emit('delete-source', $event)"
                 @pane-click="emit('pane-click')" />
</template>
