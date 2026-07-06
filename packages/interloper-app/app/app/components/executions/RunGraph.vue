<script setup lang="ts">
import type { ComponentRecord } from '~/types/component'
import type { GraphModel } from '~/types/graph'

/**
 * Run dependency graph: the run's assets laid out as a left-to-right DAG,
 * coloured by live execution status. Reuses the catalog <GraphCanvas> via the
 * run model seam; read-only (no dependency editing). Clicking a node selects
 * that asset, which the page mirrors to the events filter.
 */
const props = defineProps<{ runId: string }>()
const selectedAsset = defineModel<string | null>('selectedAsset')

const { model } = useRunGraph(() => props.runId)
const assetExecutionsStore = useAssetExecutionsStore()

/** asset_id → formatted run duration, shown on each node. */
const durationByAssetId = computed(() => {
    const map = new Map<string, string>()
    for (const ex of assetExecutionsStore.assetExecutions) {
        if (ex.asset_id && ex.started_at) map.set(ex.asset_id, formatElapsed(ex.started_at, ex.completed_at))
    }
    return map
})

/**
 * Flatten to a plain asset DAG: a run cares about the asset dependency flow,
 * not source grouping, so every asset renders as a top-level node (no source
 * container cards) coloured by its execution status, with its duration.
 */
const flatModel = computed<GraphModel>(() => ({
    sources: [],
    assets: model.value.assets.map(a => ({
        ...a,
        source: null,
        status: a.status ? { ...a.status, label: durationByAssetId.value.get(a.asset.id) } : undefined,
    })),
    dependencies: model.value.dependencies,
}))

function onAssetClick(asset: ComponentRecord) {
    selectedAsset.value = selectedAsset.value === asset.id ? null : asset.id
}
</script>

<template>
    <GraphCanvas :model="flatModel"
                 view-mode="status"
                 direction="LR"
                 compact
                 fit-to-content
                 :show-new-source-button="false"
                 :selected-id="selectedAsset ?? null"
                 @asset-click="onAssetClick"
                 @pane-click="selectedAsset = null" />
</template>
