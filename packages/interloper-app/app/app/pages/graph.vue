<script setup lang="ts">
import { SplitterGroup, SplitterPanel, SplitterResizeHandle } from 'reka-ui'
import type { Source } from '~/types/source'

definePageMeta({ title: 'Graph', fullBleed: true })

const sourceStepperRef = ref<any>(null)

const {
    open: sourceDrawerOpen,
    editing: editingSource,
    openCreate: onCreateSource,
    openEdit: openEditSource,
} = useWizardDrawer<Source>()
const sourcesStore = useSourcesStore()
const assetsStore = useAssetsStore()
const jobsStore = useJobsStore()
const catalogStore = useCatalogStore()
const toast = useToast()

// Canvas view controls (toolbar-owned)
const expandMode = ref<ExpandMode>('nodes')
const viewMode = ref<ViewMode>('topology')
const statusFilter = ref<StatusFilter>('all')

const { sourceStatus } = useNodeStatus()
const statusCounts = computed<Record<StatusFilter, number>>(() => {
    const c: Record<StatusFilter, number> = { all: 0, healthy: 0, attention: 0, paused: 0 }
    for (const s of sourcesStore.sources) {
        c.all++
        const state = sourceStatus(s).state
        if (state === 'paused') c.paused++
        else if (state === 'attention') c.attention++
        else c.healthy++
    }
    return c
})

onMounted(() => {
    if (!sourcesStore.loading) sourcesStore.fetch()
    if (!assetsStore.loading) assetsStore.fetch()
    if (!jobsStore.loading) jobsStore.fetch()
})

function onEditSource(sourceId: string) {
    const source = sourcesStore.findById(sourceId)
    if (source) openEditSource(source)
}

function handleSaved() {
    sourcesStore.fetch()
    assetsStore.fetch()
    sourceDrawerOpen.value = false
}

const panelOpen = ref(false)
const closing = ref(false)
const panelVisible = computed(() => panelOpen.value || closing.value)
const selectedAsset = ref<SourceAsset | undefined>()
const selectedAssetDefn = ref<AssetDefinition | undefined>()
const selectedSource = ref<Source | undefined>()

function onAssetClick(asset: SourceAsset | Asset, assetDefn: AssetDefinition | undefined, source: Source | null) {
    // Panel is only shown for source-owned assets; standalone assets have no source.
    if (!source) return
    selectedAsset.value = asset as SourceAsset
    selectedAssetDefn.value = assetDefn
    selectedSource.value = source
    panelOpen.value = true
    closing.value = false
}

function onPanelClose() {
    closing.value = true
    panelOpen.value = false
}

function onCloseAnimationEnd() {
    closing.value = false
}

async function onDeleteSource(sourceId: string) {
    const source = sourcesStore.findById(sourceId)
    try {
        await sourcesStore.remove(sourceId)
        toast.add({ title: `Source "${source?.name ?? 'Source'}" deleted`, color: 'success' })
    }
    catch {
        toast.add({ title: 'Failed to delete source', color: 'error' })
    }
}

async function onCreateDependencies(pairs: Array<{ upstreamAssetId: string; downstreamAssetId: string; paramName: string }>) {
    try {
        await Promise.all(
            pairs.map(({ downstreamAssetId, upstreamAssetId, paramName }) =>
                assetsStore.addDependency(downstreamAssetId, upstreamAssetId, paramName),
            ),
        )
    }
    catch {
        toast.add({ title: 'Failed to create dependency', color: 'error' })
    }
}

async function onDeleteDependency(payload: { upstreamAssetId: string; downstreamAssetId: string }) {
    try {
        await assetsStore.removeDependency(payload.downstreamAssetId, payload.upstreamAssetId)
    }
    catch {
        toast.add({ title: 'Failed to delete dependency', color: 'error' })
    }
}
</script>

<template>
    <div class="flex flex-col flex-1 min-h-0">
        <GraphToolbar v-model:expand-mode="expandMode"
                      v-model:view-mode="viewMode"
                      v-model:status-filter="statusFilter"
                      :counts="statusCounts">
            <template #end>
                <UButton icon="i-lucide-plus"
                         label="New Source"
                         size="sm"
                         @click="onCreateSource" />
            </template>
        </GraphToolbar>
        <SplitterGroup direction="horizontal"
                       auto-save-id="graph-panels"
                       class="flex-1 min-h-0">
            <SplitterPanel :default-size="panelVisible ? 70 : 100"
                           :min-size="30"
                           class="flex">
                <GraphAssetGraph :expand-mode="expandMode"
                                 :view-mode="viewMode"
                                 :status-filter="statusFilter"
                                 :show-new-source-button="false"
                                 :selected-id="panelOpen ? selectedAsset?.id : null"
                                 @add-source="onCreateSource"
                                 @edit-source="onEditSource"
                                 @asset-click="onAssetClick"
                                 @delete-source="onDeleteSource"
                                 @create-dependencies="onCreateDependencies"
                                 @delete-dependency="onDeleteDependency"
                                 @pane-click="panelOpen && onPanelClose()" />
            </SplitterPanel>

            <template v-if="panelVisible">
                <SplitterResizeHandle
                                      class="relative flex items-center justify-center rounded-lg data-[state=hover]:bg-accented data-[state=drag]:bg-accented transition-colors" />

                <SplitterPanel :default-size="30"
                               :min-size="15"
                               class="relative overflow-hidden">
                    <GraphAssetPanel v-if="selectedAsset && selectedSource"
                                     :class="['absolute inset-0', closing ? 'animate-slide-right' : 'animate-slide-left']"
                                     :asset="selectedAsset"
                                     :asset-defn="selectedAssetDefn"
                                     :source="selectedSource"
                                     @close="onPanelClose"
                                     @animationend="closing && onCloseAnimationEnd()" />
                </SplitterPanel>
            </template>
        </SplitterGroup>

        <WizardDrawer v-model:open="sourceDrawerOpen"
                      default-title="New Source"
                      description="Configure source"
                      :stepper="sourceStepperRef">
            <SourcesStepper v-if="sourceDrawerOpen"
                            :key="editingSource?.id ?? 'new'"
                            ref="sourceStepperRef"
                            :source="editingSource"
                            @created="handleSaved"
                            @updated="handleSaved" />
        </WizardDrawer>
    </div>
</template>

<style scoped>
@keyframes slide-left {
    from {
        transform: translateX(100%);
    }

    to {
        transform: translateX(0);
    }
}

@keyframes slide-right {
    from {
        transform: translateX(0);
    }

    to {
        transform: translateX(100%);
    }
}

.animate-slide-left {
    animation: slide-left 0.20s ease-out;
}

.animate-slide-right {
    animation: slide-right 0.20s ease-out forwards;
}
</style>
