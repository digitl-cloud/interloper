<script setup lang="ts">
import { SplitterGroup, SplitterPanel, SplitterResizeHandle } from 'reka-ui'
import type { ComponentRecord } from '~/types/component'
import type { DependencyPair } from '~/composables/graph'
import { qualifiedKey } from '~/types/catalog'

const sourceStepperRef = ref<any>(null)

const {
    open: sourceDrawerOpen,
    editing: editingSource,
    openCreate: onCreateSource,
    openEdit: openEditSource,
} = useWizardDrawer<ComponentRecord>()
const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const toast = useToast()

// Canvas view controls (toolbar-owned)
const groupBy = ref<GroupBy>('type')
const statusFilter = ref<StatusFilter>('all')

const { sourceStatus } = useNodeStatus()
const statusCounts = computed<Record<StatusFilter, number>>(() => {
    const c: Record<StatusFilter, number> = { all: 0, healthy: 0, attention: 0, paused: 0 }
    for (const s of componentsStore.byKind('source')) {
        c.all++
        const state = sourceStatus(s).state
        if (state === 'paused') c.paused++
        else if (state === 'attention') c.attention++
        else c.healthy++
    }
    return c
})

onMounted(() => {
    if (!componentsStore.loading) {
        componentsStore.fetchAll()
        componentsStore.fetchRelations()
    }
})

function onEditSource(sourceId: string) {
    const source = componentsStore.byId(sourceId)
    if (source) openEditSource(source)
}

function handleSaved() {
    componentsStore.fetchAll()
    componentsStore.fetchRelations()
    sourceDrawerOpen.value = false
}

const assetPane = useSlidingPane('graph-asset-pane-size')
const { pane: assetPaneRef, group: assetGroupRef, opened: panelOpen, mounted: panelMounted, animating: panelAnimating, contentStyle: assetPaneContentStyle, onLayout: onAssetPaneLayout } = assetPane
const selectedAsset = ref<ComponentRecord | undefined>()
const selectedAssetDefn = ref<AssetDefinition | undefined>()
const selectedSource = ref<ComponentRecord | undefined>()

function onAssetClick(asset: ComponentRecord, assetDefn: AssetDefinition | undefined, source: ComponentRecord | null) {
    // Panel is only shown for source-owned assets; standalone assets have no source.
    if (!source) return
    selectedAsset.value = asset
    selectedAssetDefn.value = assetDefn
    selectedSource.value = source
    assetPane.show()
}

// Deep link (command palette): /graph?select=<assetId> opens the asset panel
// once the asset and its source land in the store.
const route = useRoute()
const router = useRouter()
watchEffect(() => {
    const id = route.query.select
    if (typeof id !== 'string') return
    const asset = componentsStore.byId(id)
    if (!asset?.parent_id) return
    const source = componentsStore.byId(asset.parent_id)
    if (!source) return
    onAssetClick(asset, catalogStore.getAssetDefinition(qualifiedKey(source.key, asset.key)), source)
    router.replace({ query: { ...route.query, select: undefined } })
})

function onPanelClose() {
    assetPane.hide()
}

async function onDeleteSource(sourceId: string) {
    // Confirmation (and the in-use guard preview) happens in SourceNode before
    // this fires; the backend 409 remains the authority for races.
    const source = componentsStore.byId(sourceId)
    try {
        await componentsStore.remove(sourceId)
        toast.add({ title: `Source "${source?.name ?? 'Source'}" deleted`, color: 'success' })
    }
    catch (e) {
        toast.add(inUseToast(e, 'Source') ?? errorToast(e, 'Failed to delete source'))
    }
}

async function onCreateDependencies(pairs: DependencyPair[]) {
    try {
        await Promise.all(
            pairs.map(({ downstreamAssetId, upstreamAssetId, paramName, many }) =>
                componentsStore.addRelation(
                    downstreamAssetId,
                    { name: paramName, dst_id: upstreamAssetId },
                    { many },
                ),
            ),
        )
    }
    catch (e) {
        toast.add(errorToast(e, 'Failed to create dependency'))
    }
}

async function onDeleteDependency(payload: { upstreamAssetId: string; downstreamAssetId: string; name?: string }) {
    if (!payload.name) {
        toast.add({
            title: 'Failed to delete dependency',
            description: 'The relation was not found. Reload the page and try again.',
            color: 'error',
        })
        return
    }
    try {
        await componentsStore.removeRelation(payload.downstreamAssetId, payload.name, payload.upstreamAssetId)
    }
    catch (e) {
        toast.add(errorToast(e, 'Failed to delete dependency'))
    }
}
</script>

<template>
    <UDashboardPanel id="graph">
        <template #header>
            <AppNavbar title="Graph" />
        </template>
        <template #body>
            <GraphToolbar v-model:group-by="groupBy"
                          v-model:status-filter="statusFilter"
                          :counts="statusCounts">
                <template #actions>
                    <UButton icon="i-lucide-plus"
                             label="New source"
                             @click="onCreateSource" />
                </template>
            </GraphToolbar>
            <SplitterGroup ref="assetGroupRef"
                           direction="horizontal"
                           class="flex-1 min-h-0"
                           @layout="onAssetPaneLayout">
                <!-- p-px keeps the cards' outer rings inside the panes, which clip overflow -->
                <SplitterPanel :default-size="100"
                               :min-size="30"
                               class="flex p-px">
                    <UCard class="w-full"
                           :ui="CANVAS_CARD_UI">
                        <GraphAssetGraph :group-by="groupBy"
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
                    </UCard>
                </SplitterPanel>

                <!-- The page's panel gap, less the 1px each pane pads its card with so the ring isn't clipped. -->
                <SplitterResizeHandle class="group flex justify-center transition-[width] duration-200"
                                      :class="panelOpen ? 'w-3.5 sm:w-5.5' : 'w-0'"
                                      :disabled="!panelOpen || panelAnimating">
                    <div class="h-full w-0.5 transition-colors group-data-[state=hover]:bg-accented group-data-[state=drag]:bg-accented" />
                </SplitterResizeHandle>

                <SplitterPanel ref="assetPaneRef"
                               :default-size="0"
                               class="relative overflow-hidden">
                    <div v-if="panelMounted && selectedAsset && selectedSource"
                         class="absolute inset-y-0 right-0 p-px"
                         :style="assetPaneContentStyle">
                        <GraphAssetPanel class="h-full"
                                         :asset="selectedAsset"
                                         :asset-defn="selectedAssetDefn"
                                         :source="selectedSource"
                                         @close="onPanelClose" />
                    </div>
                </SplitterPanel>
            </SplitterGroup>

            <WizardDrawer v-model:open="sourceDrawerOpen"
                          default-title="New Source"
                          description="Configure source"
                          :stepper="sourceStepperRef">
                <SourcesWizard v-if="sourceDrawerOpen"
                                :key="editingSource?.id ?? 'new'"
                                ref="sourceStepperRef"
                                :source="editingSource"
                                @created="handleSaved"
                                @updated="handleSaved" />
            </WizardDrawer>
        </template>
    </UDashboardPanel>
</template>
