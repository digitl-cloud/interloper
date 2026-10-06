<script setup lang="ts">
import { SplitterGroup, SplitterPanel, SplitterResizeHandle } from 'reka-ui'
import type { ComponentRecord } from '~/types/component'
import type { AssetDefinition } from '~/types/catalog'

const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const runsStore = useRunsStore()

const sourceStepperRef = ref<any>(null)

const {
    open: drawerOpen,
    editing: editingSource,
    presetTypeKey,
    openCreate: onCreateSource,
    openCreateWithType: onCreateSourceFromCatalog,
    openEdit: openEditSource,
} = useWizardDrawer<ComponentRecord>()

// ── Asset panel ────────────────────────────────────────────────
const assetPane = useSlidingPane('collection-asset-pane-size')
const { pane: assetPaneRef, group: assetGroupRef, opened: panelOpen, mounted: panelMounted, animating: panelAnimating, contentStyle: assetPaneContentStyle, onLayout: onAssetPaneLayout } = assetPane
const selectedAsset = ref<ComponentRecord | undefined>()
const selectedAssetDefn = ref<AssetDefinition | undefined>()
const selectedSource = ref<ComponentRecord | undefined>()

function getAssetDefinition(key: string): AssetDefinition | undefined {
    for (const src of catalogStore.sourceDefinitions) {
        const asset = src.assets?.find(a => a.key === key)
        if (asset) return asset
    }
    return undefined
}

function onViewAsset(assetId: string, sourceId: string) {
    const source = componentsStore.byId(sourceId)
    if (!source) return
    const asset = source.children.find(a => a.id === assetId)
    if (!asset) return

    selectedSource.value = source
    selectedAsset.value = asset
    selectedAssetDefn.value = getAssetDefinition(asset.key)
    assetPane.show()
}

function onPanelClose() {
    assetPane.hide()
}

// ── Data fetching ──────────────────────────────────────────────

// Fired in setup so `loading` flags are set before the first render
// (gates the empty placeholder without a flash).
componentsStore.fetchAll()
componentsStore.fetchRelations()
runsStore.fetch()
useExecutionsStore().fetchLatest()
if (!catalogStore.loaded) catalogStore.fetchCatalog()

function handleSaved() {
    componentsStore.fetchAll()
    componentsStore.fetchRelations()
    drawerOpen.value = false
}

function onEditSource(sourceId: string) {
    const source = componentsStore.byId(sourceId)
    if (source) openEditSource(source)
}

const showEmpty = computed(() => !componentsStore.loading && componentsStore.byKind('source').length === 0)
</script>

<template>
    <UDashboardPanel id="collection"
                     :ui="{ body: 'p-0 sm:p-0 gap-0 sm:gap-0' }">
        <template #header>
            <AppNavbar title="Collection" />
        </template>
        <template #body>
            <div v-if="showEmpty"
                 class="flex-1 min-h-0 overflow-y-auto">
                <div class="p-4 sm:p-6 w-full max-w-[1040px] mx-auto">
                    <SourcesEmptyState @create="onCreateSource"
                                       @create-type="onCreateSourceFromCatalog" />
                </div>
            </div>

            <SplitterGroup v-else
                           ref="assetGroupRef"
                           direction="horizontal"
                           class="flex-1 min-h-0"
                           @layout="onAssetPaneLayout">
                <SplitterPanel :default-size="100"
                               :min-size="30"
                               class="flex flex-col min-h-0 overflow-y-auto">
                    <div class="flex flex-col gap-4 p-4 sm:p-6 min-h-0 flex-1 transition-[padding] duration-200"
                         :class="panelOpen && 'pe-px sm:pe-px'">
                        <DriftBanner />
                        <CollectionTable @edit-source="onEditSource"
                                         @view-asset="onViewAsset">
                            <template #actions>
                                <UButton icon="i-lucide-plus"
                                         label="New source"
                                         @click="onCreateSource" />
                            </template>
                        </CollectionTable>
                    </div>
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
                         class="absolute inset-y-0 right-0 py-4 pe-4 ps-px sm:py-6 sm:pe-6"
                         :style="assetPaneContentStyle">
                        <GraphAssetPanel class="h-full"
                                         :asset="selectedAsset"
                                         :asset-defn="selectedAssetDefn"
                                         :source="selectedSource"
                                         @close="onPanelClose" />
                    </div>
                </SplitterPanel>
            </SplitterGroup>

            <WizardDrawer v-model:open="drawerOpen"
                          default-title="New Source"
                          description="Configure source"
                          :stepper="sourceStepperRef">
                <SourcesWizard v-if="drawerOpen"
                                :key="editingSource?.id ?? 'new'"
                                ref="sourceStepperRef"
                                :source="editingSource"
                                :initial-type-key="presetTypeKey"
                                @created="handleSaved"
                                @updated="handleSaved" />
            </WizardDrawer>
        </template>
    </UDashboardPanel>
</template>
