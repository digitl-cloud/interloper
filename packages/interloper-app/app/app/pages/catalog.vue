<script setup lang="ts">
import { SplitterGroup, SplitterPanel, SplitterResizeHandle } from 'reka-ui'
import type { Source, SourceAsset } from '~/types/source'
import type { AssetDefinition } from '~/types/catalog'

definePageMeta({ title: 'Catalog' })

const sourcesStore = useSourcesStore()
const assetsStore = useAssetsStore()
const catalogStore = useCatalogStore()
const jobsStore = useJobsStore()
const runsStore = useRunsStore()

const sourceStepperRef = ref<any>(null)

const {
    open: drawerOpen,
    editing: editingSource,
    presetTypeKey,
    openCreate: onCreateSource,
    openCreateWithType: onCreateSourceFromCatalog,
    openEdit: openEditSource,
} = useWizardDrawer<Source>()

// ── Asset panel ────────────────────────────────────────────────
const panelOpen = ref(false)
const closing = ref(false)
const panelVisible = computed(() => panelOpen.value || closing.value)
const selectedAsset = ref<SourceAsset | undefined>()
const selectedAssetDefn = ref<AssetDefinition | undefined>()
const selectedSource = ref<Source | undefined>()

function getAssetDefinition(key: string): AssetDefinition | undefined {
    for (const src of catalogStore.sourceDefinitions) {
        const asset = src.assets?.find(a => a.key === key)
        if (asset) return asset
    }
    return undefined
}

function onViewAsset(assetId: string, sourceId: string) {
    const source = sourcesStore.findById(sourceId)
    if (!source) return
    const asset = source.assets.find(a => a.id === assetId)
    if (!asset) return

    selectedSource.value = source
    selectedAsset.value = asset
    selectedAssetDefn.value = getAssetDefinition(asset.key)
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

// ── Data fetching ──────────────────────────────────────────────

// Fired in setup so `loading` flags are set before the first render
// (gates the empty placeholder without a flash).
sourcesStore.fetch()
assetsStore.fetch()
jobsStore.fetch()
runsStore.fetch()
if (!catalogStore.loaded) catalogStore.fetchCatalog()

function handleSaved() {
    sourcesStore.fetch()
    assetsStore.fetch()
    jobsStore.fetch()
    drawerOpen.value = false
}

function onEditSource(sourceId: string) {
    const source = sourcesStore.findById(sourceId)
    if (source) openEditSource(source)
}

const showEmpty = computed(() => !sourcesStore.loading && sourcesStore.sources.length === 0)
</script>

<template>
    <div class="flex flex-col min-h-0 flex-1">
        <DriftBanner />

        <div v-if="showEmpty"
             class="w-full max-w-[1040px] mx-auto">
            <SourcesEmptyState @create="onCreateSource"
                               @create-type="onCreateSourceFromCatalog" />
        </div>

        <SplitterGroup v-else
                       direction="horizontal"
                       auto-save-id="catalog-panels"
                       class="flex-1 min-h-0">
            <SplitterPanel :default-size="panelVisible ? 70 : 100"
                           :min-size="30"
                           class="flex flex-col min-h-0">
                <CatalogTable @create="onCreateSource"
                              @edit-source="onEditSource"
                              @view-asset="onViewAsset" />
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

        <WizardDrawer v-model:open="drawerOpen"
                      default-title="New Source"
                      description="Configure source"
                      :stepper="sourceStepperRef">
            <SourcesStepper v-if="drawerOpen"
                            :key="editingSource?.id ?? 'new'"
                            ref="sourceStepperRef"
                            :source="editingSource"
                            :initial-type-key="presetTypeKey"
                            @created="handleSaved"
                            @updated="handleSaved" />
        </WizardDrawer>
    </div>
</template>

<style scoped>
@keyframes slide-left {
    from { transform: translateX(100%); }
    to { transform: translateX(0); }
}

@keyframes slide-right {
    from { transform: translateX(0); }
    to { transform: translateX(100%); }
}

.animate-slide-left {
    animation: slide-left 0.20s ease-out;
}

.animate-slide-right {
    animation: slide-right 0.20s ease-out forwards;
}
</style>
