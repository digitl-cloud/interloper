<script setup lang="ts">
import type { SplitterItem } from '@nuxt/ui'
import type { ComponentRecord } from '~/types/component'
import type { AssetDefinition } from '~/types/catalog'

const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const runsStore = useRunsStore()
const appConfig = useAppConfig()

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
const { group: splitterRef, opened: panelOpen, mounted: panelMounted, animating: panelAnimating, contentStyle: assetPaneContentStyle, onLayout: onAssetPaneLayout } = assetPane
/** What USplitter exposes: its panels, the side one being the pane the sliding pane resizes. */
type SplitterHandle = { panelsRef: NonNullable<typeof assetPane.pane.value>[] }
// USplitter renders its panels itself, so the pane is read off the splitter rather than bound.
watchEffect(() => {
    assetPane.pane.value = (splitterRef.value as unknown as SplitterHandle | null)?.panelsRef[1] ?? null
})

const splitterItems: SplitterItem[] = [
    { slot: 'main', defaultSize: 100, minSize: 30, class: 'flex-col min-h-0 overflow-y-auto' },
    { slot: 'asset', defaultSize: 0, class: 'relative overflow-hidden' },
]
/** The handle is the page's panel gap, less the 1px the side pane pads its card with so the ring isn't clipped. */
const handleClass = computed(() => `flex justify-center transition-all duration-200 ${panelOpen.value ? 'w-3.5 sm:w-5.5' : 'w-0'}`)

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

function reload() {
    componentsStore.fetchAll()
    componentsStore.fetchRelations()
}

// Fired in setup so `loading` flags are set before the first render
// (gates the empty placeholder without a flash).
reload()
runsStore.fetch()
useExecutionsStore().fetchLatest()

function handleSaved() {
    reload()
    drawerOpen.value = false
}

function onEditSource(sourceId: string) {
    const source = componentsStore.byId(sourceId)
    if (source) openEditSource(source)
}

const showEmpty = computed(() => !componentsStore.loading && !componentsStore.error && componentsStore.byKind('source').length === 0)
</script>

<template>
    <UDashboardPanel id="collection"
                     :ui="{ body: 'p-0 sm:p-0 gap-0 sm:gap-0' }">
        <template #header>
            <AppNavbar title="Collection" />
        </template>
        <template #body>
            <div v-if="showEmpty"
                 class="min-h-0 flex-1 overflow-y-auto p-4 sm:p-6">
                <SourcesEmptyState @create="onCreateSource"
                                   @create-type="onCreateSourceFromCatalog" />
            </div>

            <USplitter v-else
                       id="collection-asset-pane"
                       ref="splitterRef"
                       :items="splitterItems"
                       :disabled="!panelOpen || panelAnimating"
                       :ui="{ root: 'min-h-0 flex-1', handle: handleClass }"
                       @layout="onAssetPaneLayout">
                <template #main>
                    <div class="flex min-h-0 flex-1 flex-col gap-4 p-4 transition-all duration-200 sm:gap-6 sm:p-6"
                         :class="panelOpen && 'pe-px sm:pe-px'">
                        <LoadError v-if="componentsStore.error"
                                   title="Couldn't load the collection"
                                   :error="componentsStore.error"
                                   @retry="reload" />
                        <DriftBanner />
                        <CollectionTable @edit-source="onEditSource"
                                         @view-asset="onViewAsset">
                            <template #actions>
                                <UButton :icon="appConfig.ui.icons.plus"
                                         label="New source"
                                         @click="onCreateSource" />
                                <UButton v-if="panelOpen"
                                         :icon="appConfig.icons.closePanel"
                                         aria-label="Hide panel"
                                         color="neutral"
                                         variant="ghost"
                                         @click="onPanelClose" />
                            </template>
                        </CollectionTable>
                    </div>
                </template>

                <template #resize-handle>
                    <div class="h-full w-0.5 transition-colors group-data-[state=hover]:bg-accented group-data-[state=drag]:bg-accented" />
                </template>

                <template #asset>
                    <div v-if="panelMounted && selectedAsset && selectedSource"
                         class="absolute inset-y-0 right-0 py-4 ps-px pe-4 sm:py-6 sm:pe-6"
                         :style="assetPaneContentStyle">
                        <GraphAssetPanel class="h-full"
                                         :asset="selectedAsset"
                                         :asset-defn="selectedAssetDefn"
                                         :source="selectedSource" />
                    </div>
                </template>
            </USplitter>

            <WizardDrawer v-model:open="drawerOpen"
                          default-title="New source"
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
