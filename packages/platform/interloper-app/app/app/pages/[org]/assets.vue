<script setup lang="ts">
import type { SplitterItem } from '@nuxt/ui'
import type { ComponentRecord } from '~/types/component'
import type { DependencyPair } from '~/composables/graph'
import { qualifiedKey } from '~/types/catalog'

/**
 * The organisation's assets, as their dependency graph or as a table by type
 * and source (`?view=table`). One toolbar narrows both views, and the selected
 * asset's pane stays open across them: the search, the sources' status, the
 * grouping and the selection all ride the URL.
 */

const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const runsStore = useRunsStore()
const executionsStore = useExecutionsStore()
const appConfig = useAppConfig()
const toast = useToast()

const { view } = usePageView(useAssetViews())
const query = useQueryParam('q', '')
const statusFilter = useQueryParam('status', 'all', oneOfQuery(STATUS_FILTERS))
const groupBy = useQueryParam('group', 'type', oneOfQuery(GROUP_BYS))
const selectedId = useQueryParam('asset', null)

// -- The collection, and what the filters keep of it ----------------------------

const { upstreams } = storeToRefs(componentsStore)
const { runs } = storeToRefs(runsStore)
const { latestByAssetId } = storeToRefs(executionsStore)
const { getWarnings } = useAssetWarnings()
const sources = computed(() => componentsStore.byKind('source'))
const { data: rows, sourceInfoById, typeInfoByKey, assetCount } = useCollectionRows({
    sources,
    upstreams,
    destinations: computed(() => componentsStore.byKind('destination')),
    jobs: computed(() => componentsStore.byKind('job')),
    runs,
    latestExecutions: latestByAssetId,
    getWarnings,
})

const { sourceStatus } = useNodeStatus()
const sourceStates = computed(() => new Map(sources.value.map(source => [source.id, sourceStatus(source).state])))
const statusCounts = computed(() => {
    const counts: Record<StatusFilter, number> = { all: 0, healthy: 0, attention: 0, paused: 0 }
    for (const state of sourceStates.value.values()) {
        for (const filter of STATUS_FILTERS) {
            if (matchesStatusFilter(state, filter)) counts[filter]++
        }
    }
    return counts
})

const matchingAssetIds = computed(() => searchAssets(rows.value, sourceInfoById.value, query.value))
const shownRows = computed(() => rows.value.filter(row =>
    matchesStatusFilter(sourceStates.value.get(row.sourceId) ?? 'idle', statusFilter.value)
    && (!matchingAssetIds.value || matchingAssetIds.value.has(row.assetId))))

function reload() {
    componentsStore.fetchAll()
    componentsStore.fetchRelations()
}

// Fired in setup so `loading` is set before the first render: the empty state never flashes.
reload()
runsStore.fetch()
executionsStore.fetchLatest()

const showEmpty = computed(() => !componentsStore.loading && !componentsStore.error && sources.value.length === 0)

// -- Sources: a new one only from the empty state, editing from either view -----

const sourceStepperRef = ref<any>(null)
const {
    open: drawerOpen,
    editing: editingSource,
    presetTypeKey,
    openCreate: onCreateSource,
    openCreateWithType: onCreateSourceFromCatalog,
    openEdit: openEditSource,
} = useWizardDrawer<ComponentRecord>()

function onEditSource(sourceId: string) {
    const source = componentsStore.byId(sourceId)
    if (source) openEditSource(source)
}

function handleSaved() {
    reload()
    drawerOpen.value = false
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
        toast.add(inUseToast(e, 'Source') ?? errorToast(e, "Couldn't delete the source"))
    }
}

// -- The graph's dependency editing ---------------------------------------------

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
        toast.add(errorToast(e, "Couldn't create the dependency"))
    }
}

async function onDeleteDependency(payload: { upstreamAssetId: string; downstreamAssetId: string; name?: string }) {
    if (!payload.name) {
        toast.add({
            title: "Couldn't delete the dependency",
            description: 'The relation was not found. Reload the page and try again.',
            color: 'error',
        })
        return
    }
    try {
        await componentsStore.removeRelation(payload.downstreamAssetId, payload.name, payload.upstreamAssetId)
    }
    catch (e) {
        toast.add(errorToast(e, "Couldn't delete the dependency"))
    }
}

// -- The selected asset's pane, shared by both views ----------------------------

const assetPane = useSlidingPane('assets-pane-size')
const { group: splitterRef, opened: panelOpen, mounted: panelMounted, animating: panelAnimating, wide, contentStyle: assetPaneContentStyle, onLayout: onAssetPaneLayout } = assetPane
/** What USplitter exposes: its panels, the side one being the pane the sliding pane resizes. */
type SplitterHandle = { panelsRef: NonNullable<typeof assetPane.pane.value>[] }
// USplitter renders its panels itself, so the pane is read off the splitter rather than bound.
watchEffect(() => {
    assetPane.pane.value = (splitterRef.value as unknown as SplitterHandle | null)?.panelsRef[1] ?? null
})

// p-px keeps the cards' outer rings inside the panes, which clip overflow.
const splitterItems = computed<SplitterItem[]>(() => [
    { slot: 'main', defaultSize: 100, minSize: wide.value ? 30 : 0, class: 'flex flex-col overflow-hidden p-px' },
    { slot: 'asset', defaultSize: 0, class: 'relative overflow-hidden' },
])
/** The handle is the page's panel gap, less the 1px each pane pads its card with so the ring isn't clipped. */
const handleClass = computed(() => `flex justify-center transition-all duration-200 ${panelOpen.value && wide.value ? 'w-3.5 sm:w-5.5' : 'w-0'}`)

/** The selected asset with its source and definition: only a source's assets open the pane. */
const selection = computed(() => {
    const asset = selectedId.value ? componentsStore.byId(selectedId.value) : undefined
    const source = asset?.parent_id ? componentsStore.byId(asset.parent_id) : undefined
    if (!asset || !source) return null
    return { asset, source, definition: catalogStore.getAssetDefinition(qualifiedKey(source.key, asset.key)) }
})

/** What the pane shows: the selection, kept while the pane slides shut once it is cleared. */
const shown = shallowRef<NonNullable<typeof selection.value> | null>(null)
watch(selection, (next) => {
    if (next) shown.value = next
}, { immediate: true })

const selectedAssetId = computed(() => selection.value?.asset.id ?? null)
watch(selectedAssetId, id => (id ? assetPane.show() : assetPane.hide()))
onMounted(() => {
    if (selectedAssetId.value) assetPane.show()
})

function onAssetClick(asset: ComponentRecord) {
    selectedId.value = asset.id
}

function closePane() {
    selectedId.value = null
}
</script>

<template>
    <UDashboardPanel id="assets">
        <template #header>
            <AppNavbar title="Assets" />
        </template>
        <template #body>
            <SourcesEmptyState v-if="showEmpty"
                               @create="onCreateSource"
                               @create-type="onCreateSourceFromCatalog" />

            <template v-else>
                <LoadError v-if="componentsStore.error"
                           title="Couldn't load the assets"
                           :error="componentsStore.error"
                           @retry="reload" />
                <DriftBanner />
                <AssetsToolbar v-model:view="view"
                               v-model:query="query"
                               v-model:group-by="groupBy"
                               v-model:status-filter="statusFilter"
                               :counts="statusCounts" />

                <USplitter id="assets-pane"
                           ref="splitterRef"
                           :items="splitterItems"
                           :disabled="!panelOpen || panelAnimating || !wide"
                           :ui="{ root: 'min-h-0 flex-1', handle: handleClass }"
                           @layout="onAssetPaneLayout">
                    <template #main>
                        <CollectionTable v-if="view === 'table'"
                                         :rows="shownRows"
                                         :source-info-by-id="sourceInfoById"
                                         :type-info-by-key="typeInfoByKey"
                                         :asset-count="assetCount"
                                         :group-by="groupBy"
                                         @edit-source="onEditSource"
                                         @view-asset="selectedId = $event" />
                        <UCard v-else
                               class="w-full"
                               :ui="CANVAS_CARD_UI">
                            <GraphAssetGraph :group-by="groupBy"
                                             :status-filter="statusFilter"
                                             :asset-ids="matchingAssetIds"
                                             :show-new-source-button="false"
                                             :selected-id="panelOpen ? shown?.asset.id : null"
                                             @add-source="onCreateSource"
                                             @edit-source="onEditSource"
                                             @asset-click="onAssetClick"
                                             @delete-source="onDeleteSource"
                                             @create-dependencies="onCreateDependencies"
                                             @delete-dependency="onDeleteDependency"
                                             @pane-click="panelOpen && closePane()" />
                        </UCard>
                    </template>

                    <template #resize-handle>
                        <div class="h-full w-0.5 transition-colors group-data-[state=hover]:bg-accented group-data-[state=drag]:bg-accented" />
                    </template>

                    <template #asset>
                        <div v-if="panelMounted && shown"
                             class="absolute inset-y-0 right-0 p-px"
                             :style="assetPaneContentStyle">
                            <GraphAssetPanel class="h-full"
                                             :asset="shown.asset"
                                             :asset-defn="shown.definition"
                                             :source="shown.source">
                                <template #actions>
                                    <UTooltip text="Hide panel">
                                        <UButton :icon="appConfig.icons.closePanel"
                                                 aria-label="Hide panel"
                                                 color="neutral"
                                                 variant="ghost"
                                                 size="sm"
                                                 @click="closePane" />
                                    </UTooltip>
                                </template>
                            </GraphAssetPanel>
                        </div>
                    </template>
                </USplitter>
            </template>

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
