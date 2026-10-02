<script setup lang="ts">
import type { RunEvent } from '~/stores/events'
import type { Run } from '~/types/run'
import type { SplitterItem } from '@nuxt/ui'
import type { EventCategory } from '~/utils/events'

// orgSwitchTarget: this page is bespoke to one org's run — switching org from
// the nav lands on the runs list instead.
definePageMeta({ orgSwitchTarget: '/executions/runs' })

const route = useRoute()
const runId = route.params.run!.toString()

const runsStore = useRunsStore()
const eventsStore = useEventsStore()
const executionsStore = useExecutionsStore()
const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const toast = useToast()
const appConfig = useAppConfig()

const initialRun = ref<Run | null>(null)
const executions = computed(() => executionsStore.executions)

/** Prefer the store's copy (updated via realtime), fall back to initial fetch. */
const run = computed(() => runsStore.findById(runId) ?? initialRun.value)

const selectedAsset = ref<string | null>(null)
const statusFilter = ref<string | null>(null)
const eventInFocus = ref<RunEvent | null>(null)
/** Asset under the pointer in the rail, mirrored in the timeline and the graph. */
const hoveredAsset = ref<string | null>(null)

const stats = useRunStats(run, executions)
const attempts = useRunAttempts(run, executions)
/** Every asset of this attempt: the rail lists them all, whichever filter is active. */
const assetRows = useExecutionRows(executions)

/** Execution statuses behind the active status pill (e.g. pending → pending+queued). */
const filterStatuses = computed(() => statusFilter.value ? statusesForKey(statusFilter.value) : null)

/** Timeline rows, narrowed to the active status pill. */
const timelineRows = useExecutionRows(() => {
    const statuses = filterStatuses.value
    if (!statuses) return executions.value
    return executions.value.filter(e => statuses.includes(e.status))
})

// Selecting a single asset narrows to it; otherwise the active status pill's
// asset set drives the filter. Events are paged from the server, so the filter
// is applied there (re-paged from offset 0) rather than over the loaded pages.
const eventAssetIds = computed<string[] | null>(() => {
    if (selectedAsset.value) return [selectedAsset.value]
    const statuses = filterStatuses.value
    if (!statuses) return null
    return executions.value
        .filter(e => statuses.includes(e.status) && e.component_id)
        .map(e => e.component_id!)
})
watch(eventAssetIds, ids => eventsStore.filterByComponents(ids))

// Switching the status pill clears any single-asset drill-down.
watch(statusFilter, () => { selectedAsset.value = null })

// Event category tab (All / Lifecycle / Errors / Logs), filtered server-side.
const eventCategory = ref<EventCategory>('all')
const eventTabs = [
    { value: 'all', label: 'All', icon: 'i-lucide-list' },
    { value: 'lifecycle', label: 'Lifecycle', icon: 'i-lucide-activity' },
    { value: 'errors', label: 'Errors', icon: 'i-lucide-circle-alert' },
    { value: 'logs', label: 'Logs', icon: 'i-lucide-scroll-text' },
]
watch(eventCategory, cat => eventsStore.filterByEventTypes(eventTypesForCategory(cat)))

// Top-panel view: the Gantt timeline or the run dependency graph.
const view = ref<'timeline' | 'graph'>('timeline')
const viewTabs = [
    { value: 'timeline', label: 'Timeline', icon: 'i-lucide-gantt-chart' },
    { value: 'graph', label: 'Graph', icon: 'i-lucide-workflow' },
]

/** Design's table caption for the events panel, honest about server paging. */
const eventCaption = computed(() => {
    const loaded = eventsStore.events.length
    if (eventsStore.hasMore) return `${loaded} of ${eventsStore.total} events`
    return `${loaded} event${loaded === 1 ? '' : 's'}`
})

/** A hovered event marks its own instant; a hovered rail asset marks when it started. */
const markerTime = computed(() => {
    if (eventInFocus.value?.timestamp) return new Date(eventInFocus.value.timestamp)
    const start = assetRows.value.find(row => row.id === hoveredAsset.value)?.bars[0]?.start
    return start ? new Date(start) : null
})
const highlightedAsset = computed(() => hoveredAsset.value ?? eventInFocus.value?.component_id ?? null)

// Panes clip overflow, so each keeps a 1px inset for its card's ring.
const railItems: SplitterItem[] = [
    { slot: 'rail', sizeUnit: 'px', defaultSize: 268, minSize: 200, maxSize: 480, collapsible: true, collapsedSize: 0, class: 'overflow-hidden p-px' },
    { slot: 'main', class: 'min-w-0' },
]
const panelItems: SplitterItem[] = [
    { slot: 'timeline', defaultSize: 40, minSize: 15, class: 'flex-col overflow-hidden p-px' },
    { slot: 'events', defaultSize: 60, minSize: 20, class: 'flex-col overflow-hidden p-px' },
]
/** The handles are the 12px gaps between the cards, with a line lit while hovered or dragged. */
const HANDLE_LINE = 'relative bg-transparent before:absolute before:bg-transparent before:transition-colors data-[state=hover]:before:bg-accented data-[state=drag]:before:bg-accented'
const PANELS_HANDLE_CLASS = `${HANDLE_LINE} h-3 before:inset-x-0 before:top-1/2 before:h-px`

type SplitterPanelHandle = { collapse: () => void, expand: () => void, isCollapsed: boolean }
const railSplitter = useTemplateRef<{ panelsRef: SplitterPanelHandle[] }>('railSplitter')
const railPanel = computed(() => railSplitter.value?.panelsRef[0])
/** Read off the panel: a collapse restored from the saved layout emits no event. */
const railCollapsed = computed(() => railPanel.value?.isCollapsed ?? false)
const railHandleClass = computed(() => `${HANDLE_LINE} before:inset-y-0 before:left-1/2 before:w-px ${railCollapsed.value ? 'w-0' : 'w-3'}`)

function toggleRail() {
    if (railCollapsed.value) railPanel.value?.expand()
    else railPanel.value?.collapse()
}

const { mismatch } = useOrgGate(() => run.value?.org_id)

const retrying = ref(false)

// A retry continues the stack from its latest attempt, so only that attempt
// offers one; an attempt whose stack has not loaded yet is treated as latest.
const retryable = computed(() => {
    if (mismatch.value || run.value?.status !== 'failed') return false
    const latest = attempts.value.at(-1)?.run
    return !latest || latest.id === run.value.id
})

async function onRetry(scope: 'all' | 'failed') {
    retrying.value = true
    try {
        const newRunId = await runsStore.retryRun(runId, scope)
        toast.add({ title: `Retry queued (${newRunId.slice(0, 8)})`, color: 'success' })
        await navigateTo(`/executions/runs/${newRunId}`)
    }
    catch (e) {
        toast.add(errorToast(e, 'Failed to queue retry'))
    }
    finally {
        retrying.value = false
    }
}

const fetchError = ref<unknown>(null)

onMounted(async () => {
    try {
        const [fetchedRun] = await Promise.all([
            runsStore.fetchOne(runId),
            eventsStore.fetchForRun(runId),
            executionsStore.fetchForRun(runId),
            // Sources/assets back the Graph view (the run's target rides the run itself).
            componentsStore.byKind('source').length === 0
                ? componentsStore.fetchAll(['source', 'asset'])
                : Promise.resolve(),
            // Asset-to-asset relations back the Graph view's edges; the store
            // derives them from the whole set (they carry no name of their own).
            componentsStore.upstreams.length === 0 ? componentsStore.fetchRelations() : Promise.resolve(),
            catalogStore.loaded ? Promise.resolve() : catalogStore.fetchCatalog(),
        ])
        initialRun.value = fetchedRun
        // Seed the store so realtime updates can find and update it.
        runsStore._upsert(fetchedRun)
    }
    catch (e) {
        fetchError.value = e
    }
})

// Navigating between attempts mounts the next run's page before this one
// unmounts, so only clear stores that still hold this run.
onUnmounted(() => {
    if (eventsStore.runId === runId) eventsStore.$reset()
    if (executionsStore.runId === runId) executionsStore.$reset()
})
</script>

<template>
    <UDashboardPanel id="run">
        <template #header>
            <AppNavbar>
                <template #title>
                    <ULink to="/executions/runs"
                           class="text-base font-medium text-muted hover:text-highlighted">Runs</ULink>
                    <span class="text-base text-dimmed">/</span>
                    <span class="truncate font-mono text-base font-semibold">{{ runId }}</span>
                    <StatusPill v-if="run && !mismatch"
                                :label="statusLabel(run.status)"
                                :color="statusPillColor(run.status)"
                                :spinner="run.status === 'running' || run.status === 'dispatched'" />
                </template>
            </AppNavbar>
            <UDashboardToolbar v-if="retryable">
                <template #right>
                    <UButton label="Retry failed"
                             icon="i-lucide-rotate-ccw"
                             color="neutral"
                             variant="ghost"
                             :loading="retrying"
                             @click="onRetry('failed')" />
                    <UButton label="Retry all"
                             icon="i-lucide-refresh-cw"
                             color="neutral"
                             variant="ghost"
                             :loading="retrying"
                             @click="onRetry('all')" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <OrganizationGate :org-id="run?.org_id"
                              :error="fetchError"
                              back-to="/executions/runs"
                              resource-label="run">
                <div class="flex min-h-0 flex-1 flex-col gap-3">
                    <UCard v-if="run"
                           :ui="{ body: 'flex flex-col gap-4' }">
                        <ExecutionsRunMetaStrip :run="run"
                                                :duration="stats.duration" />
                        <ExecutionsRunStatusBar v-model:status-filter="statusFilter"
                                                :stats="stats" />
                    </UCard>

                    <USplitter id="run-rail"
                               ref="railSplitter"
                               auto-save-id="run-rail"
                               :items="railItems"
                               :ui="{ root: 'min-h-0 flex-1', handle: railHandleClass }">
                        <template #rail="{ collapsed }">
                            <UCard v-if="!collapsed"
                                   class="h-full w-full min-w-0"
                                   :ui="{ root: 'flex flex-col', body: 'min-h-0 flex-1 overflow-y-auto p-3 sm:p-3' }">
                                <ExecutionsRunRail v-model:status-filter="statusFilter"
                                                   v-model:selected="selectedAsset"
                                                   v-model:hovered="hoveredAsset"
                                                   :rows="assetRows"
                                                   :buckets="stats.buckets"
                                                   :attempts="attempts"
                                                   :current-run-id="runId" />
                            </UCard>
                        </template>

                        <template #main>
                            <USplitter id="run-panels"
                                       orientation="vertical"
                                       auto-save-id="run-panels"
                                       :items="panelItems"
                                       :ui="{ root: 'min-h-0 min-w-0 flex-1', handle: PANELS_HANDLE_CLASS }">
                                <template #timeline>
                                    <UCard :ui="CANVAS_CARD_UI">
                                        <template #header>
                                            <div class="flex items-center gap-2">
                                                <UButton :icon="railCollapsed ? appConfig.ui.icons.panelOpen : appConfig.ui.icons.panelClose"
                                                         :aria-label="railCollapsed ? 'Show panel' : 'Hide panel'"
                                                         color="neutral"
                                                         variant="ghost"
                                                         size="sm"
                                                         class="-ml-1.5"
                                                         @click="toggleRail" />
                                                <UTabs v-model="view"
                                                       :items="viewTabs"
                                                       variant="pill"
                                                       size="xs"
                                                       :content="false" />
                                            </div>
                                        </template>
                                        <div v-if="run?.status === 'queued'"
                                             class="flex h-full items-center justify-center text-muted">
                                            <span class="text-sm">Run is currently queued...</span>
                                        </div>
                                        <ChartExecutionTimeline v-else-if="view === 'timeline'"
                                                                v-model:selected-id="selectedAsset"
                                                                :rows="timelineRows"
                                                                :min-bar-ratio="0.05"
                                                                :marker-time="markerTime"
                                                                :highlighted-id="highlightedAsset"
                                                                empty-message="No asset executions yet" />
                                        <ExecutionsRunGraph v-else
                                                            v-model:selected-asset="selectedAsset"
                                                            :run-id="runId" />
                                    </UCard>
                                </template>

                                <template #events>
                                    <UCard :ui="{ ...FILL_CARD_UI, header: 'shrink-0' }">
                                        <template #header>
                                            <div class="flex items-center gap-2">
                                                <UTabs v-model="eventCategory"
                                                       :items="eventTabs"
                                                       variant="pill"
                                                       size="xs"
                                                       :content="false" />
                                                <span v-if="!eventsStore.loading"
                                                      class="ml-auto text-sm text-muted">{{ eventCaption }}</span>
                                            </div>
                                        </template>
                                        <ExecutionsEventsTable v-model:event-in-focus="eventInFocus"
                                                               :events="eventsStore.events"
                                                               :loading="eventsStore.loading"
                                                               :loading-more="eventsStore.loadingMore"
                                                               :has-more="eventsStore.hasMore"
                                                               :load-more="eventsStore.loadMore" />
                                    </UCard>
                                </template>
                            </USplitter>
                        </template>
                    </USplitter>
                </div>
            </OrganizationGate>
        </template>
    </UDashboardPanel>
</template>
