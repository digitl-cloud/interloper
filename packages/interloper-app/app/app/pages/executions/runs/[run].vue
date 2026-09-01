<script setup lang="ts">
import type { RunEvent } from '~/stores/events'
import type { Run } from '~/types/run'
import type { EventCategory } from '~/utils/events'
import { SplitterGroup, SplitterPanel, SplitterResizeHandle } from 'reka-ui'

// orgSwitchTarget: this page is bespoke to one org's run — switching org from
// the nav lands on the runs list instead.
definePageMeta({ title: 'Run', orgSwitchTarget: '/executions?tab=runs', fullBleed: true, customNavbar: true })

const route = useRoute()
const runId = route.params.run!.toString()

const runsStore = useRunsStore()
const eventsStore = useEventsStore()
const executionsStore = useExecutionsStore()
const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const toast = useToast()

const initialRun = ref<Run | null>(null)
const executions = computed(() => executionsStore.executions)

/** Prefer the store's copy (updated via realtime), fall back to initial fetch. */
const run = computed(() => runsStore.findById(runId) ?? initialRun.value)

const selectedAsset = ref<string | null>(null)
const statusFilter = ref<string | null>(null)
const eventInFocus = ref<RunEvent | null>(null)

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

const markerTime = computed(() => eventInFocus.value?.timestamp ? new Date(eventInFocus.value.timestamp) : null)
const highlightedAsset = computed(() => eventInFocus.value?.component_id ?? null)

const retrying = ref(false)

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
            // Sources/assets back the Graph view; jobs resolve the run's target in the summary.
            componentsStore.byKind('source').length === 0 || componentsStore.byKind('job').length === 0
                ? componentsStore.fetchAll(['source', 'asset', 'job'])
                : Promise.resolve(),
            // Asset dependency relations back the Graph view's edges.
            componentsStore.dependencies.length === 0 ? componentsStore.fetchRelations('dependency') : Promise.resolve(),
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

onUnmounted(() => {
    eventsStore.$reset()
    executionsStore.$reset()
})
</script>

<template>
    <OrganizationGate :org-id="run?.org_id"
             :error="fetchError"
             back-to="/executions?tab=runs"
             resource-label="run">
        <NavTitle>
            <ULink to="/executions?tab=runs"
                   class="text-[15px] font-medium text-muted hover:text-highlighted">Runs</ULink>
            <span class="text-[15px] text-dimmed">/</span>
            <span class="truncate font-mono text-[15px] font-semibold">{{ runId }}</span>
            <StatusPill v-if="run"
                        :label="statusLabel(run.status)"
                        :color="statusPillColor(run.status)" />
        </NavTitle>
        <NavActions v-if="run?.status === 'failed'">
            <UButton label="Retry failed"
                     icon="i-lucide-list-restart"
                     color="neutral"
                     variant="outline"
                     size="sm"
                     :loading="retrying"
                     @click="onRetry('failed')" />
            <UButton label="Retry all"
                     icon="i-lucide-rotate-ccw"
                     color="neutral"
                     variant="outline"
                     size="sm"
                     :loading="retrying"
                     @click="onRetry('all')" />
        </NavActions>
        <div class="flex flex-col h-full min-h-0">
            <div class="flex flex-col mb-4 shrink-0 px-4 pt-4">
                <ExecutionsRunSummary v-if="run"
                                      v-model:status-filter="statusFilter"
                                      :run="run"
                                      :executions="executions" />
            </div>

        <SplitterGroup direction="vertical"
                       auto-save-id="run-panels"
                       class="flex-1 min-h-0">
            <SplitterPanel :default-size="40"
                           :min-size="15"
                           class="flex flex-col overflow-hidden">
                <div class="flex flex-col flex-1 min-h-0 overflow-hidden">
                    <div class="flex items-center px-4 pt-4 pb-3 shrink-0">
                        <UTabs v-model="view"
                               :items="viewTabs"
                               variant="pill"
                               size="xs"
                               :content="false" />
                    </div>

                    <div class="flex min-h-0 flex-1 flex-col">
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
                    </div>
                </div>
            </SplitterPanel>

            <SplitterResizeHandle
                class="relative h-px shrink-0 cursor-ns-resize bg-(--ui-border) transition-colors data-[state=hover]:bg-primary/30 data-[state=drag]:bg-primary/30 before:absolute before:inset-x-0 before:-top-1.5 before:-bottom-1.5 before:z-1" />

            <SplitterPanel :default-size="60"
                           :min-size="20"
                           class="flex flex-col min-h-0">
                <div class="flex flex-col flex-1 min-h-0 overflow-hidden">
                    <div class="flex items-center gap-2 px-4 pt-4 pb-3 shrink-0">
                        <UTabs v-model="eventCategory"
                               :items="eventTabs"
                               variant="pill"
                               size="xs"
                               :content="false" />
                        <span v-if="!eventsStore.loading"
                              class="ml-auto text-[13.5px] text-muted">{{ eventCaption }}</span>
                    </div>
                    <div class="flex-1 min-h-0 px-4">
                        <ExecutionsEventsTable v-model:event-in-focus="eventInFocus"
                                               :events="eventsStore.events"
                                               :loading="eventsStore.loading"
                                               :loading-more="eventsStore.loadingMore"
                                               :has-more="eventsStore.hasMore"
                                               :load-more="eventsStore.loadMore" />
                    </div>
                </div>
            </SplitterPanel>
        </SplitterGroup>
        </div>
    </OrganizationGate>
</template>
