<script setup lang="ts">
import type { RunEvent } from '~/stores/events'
import type { Run } from '~/types/run'
import type { ExecutionStatus } from '~/types/asset_execution'
import type { EventCategory } from '~/utils/events'
import { SplitterGroup, SplitterPanel, SplitterResizeHandle } from 'reka-ui'

// orgSwitchTarget: this page is bespoke to one org's run — switching org from
// the nav lands on the runs list instead.
definePageMeta({ title: 'Run', orgSwitchTarget: '/executions?tab=runs', fullBleed: true })

const route = useRoute()
const runId = route.params.run!.toString()

const runsStore = useRunsStore()
const eventsStore = useEventsStore()
const assetExecutionsStore = useAssetExecutionsStore()
const sourcesStore = useSourcesStore()
const assetsStore = useAssetsStore()
const catalogStore = useCatalogStore()
const toast = useToast()

const initialRun = ref<Run | null>(null)
const assetExecutions = computed(() => assetExecutionsStore.assetExecutions)

/** Prefer the store's copy (updated via realtime), fall back to initial fetch. */
const run = computed(() => runsStore.findById(runId) ?? initialRun.value)

const selectedAsset = ref<string | null>(null)
const statusFilter = ref<string | null>(null)
const eventInFocus = ref<RunEvent | null>(null)

/** Execution statuses behind the active status pill (e.g. pending → pending+queued). */
const filterStatuses = computed(() => statusFilter.value ? statusesForKey(statusFilter.value) : null)

/** Timeline rows, narrowed to the active status pill. */
const filteredAssetExecutions = computed(() => {
    const statuses = filterStatuses.value
    if (!statuses) return assetExecutions.value
    return assetExecutions.value.filter(e => statuses.includes(e.status))
})

// Selecting a single asset narrows to it; otherwise the active status pill's
// asset set drives the filter. Events are paged from the server, so the filter
// is applied there (re-paged from offset 0) rather than over the loaded pages.
const eventAssetIds = computed<string[] | null>(() => {
    if (selectedAsset.value) return [selectedAsset.value]
    const statuses = filterStatuses.value
    if (!statuses) return null
    return assetExecutions.value
        .filter(e => statuses.includes(e.status) && e.asset_id)
        .map(e => e.asset_id!)
})
watch(eventAssetIds, ids => eventsStore.filterByAssets(ids))

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

const markerTime = computed(() => eventInFocus.value?.timestamp ? new Date(eventInFocus.value.timestamp) : null)
const highlightedAsset = computed(() => eventInFocus.value?.asset_id ?? null)

const retrying = ref(false)

async function onRetry(scope: 'all' | 'failed') {
    retrying.value = true
    try {
        const newRunId = await runsStore.retryRun(runId, scope)
        toast.add({ title: `Retry queued (${newRunId.slice(0, 8)})`, color: 'success' })
        await navigateTo(`/executions/runs/${newRunId}`)
    }
    catch {
        toast.add({ title: 'Failed to queue retry', color: 'error' })
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
            assetExecutionsStore.fetchForRun(runId),
            sourcesStore.sources.length === 0 ? sourcesStore.fetch() : Promise.resolve(),
            // Asset dependencies back the Graph view's edges.
            assetsStore.dependencies.length === 0 ? assetsStore.fetch() : Promise.resolve(),
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
    assetExecutionsStore.$reset()
})
</script>

<template>
    <OrgGate :org-id="run?.org_id"
             :error="fetchError"
             back-to="/executions?tab=runs"
             resource-label="run">
        <div class="flex flex-col h-full min-h-0">
            <div class="flex flex-col gap-4 mb-4 shrink-0 px-4 pt-4">
                <div class="flex items-center gap-3">
                    <NuxtLink to="/executions?tab=runs"
                              class="text-sm text-muted hover:text-default">
                        Runs
                    </NuxtLink>
                    <span class="text-sm text-muted">/</span>
                    <span class="text-sm font-medium font-mono">{{ runId }}</span>
                    <UBadge v-if="run"
                            :color="statusColor(run.status)">
                        {{ statusLabel(run.status) }}
                    </UBadge>
                    <div v-if="run?.status === 'failed'"
                         class="ml-auto flex items-center gap-2">
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
                    </div>
                </div>
                <ExecutionsRunSummary v-if="run"
                                      v-model:status-filter="statusFilter"
                                      :run="run"
                                      :asset-executions="assetExecutions" />
            </div>

        <SplitterGroup direction="vertical"
                       auto-save-id="run-panels"
                       class="flex-1 min-h-0 rounded-lg px-4 pb-4">
            <SplitterPanel :default-size="40"
                           :min-size="15"
                           class="flex flex-col overflow-hidden">
                <div class="flex flex-col flex-1 min-h-0 border border-default rounded-xl bg-default overflow-hidden">
                    <div class="flex items-center px-3.5 py-2 shrink-0 border-b border-default">
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
                                            v-model:selected-asset="selectedAsset"
                                            :asset-executions="filteredAssetExecutions"
                                            :status="(run?.status as ExecutionStatus)"
                                            :marker-time="markerTime"
                                            :highlighted-asset="highlightedAsset" />
                    <ExecutionsRunGraph v-else
                                        v-model:selected-asset="selectedAsset"
                                        :run-id="runId" />
                    </div>
                </div>
            </SplitterPanel>

            <SplitterResizeHandle
                class="relative flex items-center justify-center bg-default rounded-lg data-[state=hover]:bg-accented data-[state=drag]:bg-accented transition-colors">
                <div class="z-10 flex h-2 w-8 items-center justify-center rounded-md bg-muted">
                    <UIcon name="i-lucide-grip-horizontal"
                           class="size-3 text-muted" />
                </div>
            </SplitterResizeHandle>

            <SplitterPanel :default-size="60"
                           :min-size="20"
                           class="flex flex-col min-h-0">
                <div class="flex flex-col flex-1 min-h-0 border border-default rounded-xl bg-default overflow-hidden">
                    <div class="flex items-center gap-2 px-3.5 py-2 shrink-0 border-b border-default">
                        <UTabs v-model="eventCategory"
                               :items="eventTabs"
                               variant="pill"
                               size="xs"
                               :content="false" />
                        <span class="ml-auto text-xs font-medium uppercase tracking-wide text-muted">Events</span>
                        <UBadge v-if="!eventsStore.loading"
                                color="neutral"
                                variant="subtle"
                                size="sm">
                            {{ eventsStore.hasMore ? `${eventsStore.events.length} / ${eventsStore.total}` : eventsStore.events.length }}
                        </UBadge>
                    </div>
                    <div class="flex-1 min-h-0">
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
    </OrgGate>
</template>
