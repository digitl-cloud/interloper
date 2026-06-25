<script setup lang="ts">
import type { RunEvent } from '~/stores/events'
import type { Run } from '~/types/run'
import type { ExecutionStatus } from '~/types/asset_execution'
import { SplitterGroup, SplitterPanel, SplitterResizeHandle } from 'reka-ui'

// orgSwitchTarget: this page is bespoke to one org's run — switching org from
// the nav lands on the runs list instead.
definePageMeta({ title: 'Run', orgSwitchTarget: '/executions?tab=runs' })

const route = useRoute()
const runId = route.params.run!.toString()

const runsStore = useRunsStore()
const eventsStore = useEventsStore()
const assetExecutionsStore = useAssetExecutionsStore()
const sourcesStore = useSourcesStore()
const catalogStore = useCatalogStore()
const toast = useToast()

const initialRun = ref<Run | null>(null)
const assetExecutions = computed(() => assetExecutionsStore.assetExecutions)

/** Prefer the store's copy (updated via realtime), fall back to initial fetch. */
const run = computed(() => runsStore.findById(runId) ?? initialRun.value)

const selectedAsset = ref<string | null>(null)
const eventInFocus = ref<RunEvent | null>(null)

// The events table is paged from the server, so the asset filter is applied
// there (re-paged from offset 0) rather than over the loaded pages only.
watch(selectedAsset, asset => eventsStore.filterByAsset(asset))
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
                                      :run="run"
                                      :asset-executions="assetExecutions" />
            </div>

        <SplitterGroup direction="vertical"
                       auto-save-id="run-panels"
                       class="flex-1 min-h-0 rounded-lg">
            <!-- pr-0 so the timeline's scrollbar sits flush at the panel edge,
                 aligned with the events table's scrollbar below. -->
            <SplitterPanel :default-size="40"
                           :min-size="15"
                           class="overflow-hidden py-4 pl-4">
                <ChartExecutionTimeline v-if="run?.status !== 'queued'"
                                        v-model:selected-asset="selectedAsset"
                                        :asset-executions="assetExecutions"
                                        :status="(run?.status as ExecutionStatus)"
                                        :marker-time="markerTime"
                                        :highlighted-asset="highlightedAsset" />
                <div v-else
                     class="flex h-full items-center justify-center text-muted">
                    <span class="text-sm">Run is currently queued...</span>
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
                <div class="flex items-center gap-2 px-4 py-2 shrink-0">
                    <span class="text-xs font-medium uppercase tracking-wide text-muted">Events</span>
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
            </SplitterPanel>
        </SplitterGroup>
        </div>
    </OrgGate>
</template>
