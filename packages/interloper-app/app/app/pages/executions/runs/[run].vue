<script setup lang="ts">
import type { RunEvent } from '~/stores/events'
import type { Run } from '~/types/run'
import type { ExecutionStatus } from '~/types/asset_execution'
import { SplitterGroup, SplitterPanel, SplitterResizeHandle } from 'reka-ui'

definePageMeta({ title: 'Run' })

const route = useRoute()
const runId = route.params.run!.toString()

const runsStore = useRunsStore()
const eventsStore = useEventsStore()
const assetExecutionsStore = useAssetExecutionsStore()
const sourcesStore = useSourcesStore()
const catalogStore = useCatalogStore()

const initialRun = ref<Run | null>(null)
const assetExecutions = computed(() => assetExecutionsStore.assetExecutions)

/** Prefer the store's copy (updated via realtime), fall back to initial fetch. */
const run = computed(() => runsStore.findById(runId) ?? initialRun.value)

const selectedAsset = ref<string | null>(null)
const eventInFocus = ref<RunEvent | null>(null)
const markerTime = computed(() => eventInFocus.value?.timestamp ? new Date(eventInFocus.value.timestamp) : null)
const highlightedAsset = computed(() => eventInFocus.value?.asset_id ?? null)

onMounted(async () => {
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
})

onUnmounted(() => {
    eventsStore.$reset()
    assetExecutionsStore.$reset()
})
</script>

<template>
    <div>
        <div class="flex items-center gap-3 mb-4 shrink-0 px-4 pt-4">
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
        </div>

        <SplitterGroup direction="vertical"
                       auto-save-id="run-panels"
                       class="flex-1 min-h-0 rounded-lg">
            <SplitterPanel :default-size="40"
                           :min-size="15"
                           class="overflow-hidden p-4">
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
                           :min-size="20">
                <ExecutionsEventsTable v-model:selected-asset="selectedAsset"
                                       v-model:event-in-focus="eventInFocus"
                                       :events="eventsStore.events"
                                       :loading="eventsStore.loading" />
            </SplitterPanel>
        </SplitterGroup>
    </div>
</template>
