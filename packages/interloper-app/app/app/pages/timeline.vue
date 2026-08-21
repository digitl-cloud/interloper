<script setup lang="ts">
import type { TimelineBar } from '~/types/timeline'

definePageMeta({ title: 'Timeline', fullBleed: true })

/** Width of the row-label gutter, wide enough for a job name. */
const LABEL_WIDTH = 220

/** How often the window re-anchors to now, so the view keeps up on its own. */
const REFRESH_INTERVAL = 60_000

const timelineStore = useTimelineStore()
const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()

const { runs, span, rangeStart, rangeEnd, loading, total, truncated } = storeToRefs(timelineStore)

const rows = useRunTimelineRows(runs)
const selectedId = ref<string | null>(null)

const spanItems = TIMELINE_SPANS.map(s => ({ label: s.label, value: String(s.value) }))
const activeSpan = computed({
    get: () => String(span.value),
    set: (value: string) => timelineStore.setSpan(Number(value)),
})

const runCount = computed(() => runs.value.reduce((n, run) => n + (run.started_at ? 1 : 0), 0))

function onBarClick(bar: TimelineBar) {
    navigateTo(`/executions/runs/${bar.id}`)
}

let refreshTimer: ReturnType<typeof setInterval> | null = null

onMounted(async () => {
    await Promise.all([
        timelineStore.fetch(),
        // Jobs give the rows; sources/assets name and icon the ad-hoc ones.
        componentsStore.fetchAll(['job', 'source', 'asset']),
        catalogStore.loaded ? Promise.resolve() : catalogStore.fetchCatalog(),
    ])
    refreshTimer = setInterval(() => timelineStore.fetch(), REFRESH_INTERVAL)
})

onUnmounted(() => {
    if (refreshTimer) clearInterval(refreshTimer)
    timelineStore.$reset()
})
</script>

<template>
    <div class="flex flex-col flex-1 min-h-0">
        <div class="flex shrink-0 items-center gap-2 border-b border-default px-4 py-2">
            <span class="text-xs text-muted">Window</span>
            <UTabs v-model="activeSpan"
                   :items="spanItems"
                   variant="pill"
                   size="xs"
                   :content="false" />

        </div>

        <NavActions>
            <UBadge v-if="truncated"
                    color="warning"
                    variant="subtle"
                    icon="i-lucide-triangle-alert"
                    :title="`Only the ${runCount} most recent of ${total} runs in this window are shown — narrow the window to see them all.`">
                Showing {{ runCount }} of {{ total }}
            </UBadge>
            <span v-else
                  class="text-xs text-muted">{{ runCount }} run(s)</span>
            <UButton icon="i-lucide-refresh-cw"
                     color="neutral"
                     variant="outline"
                     size="sm"
                     :loading="loading"
                     aria-label="Refresh"
                     @click="timelineStore.fetch()" />
        </NavActions>

        <div class="flex flex-1 min-h-0 flex-col">
            <div v-if="!loading && !rows.length"
                 class="w-full max-w-[1040px] mx-auto p-4">
                <EmptyState icon="i-lucide-gantt-chart"
                            title="Nothing scheduled yet"
                            description="The timeline lays every job's runs out on a wall-clock axis, so you can see what ran when, what overlapped, and what took longer than it should.">
                    <UButton icon="i-lucide-calendar-plus"
                             label="Create a job"
                             class="mt-5"
                             to="/jobs" />
                </EmptyState>
            </div>

            <div v-else
                 class="flex flex-1 min-h-0 flex-col overflow-hidden">
                <ChartExecutionTimeline v-model:selected-id="selectedId"
                                        :rows="rows"
                                        :range-start="rangeStart"
                                        :range-end="rangeEnd"
                                        axis="clock"
                                        :label-width="LABEL_WIDTH"
                                        label-title="Jobs"
                                        empty-message="No runs in this window"
                                        @bar-click="onBarClick" />
            </div>
        </div>
    </div>
</template>
