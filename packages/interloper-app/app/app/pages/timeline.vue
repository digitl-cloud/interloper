<script setup lang="ts">
import type { TimelineBar } from '~/types/timeline'

/** Width of the row-label gutter: a target's name, plus the badge for its kind. */
const LABEL_WIDTH = 280

/** How often the window re-anchors to now, so the view keeps up on its own. */
const REFRESH_INTERVAL = 60_000

const timelineStore = useTimelineStore()
const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()

const { runs, span, rangeStart, rangeEnd, loading, total, truncated } = storeToRefs(timelineStore)

/** Active status bucket from the breakdown bar; narrows the bars to that status. */
const statusFilter = ref<string | null>(null)
const stats = computed(() => runStats(null, executionCounts(runs.value)))
const shownRuns = computed(() => statusFilter.value
    ? runs.value.filter(run => statusesForKey(statusFilter.value!).includes(run.status))
    : runs.value)

const rows = useRunTimelineRows(shownRuns)
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

useIntervalFn(() => timelineStore.fetch(), REFRESH_INTERVAL)

onMounted(async () => {
    await Promise.all([
        timelineStore.fetch(),
        // Jobs give the rows; sources/assets name and icon the ad-hoc ones.
        componentsStore.fetchAll(['job', 'source', 'asset']),
        catalogStore.loaded ? Promise.resolve() : catalogStore.fetchCatalog(),
    ])
})

onUnmounted(() => timelineStore.$reset())
</script>

<template>
    <UDashboardPanel id="timeline">
        <template #header>
            <AppNavbar title="Timeline" />
        </template>
        <template #body>
            <div v-if="!loading && !rows.length"
                 class="w-full max-w-[1040px] mx-auto">
                <EmptyState icon="i-lucide-gantt-chart"
                            title="Nothing scheduled yet"
                            description="The timeline lays every job's runs out on a wall-clock axis, so you can see what ran when, what overlapped, and what took longer than it should.">
                    <UButton icon="i-lucide-calendar-plus"
                             label="Create a job"
                             class="mt-5"
                             :to="kindPath('job')" />
                </EmptyState>
            </div>

            <template v-else>
                <UCard>
                    <ExecutionsRunStatusBar v-model:status-filter="statusFilter"
                                            :stats="stats"
                                            noun="runs" />
                </UCard>
                <UCard :ui="CANVAS_CARD_UI">
                    <template #header>
                        <div class="flex flex-wrap items-center gap-3">
                            <span class="text-sm text-muted">Window</span>
                            <UTabs v-model="activeSpan"
                                   :items="spanItems"
                                   variant="pill"
                                   size="xs"
                                   :content="false" />
                            <div class="ml-auto flex items-center gap-2">
                                <UBadge v-if="truncated"
                                        color="warning"
                                        variant="subtle"
                                        icon="i-lucide-triangle-alert"
                                        :title="`Only the ${runCount} most recent of ${total} runs in this window are shown. Narrow the window to see them all.`">
                                    Showing {{ runCount }} of {{ total }}
                                </UBadge>
                                <UButton icon="i-lucide-refresh-cw"
                                         color="neutral"
                                         variant="outline"
                                         :loading="loading"
                                         aria-label="Refresh"
                                         @click="timelineStore.fetch()" />
                            </div>
                        </div>
                    </template>
                    <ChartExecutionTimeline v-model:selected-id="selectedId"
                                            :rows="rows"
                                            :range-start="rangeStart"
                                            :range-end="rangeEnd"
                                            axis="clock"
                                            :label-width="LABEL_WIDTH"
                                            label-title="Target"
                                            empty-message="No runs in this window"
                                            @bar-click="onBarClick" />
                </UCard>
            </template>
        </template>
    </UDashboardPanel>
</template>
