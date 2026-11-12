<script setup lang="ts">
import type { TimelineBar } from '~/types/timeline'

/** Width of the row-label gutter: a target's name, plus the badge for its kind. */
const LABEL_WIDTH = 280

/** How often the window re-anchors to now, so the view keeps up on its own. */
const REFRESH_INTERVAL = 60_000

const timelineStore = useTimelineStore()
const organisationStore = useOrganisationStore()
const componentsStore = useComponentsStore()
const appConfig = useAppConfig()

const { runs, rangeStart, rangeEnd, loading, error, total, truncated } = storeToRefs(timelineStore)

/** Active status bucket from the breakdown bar; narrows the bars to that status. */
const statusFilter = useQueryParam('status', null)
const stats = computed(() => runStats(null, executionCounts(runs.value)))
const shownRuns = computed(() => statusFilter.value
    ? runs.value.filter(run => statusesForKey(statusFilter.value!).includes(run.status))
    : runs.value)

const rows = useRunTimelineRows(shownRuns)
const selectedId = ref<string | null>(null)

/** The window travels by its label (`?span=7d`). */
const spanQuery: QueryCodec<number> = {
    parse: raw => TIMELINE_SPANS.find(s => s.label === raw)?.value,
    format: ms => TIMELINE_SPANS.find(s => s.value === ms)?.label ?? String(ms),
}
const windowSpan = useQueryParam('span', DEFAULT_TIMELINE_SPAN, spanQuery)
watch(windowSpan, ms => timelineStore.setSpan(ms))

const spanItems = TIMELINE_SPANS.map(s => ({ label: s.label, value: String(s.value) }))
const activeSpan = computed({
    get: () => String(windowSpan.value),
    set: (value: string) => { windowSpan.value = Number(value) },
})

const runCount = computed(() => runs.value.reduce((n, run) => n + (run.started_at ? 1 : 0), 0))

function onBarClick(bar: TimelineBar) {
    navigateTo(organisationStore.path(`/executions/runs/${bar.id}`))
}

useIntervalFn(() => timelineStore.fetch(), REFRESH_INTERVAL)

onMounted(async () => {
    await Promise.all([
        timelineStore.setSpan(windowSpan.value),
        // Jobs give the rows; sources/assets name and icon the ad-hoc ones.
        componentsStore.fetchAll(['job', 'source', 'asset']),
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
            <LoadError v-if="error"
                       title="Couldn't load the timeline"
                       :error="error"
                       @retry="timelineStore.fetch()" />

            <UEmpty v-if="!loading && !error && !rows.length"
                    :icon="appConfig.icons.timeline"
                    title="Nothing scheduled yet"
                    description="The timeline lays every job's runs out on a wall-clock axis, so you can see what ran when, what overlapped, and what took longer than it should."
                    :actions="[{
                        label: 'New job',
                        icon: appConfig.ui.icons.plus,
                        to: { path: organisationStore.path(kindPath('job')), query: { new: '1' } },
                    }]" />

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
                                <UTooltip v-if="truncated"
                                          :text="`Only the ${runCount} most recent of ${total} runs in this window are shown. Narrow the window to see them all.`">
                                    <UBadge color="warning"
                                            :icon="appConfig.ui.icons.warning">
                                        <span class="tabular-nums">Showing {{ runCount }} of {{ total }}</span>
                                    </UBadge>
                                </UTooltip>
                                <UButton :icon="appConfig.icons.refresh"
                                         color="neutral"
                                         variant="ghost"
                                         size="sm"
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
                    <template #footer>
                        <ChartTimelineLegend />
                    </template>
                </UCard>
            </template>
        </template>
    </UDashboardPanel>
</template>
