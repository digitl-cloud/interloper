<script setup lang="ts">
import type { UpcomingRun } from '~/types/overview'
import type { TimelineBar, TimelineRow } from '~/types/timeline'

const organisationStore = useOrganisationStore()

const props = defineProps<{ upcoming: UpcomingRun[] }>()

const LABEL_WIDTH = 250
const { rowHeight: ROW_HEIGHT, axisHeight: AXIS_HEIGHT } = TIMELINE_LAYOUT
const MAX_ROWS = 10
const SKELETON_ROWS = 3
const REFRESH_INTERVAL = 60_000
/** The window's share that lies ahead of now, so scheduled firings appear as ghosts. */
const FUTURE_RATIO = 1 / 3

const timelineStore = useTimelineStore()
const userStore = useUserStore()
const { runs, span, rangeStart, rangeEnd, loaded, error } = storeToRefs(timelineStore)
const appConfig = useAppConfig()
/** A window the user picked is loading: the bars on screen still show the previous one. */
const switching = ref(false)

const runRows = useRunTimelineRows(runs)
const now = ref(new Date())

/** Job rows gain a dashed bar at their next firing; its length is the job's latest completed run in view. */
const rows = computed<TimelineRow[]>(() => runRows.value.map((row) => {
    const slot = props.upcoming.find(u => u.job_id === row.id)
    if (!slot) return row
    const start = new Date(slot.next_run_at).getTime()
    if (start < now.value.getTime() || start > rangeEnd.value) return row
    const latest = row.bars.reduce<TimelineBar | null>((best, b) => (b.end !== null && (!best || b.end > best.end!) ? b : best), null)
    const duration = latest ? latest.end! - latest.start : 0
    const ghost: TimelineBar = {
        id: `scheduled:${row.id}`,
        status: 'scheduled',
        start,
        end: Math.min(start + duration, rangeEnd.value),
        detail: slot.start_key ? (slot.start_key === slot.end_key ? slot.start_key : `${slot.start_key} → ${slot.end_key}`) : undefined,
    }
    return { ...row, bars: [...row.bars, ghost] }
}))

const spanItems = TIMELINE_SPANS.map(s => ({ label: s.label, value: String(s.value) }))
const activeSpan = computed({
    get: () => String(span.value),
    set: async (value: string) => {
        span.value = Number(value)
        switching.value = true
        await timelineStore.fetch({ futureRatio: FUTURE_RATIO })
        switching.value = false
    },
})

const timezone = computed(() => userStore.user?.timezone ?? Intl.DateTimeFormat().resolvedOptions().timeZone)
const rangeLabel = computed(() => `${formatDayTime(rangeStart.value)} → ${formatDayTime(rangeEnd.value)}`)
const height = computed(() => AXIS_HEIGHT + (loaded.value && rows.value.length ? Math.min(rows.value.length, MAX_ROWS) : SKELETON_ROWS) * ROW_HEIGHT + 1)

function refresh() {
    now.value = new Date()
    timelineStore.fetch({ futureRatio: FUTURE_RATIO })
}

function onBarClick(bar: TimelineBar) {
    if (bar.status === 'scheduled') navigateTo(organisationStore.path(kindPath('job')))
    else navigateTo(organisationStore.path(`/executions/runs/${bar.id}`))
}

watch(rangeEnd, () => {
    now.value = new Date()
})

useIntervalFn(refresh, REFRESH_INTERVAL)
onMounted(refresh)
onUnmounted(() => timelineStore.$reset())
</script>

<template>
    <UCard>
        <template #header>
            <CardHeader title="Timeline"
                        :description="`${timezone} · ${rangeLabel}`">
                <UIcon v-if="switching"
                       :name="appConfig.ui.icons.loading"
                       class="size-4 animate-spin text-dimmed" />
                <span class="text-sm text-muted">Window</span>
                <UTabs v-model="activeSpan"
                       :items="spanItems"
                       variant="pill"
                       size="xs"
                       :content="false" />
                <UButton label="All executions"
                         :to="organisationStore.path('/executions/runs')"
                         color="neutral"
                         variant="outline"
                         size="sm" />
            </CardHeader>
        </template>
        <LoadError v-if="error"
                   title="Couldn't load the timeline"
                   :error="error"
                   @retry="refresh" />
        <div v-else-if="!loaded"
             class="flex flex-col"
             :style="{ height: `${height}px`, paddingTop: `${AXIS_HEIGHT}px` }">
            <div v-for="i in SKELETON_ROWS"
                 :key="i"
                 class="flex items-center gap-4"
                 :style="{ height: `${ROW_HEIGHT}px` }">
                <USkeleton class="h-4 shrink-0"
                           :style="{ width: `${LABEL_WIDTH - 60}px` }" />
                <USkeleton class="h-3 flex-1" />
            </div>
        </div>
        <UEmpty v-else-if="!rows.length"
                :icon="appConfig.icons.timeline"
                title="No runs in this window"
                description="Runs show up here as they execute, and each job's next firing as a dashed bar."
                variant="naked"
                size="sm"
                class="transition-opacity"
                :class="switching ? 'opacity-50' : ''"
                :style="{ minHeight: `${height}px` }" />
        <div v-else
             class="transition-opacity"
             :class="switching ? 'opacity-50' : ''"
             :style="{ height: `${height}px` }">
            <ChartExecutionTimeline :rows="rows"
                                    :range-start="rangeStart"
                                    :range-end="rangeEnd"
                                    :marker-time="now"
                                    :future-from="now.getTime()"
                                    :marker-label="`Now · ${formatClockTime(now)}`"
                                    axis="clock"
                                    :label-width="LABEL_WIDTH"
                                    label-title="Target"
                                    @bar-click="onBarClick" />
        </div>
        <ChartTimelineLegend v-if="!error && (!loaded || rows.length)"
                             scheduled
                             class="mt-3" />
    </UCard>
</template>
