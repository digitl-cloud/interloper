<script setup lang="ts">
import type { UpcomingRun } from '~/types/overview'
import type { TimelineBar, TimelineRow } from '~/types/timeline'

const props = defineProps<{ upcoming: UpcomingRun[] }>()

const LABEL_WIDTH = 250
const ROW_HEIGHT = 40
const AXIS_HEIGHT = 30
const MAX_ROWS = 10
const REFRESH_INTERVAL = 60_000
/** The window's share that lies ahead of now, so scheduled firings appear as ghosts. */
const FUTURE_RATIO = 1 / 3

const timelineStore = useTimelineStore()
const userStore = useUserStore()
const { runs, span, rangeStart, rangeEnd, loading } = storeToRefs(timelineStore)

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
    set: (value: string) => {
        span.value = Number(value)
        timelineStore.fetch({ futureRatio: FUTURE_RATIO })
    },
})

const timezone = computed(() => userStore.user?.timezone ?? Intl.DateTimeFormat().resolvedOptions().timeZone)
const rangeLabel = computed(() => {
    const format = (ms: number) => `${formatShortDay(new Date(ms))} ${formatClockTime(new Date(ms))}`
    return `${format(rangeStart.value)} → ${format(rangeEnd.value)}`
})
const height = computed(() => AXIS_HEIGHT + Math.min(rows.value.length, MAX_ROWS) * ROW_HEIGHT + 1)

function onBarClick(bar: TimelineBar) {
    if (bar.status === 'scheduled') navigateTo(kindPath('job'))
    else navigateTo(`/executions/runs/${bar.id}`)
}

watch(rangeEnd, () => {
    now.value = new Date()
})

let refreshTimer: ReturnType<typeof setInterval> | null = null
onMounted(async () => {
    await timelineStore.fetch({ futureRatio: FUTURE_RATIO })
    refreshTimer = setInterval(() => {
        now.value = new Date()
        timelineStore.fetch({ futureRatio: FUTURE_RATIO })
    }, REFRESH_INTERVAL)
})
onUnmounted(() => {
    if (refreshTimer) clearInterval(refreshTimer)
    timelineStore.$reset()
})
</script>

<template>
    <UCard>
        <template #header>
            <CardHeader title="Timeline"
                        :description="`${timezone} · ${rangeLabel}`">
                <span class="text-sm text-muted">Window</span>
                <UTabs v-model="activeSpan"
                       :items="spanItems"
                       variant="pill"
                       size="xs"
                       :content="false" />
                <UButton label="All executions"
                         to="/executions/runs"
                         color="neutral"
                         variant="outline"
                         size="sm" />
            </CardHeader>
        </template>
        <div :style="{ height: `${height}px` }">
            <ChartExecutionTimeline :rows="rows"
                                    :range-start="rangeStart"
                                    :range-end="rangeEnd"
                                    :marker-time="now"
                                    :future-from="now.getTime()"
                                    :marker-label="`Now · ${formatClockTime(now)}`"
                                    axis="clock"
                                    :label-width="LABEL_WIDTH"
                                    label-title="Target"
                                    :empty-message="loading ? 'Loading…' : 'No runs in this window'"
                                    @bar-click="onBarClick" />
        </div>
        <div class="mt-2.5 flex items-center gap-4 text-xs text-dimmed">
            <span class="inline-flex items-center gap-1.5"><span class="h-2 w-3.5 rounded-[3px] bg-success" />Success</span>
            <span class="inline-flex items-center gap-1.5"><span class="h-2 w-3.5 rounded-[3px] bg-error" />Failed</span>
            <span class="inline-flex items-center gap-1.5"><span class="h-2 w-3.5 rounded-[3px] bg-primary" />Running</span>
            <span class="inline-flex items-center gap-1.5"><span class="h-2 w-3.5 rounded-[3px] bg-accented" />Queued</span>
            <span class="inline-flex items-center gap-1.5"><span class="h-2 w-3.5 rounded-[3px] border-[1.5px] border-dashed border-dimmed" />Scheduled</span>
        </div>
    </UCard>
</template>
