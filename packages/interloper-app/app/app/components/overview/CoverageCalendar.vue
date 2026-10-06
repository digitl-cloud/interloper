<script setup lang="ts">
import VChart from 'vue-echarts'
import { HeatmapChart, ScatterChart } from 'echarts/charts'
import { CalendarComponent, TooltipComponent, VisualMapComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { use } from 'echarts/core'
import type { CoverageMonths } from '~/types/overview'

use([CanvasRenderer, HeatmapChart, ScatterChart, CalendarComponent, TooltipComponent, VisualMapComponent])

const overviewStore = useOverviewStore()
const { coverage, coverageMonths, coverageWindow, coverageLoading, coverageError } = storeToRefs(overviewStore)
/** The calendar draws the requested window before its answer arrives, so it never changes size on load. */
const calendarWindow = computed(() => coverage.value ?? coverageWindow.value)
/** A window the user picked is loading: the calendar on screen still shows the previous one. */
const switching = computed(() => !!coverage.value && coverageLoading.value && coverageWindow.value?.since !== coverage.value.since)
const SKELETON_DETAIL_ROWS = 3
const colorMode = useColorMode()

const sourceFilter = ref('all')
const selected = ref<string | null>(null)
const { byDate, summary } = useCoverageCalendar(coverage, sourceFilter)

const sourceOptions = computed(() => [
    { label: 'All sources', value: 'all' },
    ...(coverage.value?.sources ?? []).map(s => ({ label: s.name, value: s.id })),
])
const windowItems = ([3, 6, 12] as CoverageMonths[]).map(m => ({ label: `${m}m`, value: String(m) }))
const activeWindow = computed({
    get: () => String(coverageMonths.value),
    set: (value: string) => overviewStore.setCoverageMonths(Number(value) as CoverageMonths),
})

const CALENDAR_LEFT = 36
/** Cells keep this size whatever the window, and shrink only when the weeks would not fit the card. */
const CELL_SIZE = 18
/** Below this the card scrolls sideways instead. */
const MIN_CELL_SIZE = 8

const frame = ref<HTMLElement | null>(null)
const { width: frameWidth } = useElementSize(frame)

/** Monday-start week columns the calendar draws for the window. */
const weeks = computed(() => {
    if (!calendarWindow.value) return 0
    const since = new Date(`${calendarWindow.value.since}T00:00:00Z`)
    const until = new Date(`${calendarWindow.value.until}T00:00:00Z`)
    const days = Math.round((until.getTime() - since.getTime()) / 86_400_000) + 1
    const leadingDays = (since.getUTCDay() + 6) % 7
    return Math.ceil((leadingDays + days) / 7)
})

const cell = computed(() => {
    if (!frameWidth.value || !weeks.value) return CELL_SIZE
    const fitting = Math.floor((frameWidth.value - 2 * CALENDAR_LEFT) / weeks.value)
    return Math.max(MIN_CELL_SIZE, Math.min(CELL_SIZE, fitting))
})
const gap = computed(() => (cell.value >= 13 ? 3 : 2))
/** Room above the cells for the month labels. */
const CALENDAR_TOP = 20

const option = computed(() => {
    if (!coverage.value) return {}
    const dark = colorMode.value === 'dark'
    const mode = dark ? 'dark' : 'light'
    const axis = CHART_AXIS_COLORS.axis[mode]
    const line = CHART_AXIS_COLORS.grid[mode]
    const surface = CHART_AXIS_COLORS.surface[mode]
    const ink = CHART_AXIS_COLORS.ink[mode]
    const data: unknown[] = []
    const cursor = new Date(`${coverage.value.since}T00:00:00Z`)
    const until = new Date(`${coverage.value.until}T00:00:00Z`)
    while (cursor <= until) {
        const date = cursor.toISOString().slice(0, 10)
        const day = byDate.value.get(date)
        data.push({
            value: [date, cellStatus(day)],
            itemStyle: { borderColor: surface, borderWidth: gap.value / 2 },
        })
        cursor.setUTCDate(cursor.getUTCDate() + 1)
    }
    return {
        tooltip: {
            backgroundColor: surface,
            borderColor: line,
            textStyle: { color: ink },
            formatter: (params: any) => {
                const date = params.data.value[0]
                const day = byDate.value.get(date)
                if (!day) return `<b>${date}</b><br/>nothing expected`
                return `<b>${date}</b><br/>${day.covered}/${day.expected} covered${day.failed ? ` · ${day.failed} failed` : ''}`
            },
        },
        visualMap: {
            show: false,
            type: 'piecewise',
            dimension: 1,
            seriesIndex: 0,
            pieces: CELL_ORDER.map((key, i) => ({ value: i, color: CELL[key][mode] })),
        },
        calendar: {
            left: CALENDAR_LEFT,
            top: CALENDAR_TOP,
            cellSize: [cell.value, cell.value],
            range: [coverage.value.since, coverage.value.until],
            orient: 'horizontal',
            splitLine: { show: false },
            itemStyle: { color: surface, borderWidth: 0 },
            dayLabel: { firstDay: 1, nameMap: ['', 'Mon', '', 'Wed', '', 'Fri', ''], color: axis, fontSize: 10.5, margin: 6 },
            monthLabel: { color: axis, fontSize: 11, margin: 8 },
            yearLabel: { show: false },
        },
        series: [
            {
                type: 'heatmap',
                coordinateSystem: 'calendar',
                data,
                emphasis: { itemStyle: { borderColor: line, borderWidth: 1 } },
            },
            // The selection ring is its own layer: as a cell border, the cells painted after it cover its edges.
            {
                type: 'scatter',
                coordinateSystem: 'calendar',
                silent: true,
                z: 3,
                symbol: 'rect',
                symbolSize: cell.value + 1,
                itemStyle: { color: 'transparent', borderColor: ink, borderWidth: 2 },
                data: selected.value ? [{ value: [selected.value, 0] }] : [],
            },
        ],
    }
})

/** Gaps are drawn as cell borders, so one row or week is exactly `cell` pixels. */
const chartHeight = computed(() => CALENDAR_TOP + 7 * cell.value + 2)

/**
 * Without `right`, ECharts keeps `cellSize` and the canvas must be as wide as the week columns. The
 * day-label gutter is mirrored on the right so the cells sit in the middle once the canvas is centred.
 */
const chartWidth = computed(() => 2 * CALENDAR_LEFT + weeks.value * cell.value)

function onClick(params: any) {
    const date = params?.data?.value?.[0]
    if (typeof date === 'string' && byDate.value.has(date)) selected.value = date
}

watch(coverage, (value) => {
    if (sourceFilter.value !== 'all' && !value?.sources.some(s => s.id === sourceFilter.value)) sourceFilter.value = 'all'
})

watch(coverage, (value) => {
    if (!value || (selected.value && byDate.value.has(selected.value))) return
    // Land on the most recent day that has anything to say, so the panel is never empty on load.
    selected.value = [...byDate.value.keys()].sort().at(-1) ?? null
}, { immediate: true })
</script>

<template>
    <UCard>
        <template #header>
            <CardHeader title="Partition coverage"
                        :description="coverage ? summary : undefined"
                        :loading="!coverage && !coverageError">
                <UIcon v-if="switching"
                       name="i-lucide-loader-circle"
                       class="size-4 animate-spin text-dimmed" />
                <USelect v-model="sourceFilter"
                         :items="sourceOptions"
                         size="sm"
                         class="w-44" />
                <UTabs v-model="activeWindow"
                       :items="windowItems"
                       variant="pill"
                       size="xs"
                       :content="false" />
            </CardHeader>
        </template>
        <div ref="frame"
             class="overflow-x-auto">
            <UAlert v-if="coverageError"
                    color="error"
                    icon="i-lucide-triangle-alert"
                    title="Couldn't load coverage"
                    :description="errorDetail(coverageError) ?? GENERIC_ERROR"
                    :actions="[{
                        label: 'Try again',
                        icon: 'i-lucide-refresh-cw',
                        color: 'neutral',
                        variant: 'outline',
                        onClick: () => overviewStore.fetchCoverage(),
                    }]"
                    class="mb-3.5" />
            <VChart v-if="coverage"
                    :option="option"
                    class="mx-auto transition-opacity"
                    :class="switching ? 'opacity-50' : ''"
                    :style="{ height: `${chartHeight}px`, width: `${chartWidth}px` }"
                    autoresize
                    @click="onClick" />
            <div v-else-if="weeks && !coverageError"
                 class="mx-auto"
                 :style="{ height: `${chartHeight}px`, width: `${chartWidth}px`, padding: `${CALENDAR_TOP}px ${CALENDAR_LEFT}px 0` }">
                <div class="grid grid-flow-col grid-rows-7"
                     :style="{ gap: `${gap}px`, gridAutoColumns: `${cell - gap}px` }">
                    <USkeleton v-for="i in weeks * 7"
                               :key="i"
                               class="rounded-[2px]"
                               :style="{ height: `${cell - gap}px` }" />
                </div>
            </div>
        </div>
        <OverviewCoverageDayDetail v-if="coverage && selected"
                                   :date="selected"
                                   :coverage="coverage"
                                   :source-filter="sourceFilter" />
        <div v-else-if="!coverage && !coverageError"
             class="mt-5 flex flex-col gap-2.5">
            <USkeleton class="h-4 w-48" />
            <USkeleton v-for="i in SKELETON_DETAIL_ROWS"
                       :key="i"
                       class="h-8 w-full" />
        </div>
    </UCard>
</template>
