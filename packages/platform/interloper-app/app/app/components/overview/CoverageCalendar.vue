<script setup lang="ts">
import VChart from 'vue-echarts'
import { HeatmapChart, ScatterChart } from 'echarts/charts'
import { CalendarComponent, VisualMapComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { use } from 'echarts/core'
import type { CoverageMonths } from '~/types/overview'

use([CanvasRenderer, HeatmapChart, ScatterChart, CalendarComponent, VisualMapComponent])

const overviewStore = useOverviewStore()
const { coverage, coverageMonths, coverageWindow, coverageLoading, coverageError } = storeToRefs(overviewStore)
/** The calendar draws the requested window before its answer arrives, so it never changes size on load. */
const calendarWindow = computed(() => coverage.value ?? coverageWindow.value)
/** A window the user picked is loading: the calendar on screen still shows the previous one. */
const switching = computed(() => !!coverage.value && coverageLoading.value && coverageWindow.value?.since !== coverage.value.since)
/** No partition in the window: an all-blank calendar would read as one still loading. */
const empty = computed(() => !!coverage.value && !coverage.value.sources.length)
const SKELETON_DETAIL_ROWS = 3
const theme = useChartTheme()
const appConfig = useAppConfig()

const catalogStore = useCatalogStore()
const typeFilter = ref('all')
const selected = ref<string | null>(null)
const { byDate, summary } = useCoverageCalendar(coverage, typeFilter)

const typeOptions = computed(() => [
    { label: 'All types', value: 'all' },
    ...[...new Set((coverage.value?.sources ?? []).map(s => s.key))]
        .map(key => ({ label: catalogStore.typeName(key), value: key, icon: componentIcon(key) }))
        .sort((a, b) => a.label.localeCompare(b.label)),
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

/** The cell colours, resolved once per colour mode rather than on every redraw. */
const cellColors = computed(() => CELL_ORDER.map(key => theme.value.resolve(CELL[key])))

const option = computed(() => {
    if (!coverage.value) return {}
    const { text, line, surface, ink, fontFamily, fontSize } = theme.value
    const label = { color: text, fontFamily, fontSize }
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
        visualMap: {
            show: false,
            type: 'piecewise',
            dimension: 1,
            seriesIndex: 0,
            pieces: cellColors.value.map((color, i) => ({ value: i, color })),
        },
        calendar: {
            left: CALENDAR_LEFT,
            top: CALENDAR_TOP,
            cellSize: [cell.value, cell.value],
            range: [coverage.value.since, coverage.value.until],
            orient: 'horizontal',
            splitLine: { show: false },
            itemStyle: { color: surface, borderWidth: 0 },
            dayLabel: { ...label, firstDay: 1, nameMap: ['', 'Mon', '', 'Wed', '', 'Fri', ''], margin: 6 },
            monthLabel: { ...label, margin: 8 },
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

const chart = ref<InstanceType<typeof VChart> | null>(null)

/**
 * The hovered cell, with the box the tooltip anchors to: ECharts paints cells on a canvas, so there is no element.
 * It is kept after the pointer leaves, so the tooltip fades out with its content.
 */
const hovered = shallowRef<{ date: string, reference: { getBoundingClientRect: () => DOMRect } } | null>(null)
const tooltipOpen = ref(false)

function onMouseOver(params: any) {
    const date = params?.data?.value?.[0]
    const canvas = params?.event?.event?.target as HTMLElement | undefined
    const center = chart.value?.convertToPixel({ seriesIndex: 0 }, date) as number[] | undefined
    if (typeof date !== 'string' || !canvas || !center) return
    const frame = canvas.getBoundingClientRect()
    const half = cell.value / 2
    const box = new DOMRect(frame.left + (center[0] ?? 0) - half, frame.top + (center[1] ?? 0) - half, cell.value, cell.value)
    hovered.value = { date, reference: { getBoundingClientRect: () => box } }
    tooltipOpen.value = true
}

function onClick(params: any) {
    const date = params?.data?.value?.[0]
    if (typeof date === 'string' && byDate.value.has(date)) selected.value = date
}

watch(coverage, (value) => {
    if (typeFilter.value !== 'all' && !value?.sources.some(s => s.key === typeFilter.value)) typeFilter.value = 'all'
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
                        :description="coverage && !coverageError && !empty ? summary : undefined"
                        :loading="!coverage && !coverageError">
                <UIcon v-if="switching"
                       :name="appConfig.ui.icons.loading"
                       class="size-4 animate-spin text-dimmed" />
                <USelect v-if="!empty"
                         v-model="typeFilter"
                         :items="typeOptions"
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
            <LoadError v-if="coverageError"
                       title="Couldn't load coverage"
                       :error="coverageError"
                       @retry="overviewStore.fetchCoverage()" />
            <UEmpty v-else-if="empty"
                    :icon="appConfig.icons.window"
                    :title="NO_PARTITIONS"
                    :description="`No partitioned asset had a partition in the last ${coverageMonths} months. Coverage fills in once a job materialises one.`"
                    variant="naked"
                    size="sm"
                    class="transition-opacity"
                    :class="switching ? 'opacity-50' : ''"
                    :style="{ minHeight: `${chartHeight}px` }" />
            <UTooltip v-else-if="coverage"
                      :open="tooltipOpen"
                      :reference="hovered?.reference"
                      :delay-duration="0"
                      :content="{ side: 'top', sideOffset: 6 }"
                      :ui="{ content: 'h-auto pointer-events-none' }">
                <VChart ref="chart"
                        :option="option"
                        class="mx-auto transition-opacity"
                        :class="switching ? 'opacity-50' : ''"
                        :style="{ height: `${chartHeight}px`, width: `${chartWidth}px` }"
                        autoresize
                        @click="onClick"
                        @mouseover="onMouseOver"
                        @mouseout="tooltipOpen = false"
                        @globalout="tooltipOpen = false" />
                <template #content>
                    <OverviewCoverageDayBreakdown v-if="hovered"
                                                  :date="hovered.date"
                                                  :day="byDate.get(hovered.date)" />
                </template>
            </UTooltip>
            <div v-else-if="weeks"
                 class="mx-auto"
                 :style="{ height: `${chartHeight}px`, width: `${chartWidth}px`, padding: `${CALENDAR_TOP}px ${CALENDAR_LEFT}px 0` }">
                <div class="grid grid-flow-col grid-rows-7"
                     :style="{ gap: `${gap}px`, gridAutoColumns: `${cell - gap}px` }">
                    <USkeleton v-for="i in weeks * 7"
                               :key="i"
                               :style="{ height: `${cell - gap}px` }" />
                </div>
            </div>
        </div>
        <OverviewCoverageSourceList v-if="coverage && selected && !coverageError"
                                    :date="selected"
                                    :coverage="coverage"
                                    :type-filter="typeFilter"
                                    @select="selected = $event" />
        <div v-else-if="!coverage && !coverageError"
             class="mt-4 flex flex-col gap-3">
            <USkeleton class="h-4 w-48" />
            <USkeleton v-for="i in SKELETON_DETAIL_ROWS"
                       :key="i"
                       class="h-8 w-full" />
        </div>
    </UCard>
</template>
