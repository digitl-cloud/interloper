<script setup lang="ts">
import type { TimelineBar, TimelineRow } from '~/types/timeline'

/**********************
 * Models
 **********************/
/** Row in focus: everything else dims. Null clears the focus. */
const selectedId = defineModel<string | null>('selectedId')

/**********************
 * Props
 **********************/
interface Props {
    /** Lanes, in display order — sorting and grouping belong to the caller. */
    rows: TimelineRow[]
    /**
     * Explicit time window in epoch ms. Without it the axis fits the data,
     * spanning the earliest start to the latest end (a run's own duration);
     * with it the axis spans the window (a wall-clock period), still growing
     * to the right while something runs past its end.
     */
    rangeStart?: number | null
    rangeEnd?: number | null
    /** Tick labels: elapsed time from the window start, or wall-clock time. */
    axis?: 'duration' | 'clock'
    /** Width of the left label column in px; 0 renders labels inside the bars. */
    labelWidth?: number
    /** Heading for the label column, shown in the ruler above it. */
    labelTitle?: string
    /**
     * Floor on a bar's rendered length as a fraction of the data extent, so
     * brief executions stay legible when they're read as durations. Distorts
     * the axis, so leave it at 0 for wall-clock windows.
     */
    minBarRatio?: number
    refreshRate?: number
    markerTime?: Date | null
    /** Row highlighted from outside (e.g. the event in focus); loses to a selection. */
    highlightedId?: string | null
    emptyMessage?: string
}

const props = withDefaults(defineProps<Props>(), {
    rangeStart: null,
    rangeEnd: null,
    axis: 'duration',
    labelWidth: 0,
    labelTitle: '',
    minBarRatio: 0,
    // 0 → advance every animation frame (display refresh rate). A positive value
    // throttles the layout updates to at most once per `refreshRate` ms.
    refreshRate: 0,
    markerTime: null,
    highlightedId: null,
    emptyMessage: 'No executions yet',
})

const emit = defineEmits<{
    barClick: [bar: TimelineBar, row: TimelineRow]
}>()

/**********************
 * Colors
 **********************/
const colorMode = useColorMode()
const isDark = computed(() => colorMode.value === 'dark')

function getStatusColor(status: string) {
    const entry = CHART_STATUS_COLORS[status] ?? CHART_STATUS_COLORS.default!
    return isDark.value ? entry.dark : entry.light
}

/**********************
 * Layout constants
 **********************/
const BAR_HEIGHT = 28
const ROW_HEIGHT = 40
const AXIS_HEIGHT = 30
const OVERSCAN = 4
const MAX_ZOOM = 200
/**
 * Breathing room on both ends of the time track, so the first and last tick
 * labels don't sit flush against the panel edges. The ruler and the rows share
 * it, which keeps ticks, gridlines and bars on the same scale.
 */
const PLOT_GUTTER = 16

function clamp(v: number, min: number, max: number) {
    return Math.min(Math.max(v, min), max)
}

/**********************
 * Data Processing
 *
 * `baseRows` is purely data-driven — it does NOT read the live clock, so it only
 * recomputes when `rows` change. Per-frame growth of running bars is handled
 * downstream (axisMax + the template), keeping animation work scoped to the
 * running bars rather than re-deriving the whole list every frame.
 **********************/
interface LayoutBar {
    bar: TimelineBar
    running: boolean
    /** Relative ms from baseTime. */
    start: number
    /** Relative ms from baseTime; undefined while running (grows live). */
    fixedEnd?: number
}

interface LayoutRow {
    row: TimelineRow
    bars: LayoutBar[]
}

/** Zero of the relative time axis: the window start, or the earliest execution. */
const baseTime = computed(() => {
    if (props.rangeStart !== null) return props.rangeStart
    let earliest = Infinity
    for (const row of props.rows) {
        for (const bar of row.bars) if (bar.start < earliest) earliest = bar.start
    }
    return Number.isFinite(earliest) ? earliest : 0
})

/**
 * Largest relative end across all *settled* bars, as the data reports it — the
 * scale the minimum-bar floor is a fraction of. Clock-free, so it only moves
 * when the data does.
 */
const dataExtent = computed(() => {
    let max = 0
    for (const row of props.rows) {
        for (const bar of row.bars) {
            if (bar.end === null) continue
            const end = bar.end - baseTime.value
            if (end > max) max = end
        }
    }
    return max
})

const minVisualDuration = computed(() => {
    if (props.minBarRatio <= 0) return 0
    return Math.max(dataExtent.value * props.minBarRatio, 1)
})

const baseRows = computed<LayoutRow[]>(() => props.rows.map(row => ({
    row,
    bars: row.bars.map((bar) => {
        const running = bar.end === null
        const start = bar.start - baseTime.value
        const fixedEnd = running
            ? undefined
            : Math.max(bar.end! - baseTime.value, start + minVisualDuration.value)
        return { bar, running, start, fixedEnd }
    }),
})))

const hasRunning = computed(() => baseRows.value.some(r => r.bars.some(b => b.running)))

/**
 * Right edge of the settled data *as laid out* — the floor can widen a bar past
 * the raw extent, and the axis has to cover it or that bar renders as a sliver
 * clipped against the edge.
 */
const settledExtent = computed(() => {
    let max = dataExtent.value
    for (const row of baseRows.value) {
        for (const bar of row.bars) {
            if (bar.fixedEnd !== undefined && bar.fixedEnd > max) max = bar.fixedEnd
        }
    }
    return max
})

/**********************
 * Live clock (running bars only)
 **********************/
const now = ref(Date.now())
const rafId = ref<number | null>(null)
let lastTick = 0

const relativeNow = computed(() => now.value - baseTime.value)

/** Live end of a bar in relative ms — only running bars depend on the clock. */
function barEnd(bar: LayoutBar): number {
    if (bar.running) return relativeNow.value
    return bar.fixedEnd ?? bar.start
}

/** Right edge of the window (100% when fitted). Only moves while something runs. */
const axisMax = computed(() => {
    const windowEnd = props.rangeEnd !== null ? props.rangeEnd - baseTime.value : 0
    const settled = Math.max(settledExtent.value, windowEnd)
    const live = hasRunning.value ? Math.max(settled, relativeNow.value) : settled
    return Math.max(live, 1)
})

/**********************
 * Time viewport (zoom / pan)
 *
 * When `fitted`, the viewport tracks [0, axisMax] and follows the run live. Any
 * zoom/pan freezes it to an explicit [start, start+span] window in ms, so the
 * view stays put as the run grows to the right.
 **********************/
const fitted = ref(true)
const viewStart = ref(0)
const viewSpan = ref(0)

const view = computed(() => {
    if (fitted.value) return { start: 0, end: axisMax.value, span: axisMax.value }
    const span = clamp(viewSpan.value, 1, axisMax.value)
    const start = clamp(viewStart.value, 0, Math.max(0, axisMax.value - span))
    return { start, end: start + span, span }
})

function toPercent(relativeTime: number): number {
    const { start, span } = view.value
    return ((relativeTime - start) / span) * 100
}

/** Rendered edges of a bar, clipped to the viewport. */
function barGeometry(bar: LayoutBar): { left: number, width: number, visible: boolean } {
    const left = toPercent(bar.start)
    const right = toPercent(barEnd(bar))
    if (right < 0 || left > 100) return { left: 0, width: 0, visible: false }
    const clippedLeft = clamp(left, 0, 100)
    return { left: clippedLeft, width: Math.max(clamp(right, 0, 100) - clippedLeft, 0), visible: true }
}

function resetZoom() {
    fitted.value = true
    viewStart.value = 0
    viewSpan.value = 0
}

function zoomAt(fraction: number, factor: number) {
    const v = view.value
    const anchor = v.start + fraction * v.span
    const span = clamp(v.span * factor, axisMax.value / MAX_ZOOM, axisMax.value)
    if (span >= axisMax.value) {
        resetZoom()
        return
    }
    fitted.value = false
    viewSpan.value = span
    viewStart.value = clamp(anchor - fraction * span, 0, axisMax.value - span)
}

function panByMs(deltaMs: number) {
    if (fitted.value) return
    const span = view.value.span
    viewStart.value = clamp(view.value.start + deltaMs, 0, axisMax.value - span)
}

/**********************
 * Axis ticks
 **********************/
function formatDuration(val: number): string {
    if (val === 0) return '0'
    if (val < 1000) return `${Math.round(val)}ms`
    if (val < 60000) return `${(val / 1000).toFixed(1)}s`
    if (val < 3600000) return `${(val / 60000).toFixed(1)}m`
    return `${(val / 3600000).toFixed(1)}h`
}

const SECOND = 1000
const MINUTE = 60 * SECOND
const HOUR = 60 * MINUTE
const DAY = 24 * HOUR

/** Steps a wall-clock reader expects to see ticks on. */
const CLOCK_STEPS = [
    SECOND, 5 * SECOND, 15 * SECOND, 30 * SECOND,
    MINUTE, 5 * MINUTE, 15 * MINUTE, 30 * MINUTE,
    HOUR, 2 * HOUR, 3 * HOUR, 6 * HOUR, 12 * HOUR,
    DAY, 7 * DAY,
]

function formatClock(absolute: number, step: number): string {
    const date = new Date(absolute)
    const midnight = millisSinceMidnight(date) < SECOND
    if (step >= DAY || midnight) return formatShortDay(date)
    return formatClockTime(date, step < MINUTE)
}

/** "Nice" evenly-spaced ticks across the current viewport. */
const ticks = computed(() => {
    const { start, end, span } = view.value
    const result: { value: number, percent: number, label: string }[] = []

    if (props.axis === 'clock') {
        const step = CLOCK_STEPS.find(s => span / s <= 8) ?? CLOCK_STEPS.at(-1)!
        const anchor = startOfDay(baseTime.value + start) - baseTime.value
        for (let v = anchor + Math.ceil((start - anchor) / step) * step; v <= end; v += step) {
            result.push({ value: v, percent: toPercent(v), label: formatClock(baseTime.value + v, step) })
        }
        return result
    }

    const rawStep = span / 6
    const magnitude = 10 ** Math.floor(Math.log10(rawStep))
    const normalized = rawStep / magnitude
    const step = (normalized < 1.5 ? 1 : normalized < 3 ? 2 : normalized < 7 ? 5 : 10) * magnitude
    for (let v = Math.ceil(start / step) * step; v <= end + step * 0.001; v += step) {
        result.push({ value: v, percent: toPercent(v), label: formatDuration(v) })
    }
    return result
})

/**********************
 * Marker
 **********************/
const markerPercent = computed(() => {
    if (!props.markerTime) return null
    const pct = toPercent(props.markerTime.getTime() - baseTime.value)
    if (pct < 0 || pct > 100) return null
    return pct
})

/**********************
 * Virtualization
 **********************/
const scrollEl = ref<HTMLElement | null>(null)
const plotEl = ref<HTMLElement | null>(null)
const scrollTop = ref(0)
const viewportH = ref(600)

function onScroll() {
    if (scrollEl.value) scrollTop.value = scrollEl.value.scrollTop
}

const totalHeight = computed(() => baseRows.value.length * ROW_HEIGHT)

/** Index range of rows intersecting the viewport (plus a small overscan). */
const visibleRange = computed(() => {
    const total = baseRows.value.length
    const first = Math.floor((scrollTop.value - AXIS_HEIGHT) / ROW_HEIGHT) - OVERSCAN
    const last = Math.ceil((scrollTop.value + viewportH.value - AXIS_HEIGHT) / ROW_HEIGHT) + OVERSCAN
    return { start: clamp(first, 0, total), end: clamp(last, 0, total) }
})

/**
 * Rows intersecting the viewport, each carrying only the bars visible in the
 * current time window and their rendered geometry. Recomputed per frame while
 * something runs, which is the only work the live clock should cost.
 */
const visibleRows = computed(() => {
    const { start, end } = visibleRange.value
    return baseRows.value.slice(start, end).map((layout, i) => ({
        row: layout.row,
        index: start + i,
        /** The row had nothing to draw at all — not merely nothing in view. */
        placeholder: layout.bars.length === 0,
        bars: layout.bars
            .map(bar => ({ ...bar, geometry: barGeometry(bar) }))
            .filter(bar => bar.geometry.visible),
    }))
})

/**********************
 * Focus / selection
 **********************/
const focusedId = computed(() => selectedId.value ?? props.highlightedId)

function rowOpacity(id: string | null): number {
    if (!focusedId.value) return 1
    return id === focusedId.value ? 1 : 0.25
}

function onBarClick(layout: LayoutBar, row: TimelineRow) {
    selectedId.value = row.id
    emit('barClick', layout.bar, row)
}

function onBlankClick() {
    selectedId.value = null
}

/** Row name, the execution's own context, then how long it took. */
function barTooltip(row: TimelineRow, layout: LayoutBar): string {
    const parts = [row.name]
    if (layout.bar.detail) parts.push(layout.bar.detail)
    if (props.axis === 'clock') parts.push(formatDate(new Date(layout.bar.start)))
    parts.push(formatElapsed(new Date(layout.bar.start), layout.bar.end ? new Date(layout.bar.end) : null))
    return parts.join(' · ')
}

/**********************
 * Interaction: wheel zoom/pan + drag-to-pan the ruler
 **********************/
function plotRect(): DOMRect | null {
    return plotEl.value?.getBoundingClientRect() ?? null
}

function onWheel(e: WheelEvent) {
    const rect = plotRect()
    if (!rect) return

    if (e.ctrlKey || e.metaKey) {
        // Ctrl/⌘ + wheel (and trackpad pinch) → zoom at the cursor.
        e.preventDefault()
        const fraction = clamp((e.clientX - rect.left) / rect.width, 0, 1)
        zoomAt(fraction, e.deltaY > 0 ? 1.15 : 1 / 1.15)
        return
    }

    const horizontal = e.shiftKey || Math.abs(e.deltaX) > Math.abs(e.deltaY)
    if (horizontal && !fitted.value) {
        // Horizontal / shift wheel → pan (only meaningful when zoomed in).
        e.preventDefault()
        const delta = e.shiftKey ? e.deltaY : e.deltaX
        panByMs((delta / rect.width) * view.value.span)
    }
    // Otherwise: let the container scroll vertically natively.
}

const dragging = ref(false)
let dragX = 0

function onRulerPointerDown(e: PointerEvent) {
    if (fitted.value) return
    dragging.value = true
    dragX = e.clientX
    ;(e.currentTarget as HTMLElement).setPointerCapture(e.pointerId)
}

function onRulerPointerMove(e: PointerEvent) {
    const rect = plotRect()
    if (!dragging.value || !rect) return
    const dx = e.clientX - dragX
    dragX = e.clientX
    // Dragging right reveals earlier time → viewStart decreases.
    panByMs((-dx / rect.width) * view.value.span)
}

function onRulerPointerUp() {
    dragging.value = false
}

/**********************
 * Live ticking — advance running bars on requestAnimationFrame
 **********************/
function frame(timestamp: number) {
    if (!hasRunning.value) return
    if (timestamp - lastTick >= props.refreshRate) {
        now.value = Date.now()
        lastTick = timestamp
    }
    rafId.value = requestAnimationFrame(frame)
}

function start() {
    stop()
    now.value = Date.now()
    lastTick = 0
    rafId.value = requestAnimationFrame(frame)
}

function stop() {
    if (rafId.value !== null) {
        cancelAnimationFrame(rafId.value)
        rafId.value = null
    }
}

/**********************
 * Lifecycle
 **********************/
let resizeObserver: ResizeObserver | null = null

onMounted(() => {
    if (scrollEl.value) {
        viewportH.value = scrollEl.value.clientHeight
        scrollEl.value.addEventListener('wheel', onWheel, { passive: false })
        resizeObserver = new ResizeObserver(() => {
            if (scrollEl.value) viewportH.value = scrollEl.value.clientHeight
        })
        resizeObserver.observe(scrollEl.value)
    }
    if (hasRunning.value) start()
})

onUnmounted(() => {
    stop()
    resizeObserver?.disconnect()
    scrollEl.value?.removeEventListener('wheel', onWheel)
})

watch(hasRunning, (running) => {
    if (running) start()
    else stop()
})

// A shrinking dataset can leave the viewport scrolled past the new end.
watch(axisMax, () => {
    if (!fitted.value && viewStart.value > axisMax.value) resetZoom()
})
</script>

<template>
    <div ref="scrollEl"
         class="relative h-full w-full overflow-y-auto overflow-x-hidden"
         @scroll="onScroll"
         @click="onBlankClick"
         @dblclick="resetZoom">
        <!-- Time axis / ruler: sticky, and the drag-to-pan surface when zoomed. -->
        <div class="sticky top-0 z-30 flex select-none border-b border-default bg-default"
             :class="fitted ? '' : 'cursor-ew-resize'"
             :style="{ height: AXIS_HEIGHT + 'px' }"
             @pointerdown="onRulerPointerDown"
             @pointermove="onRulerPointerMove"
             @pointerup="onRulerPointerUp"
             @pointercancel="onRulerPointerUp">
            <div v-if="labelWidth"
                 class="flex shrink-0 items-center border-r border-default px-4"
                 :style="{ width: labelWidth + 'px' }">
                <span class="truncate text-xs font-medium uppercase tracking-wide text-muted">{{ labelTitle }}</span>
            </div>
            <div ref="plotEl"
                 class="relative flex-1"
                 :style="{ marginInline: PLOT_GUTTER + 'px' }">
                <div v-for="t in ticks"
                     :key="t.value"
                     class="absolute top-0 flex h-full items-center whitespace-nowrap text-[12.5px] font-medium text-muted"
                     :style="{
                         left: `min(${t.percent}%, calc(100% - 1px))`,
                         transform: t.percent <= 1 ? 'translateX(0)' : t.percent >= 96 ? 'translateX(-100%)' : 'translateX(-50%)',
                     }">
                    {{ t.label }}
                </div>

                <UButton v-if="!fitted"
                         icon="i-lucide-zoom-out"
                         label="Reset zoom"
                         size="xs"
                         color="neutral"
                         variant="subtle"
                         class="absolute right-2 top-1/2 z-30 -translate-y-1/2"
                         @click.stop="resetZoom"
                         @pointerdown.stop
                         @dblclick.stop />
            </div>
        </div>

        <!-- Rows -->
        <div v-if="baseRows.length"
             class="relative"
             :style="{ height: totalHeight + 'px', minHeight: `calc(100% - ${AXIS_HEIGHT}px)` }">
            <!-- Label gutter -->
            <div v-if="labelWidth"
                 class="absolute inset-y-0 left-0 z-20 border-r border-default bg-default"
                 :style="{ width: labelWidth + 'px' }">
                <div v-for="{ row, index } in visibleRows"
                     :key="`label-${row.id ?? row.name}`"
                     class="absolute flex w-full items-center gap-2 px-4 cursor-pointer transition-opacity"
                     :style="{
                         top: index * ROW_HEIGHT + 'px',
                         height: ROW_HEIGHT + 'px',
                         opacity: rowOpacity(row.id),
                     }"
                     :title="row.name"
                     @click.stop="selectedId = row.id">
                    <UIcon :name="row.icon"
                           class="size-4 shrink-0 text-muted" />
                    <span class="truncate text-sm font-medium">{{ row.name }}</span>
                </div>
            </div>

            <!-- Plot area: gridlines, bars, marker -->
            <div class="absolute inset-y-0"
                 :style="{ left: labelWidth + PLOT_GUTTER + 'px', right: PLOT_GUTTER + 'px' }">
                <!-- Gridlines -->
                <div v-for="t in ticks"
                     :key="`grid-${t.value}`"
                     class="absolute top-0 bottom-0 w-px bg-elevated"
                     :style="{ left: `${t.percent}%` }" />

                <!-- Bars (virtualized: only rows intersecting the viewport are rendered) -->
                <template v-for="{ row, bars, index, placeholder } in visibleRows"
                          :key="row.id ?? row.name">
                    <!-- Nothing to draw: a placeholder carrying the row's status -->
                    <div v-if="placeholder && !labelWidth"
                         class="absolute flex max-w-[45%] items-center gap-1.5 overflow-hidden rounded-md border border-dashed border-accented px-2 cursor-pointer text-dimmed transition-opacity"
                         :style="{
                             top: index * ROW_HEIGHT + (ROW_HEIGHT - BAR_HEIGHT) / 2 + 'px',
                             left: '0',
                             height: BAR_HEIGHT + 'px',
                             opacity: rowOpacity(row.id),
                         }"
                         :title="`${row.name} — ${statusLabel(row.status)}`"
                         @click.stop="selectedId = row.id">
                        <UIcon :name="row.icon"
                               class="size-3.5 shrink-0" />
                        <span class="truncate text-xs font-medium">{{ row.name }}</span>
                    </div>

                    <!-- Time bars -->
                    <div v-for="layout in bars"
                         :key="layout.bar.id"
                         class="absolute flex items-center gap-1.5 overflow-hidden rounded-md cursor-pointer transition-opacity"
                         :class="labelWidth ? '' : 'px-2'"
                         :style="{
                             top: index * ROW_HEIGHT + (ROW_HEIGHT - BAR_HEIGHT) / 2 + 'px',
                             left: `${layout.geometry.left}%`,
                             width: `${layout.geometry.width}%`,
                             minWidth: '6px',
                             height: BAR_HEIGHT + 'px',
                             backgroundColor: getStatusColor(layout.bar.status),
                             opacity: rowOpacity(row.id) * 0.96,
                         }"
                         :title="barTooltip(row, layout)"
                         @click.stop="onBarClick(layout, row)">
                        <template v-if="!labelWidth">
                            <UIcon :name="row.icon"
                                   class="size-3.5 shrink-0 text-white" />
                            <span class="truncate text-xs font-bold text-white">{{ row.name }}</span>
                        </template>
                    </div>
                </template>

                <!-- Marker -->
                <div v-if="markerPercent !== null"
                     class="pointer-events-none absolute top-0 bottom-0 z-10 w-0.5 bg-primary/50"
                     :style="{ left: `${markerPercent}%` }" />
            </div>
        </div>

        <!-- Empty state -->
        <div v-else
             class="flex h-full items-center justify-center text-sm text-muted">
            {{ emptyMessage }}
        </div>
    </div>
</template>
