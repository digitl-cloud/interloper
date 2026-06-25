<script setup lang="ts">
import type { AssetExecution, ExecutionStatus } from '~/types/asset_execution'

/**********************
 * Models
 **********************/
const selectedAsset = defineModel<string | null>('selectedAsset')

/**********************
 * Props
 **********************/
interface Props {
    assetExecutions: AssetExecution[]
    status?: ExecutionStatus
    refreshRate?: number
    markerTime?: Date | null
    highlightedAsset?: string | null
}

const props = withDefaults(defineProps<Props>(), {
    // 0 → advance every animation frame (display refresh rate). A positive value
    // throttles the layout updates to at most once per `refreshRate` ms.
    refreshRate: 0,
    markerTime: null,
    highlightedAsset: null,
    status: 'pending',
})

const isRunning = computed(() => props.status === 'running')

const assetDisplayName = useAssetDisplayName()
const assetIcon = useAssetIcon()

/**********************
 * Colors
 **********************/
const colorMode = useColorMode()
const isDark = computed(() => colorMode.value === 'dark')

function getStatusColor(status: string) {
    switch (status) {
        case 'success': return isDark.value ? '#22c55e' : '#16a34a'
        case 'failed': return isDark.value ? '#f87171' : '#dc2626'
        case 'running': return isDark.value ? '#60a5fa' : '#2563eb'
        case 'canceled': return isDark.value ? '#fbbf24' : '#d97706'
        default: return isDark.value ? '#6b7280' : '#d1d5db'
    }
}

/**********************
 * Layout constants
 **********************/
const BAR_HEIGHT = 28
const ROW_HEIGHT = 40
const AXIS_HEIGHT = 28
const OVERSCAN = 4
const MAX_ZOOM = 200

const DEFAULT_ICON = 'i-lucide-box'

function clamp(v: number, min: number, max: number) {
    return Math.min(Math.max(v, min), max)
}

/**********************
 * Data Processing
 *
 * `baseRows` is purely data-driven — it does NOT read the live clock, so it only
 * recomputes when `assetExecutions` change. Per-frame growth of running bars is
 * handled downstream (axisMax + the template), keeping animation work scoped to
 * the running rows rather than re-deriving the whole list every frame.
 **********************/
interface BaseRow {
    id: string | null
    name: string
    icon: string
    status: ExecutionStatus
    /** Has a start time → render a time bar; otherwise a not-started placeholder. */
    timed: boolean
    running: boolean
    /** Relative ms from baseTime. */
    start: number
    /** Relative ms from baseTime; undefined for running (grows live) and untimed rows. */
    fixedEnd?: number
}

/** Sort weight: assets that ran sort first, non-started assets sink to the bottom. */
const STATUS_WEIGHT: Record<string, number> = {
    success: 0,
    running: 1,
    failed: 2,
    canceled: 3,
    skipped: 4,
    queued: 5,
    pending: 5,
}

interface Parsed {
    id: string | null
    name: string
    icon: string
    status: ExecutionStatus
    startTime?: number
    endTime?: number
}

const parsed = computed<Parsed[]>(() => {
    return props.assetExecutions
        .map(parseExecution)
        .sort((a, b) => {
            const wa = STATUS_WEIGHT[a.status] ?? 9
            const wb = STATUS_WEIGHT[b.status] ?? 9
            if (wa !== wb) return wa - wb
            return (a.startTime ?? Infinity) - (b.startTime ?? Infinity)
        })
})

function parseExecution(record: AssetExecution): Parsed {
    const displayName = record.asset_id ? assetDisplayName.value.get(record.asset_id) : undefined
    return {
        id: record.asset_id,
        name: displayName ?? record.asset_key,
        icon: (record.asset_id ? assetIcon.value.get(record.asset_id) : undefined) ?? DEFAULT_ICON,
        status: record.status ?? 'pending',
        startTime: record.started_at ? new Date(record.started_at).getTime() : undefined,
        endTime: record.completed_at ? new Date(record.completed_at).getTime() : undefined,
    }
}

/** Earliest start across the run; the zero of the relative time axis. */
const baseTime = computed(() => {
    const starts = parsed.value.map(p => p.startTime).filter((t): t is number => t !== undefined)
    return starts.length ? Math.min(...starts) : 0
})

/**
 * Largest relative end across all *settled* timed rows (running rows grow live and
 * are folded in via `axisMax`). Drives the min-visual-duration floor and the axis
 * extent without depending on the clock.
 */
const staticMaxEnd = computed(() => {
    let max = 0
    for (const p of parsed.value) {
        if (p.startTime === undefined || p.status === 'running') continue
        const end = (p.endTime ?? p.startTime) - baseTime.value
        if (end > max) max = end
    }
    return max
})

const minVisualDuration = computed(() => Math.max(staticMaxEnd.value * 0.05, 1))

const baseRows = computed<BaseRow[]>(() => {
    return parsed.value.map((p) => {
        const timed = p.startTime !== undefined
        const running = p.status === 'running'
        const start = timed ? p.startTime! - baseTime.value : 0
        let fixedEnd: number | undefined
        if (timed && !running) {
            const rawEnd = (p.endTime ?? p.startTime!) - baseTime.value
            fixedEnd = Math.max(rawEnd, start + minVisualDuration.value)
        }
        return { id: p.id, name: p.name, icon: p.icon, status: p.status, timed, running, start, fixedEnd }
    })
})

const hasRunning = computed(() => baseRows.value.some(r => r.running))

/**********************
 * Live clock (running bars only)
 **********************/
const now = ref(Date.now())
const rafId = ref<number | null>(null)
let lastTick = 0

const relativeNow = computed(() => now.value - baseTime.value)

/** Live end of a row in relative ms — only running rows depend on the clock. */
function rowEnd(row: BaseRow): number {
    if (row.running) return relativeNow.value
    return row.fixedEnd ?? row.start
}

/** Right edge of the data (100% when fitted). Only moves while something runs. */
const axisMax = computed(() => {
    const settled = staticMaxEnd.value
    const live = hasRunning.value ? Math.max(settled, relativeNow.value) : settled
    return Math.max(live, 1)
})

/**********************
 * Time viewport (zoom / pan) — #6
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
function formatTime(val: number): string {
    if (val === 0) return '0'
    if (val < 1000) return `${Math.round(val)}ms`
    if (val < 60000) return `${(val / 1000).toFixed(1)}s`
    if (val < 3600000) return `${(val / 60000).toFixed(1)}m`
    return `${(val / 3600000).toFixed(1)}h`
}

/** "Nice" evenly-spaced ticks across the current viewport. */
const ticks = computed(() => {
    const { start, end, span } = view.value
    const rawStep = span / 6
    const magnitude = 10 ** Math.floor(Math.log10(rawStep))
    const normalized = rawStep / magnitude
    const step = (normalized < 1.5 ? 1 : normalized < 3 ? 2 : normalized < 7 ? 5 : 10) * magnitude

    const result: { value: number; percent: number; label: string }[] = []
    for (let v = Math.ceil(start / step) * step; v <= end + step * 0.001; v += step) {
        result.push({ value: v, percent: toPercent(v), label: formatTime(v) })
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
 * Virtualization — #5
 **********************/
const scrollEl = ref<HTMLElement | null>(null)
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

const visibleRows = computed(() => {
    const { start, end } = visibleRange.value
    return baseRows.value.slice(start, end).map((row, i) => ({ row, index: start + i }))
})

/**********************
 * Focus / selection
 **********************/
const focusedAsset = computed(() => selectedAsset.value ?? props.highlightedAsset)

function rowOpacity(id: string | null): number {
    if (!focusedAsset.value) return 1
    return id === focusedAsset.value ? 1 : 0.25
}

function onBarClick(id: string | null) {
    selectedAsset.value = id
}

function onBlankClick() {
    selectedAsset.value = null
}

/**********************
 * Interaction: wheel zoom/pan + drag-to-pan the ruler
 **********************/
function onWheel(e: WheelEvent) {
    if (!scrollEl.value) return
    const rect = scrollEl.value.getBoundingClientRect()

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
    if (!dragging.value || !scrollEl.value) return
    const width = scrollEl.value.getBoundingClientRect().width
    const dx = e.clientX - dragX
    dragX = e.clientX
    // Dragging right reveals earlier time → viewStart decreases.
    panByMs((-dx / width) * view.value.span)
}

function onRulerPointerUp() {
    dragging.value = false
}

/**********************
 * Live ticking — advance running bars on requestAnimationFrame
 **********************/
function frame(timestamp: number) {
    if (!isRunning.value) return
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
    if (isRunning.value) start()
})

onUnmounted(() => {
    stop()
    resizeObserver?.disconnect()
    scrollEl.value?.removeEventListener('wheel', onWheel)
})

watch(isRunning, (running) => {
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
        <div class="sticky top-0 z-20 flex select-none border-b border-default bg-default"
             :class="fitted ? '' : 'cursor-ew-resize'"
             :style="{ height: AXIS_HEIGHT + 'px' }"
             @pointerdown="onRulerPointerDown"
             @pointermove="onRulerPointerMove"
             @pointerup="onRulerPointerUp"
             @pointercancel="onRulerPointerUp">
            <div v-for="t in ticks"
                 :key="t.value"
                 class="absolute top-0 flex h-full items-center whitespace-nowrap text-[10px] text-muted"
                 :style="{
                     left: `min(${t.percent}%, calc(100% - 1px))`,
                     transform: t.percent <= 1 ? 'translateX(0)' : t.percent >= 99 ? 'translateX(-100%)' : 'translateX(-50%)',
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

        <!-- Rows -->
        <div v-if="baseRows.length"
             class="relative"
             :style="{ height: totalHeight + 'px' }">
            <!-- Gridlines -->
            <div v-for="t in ticks"
                 :key="`grid-${t.value}`"
                 class="absolute top-0 bottom-0 w-px bg-default"
                 :style="{ left: `${t.percent}%` }" />

            <!-- Bars (virtualized: only rows intersecting the viewport are rendered) -->
            <template v-for="{ row, index } in visibleRows"
                      :key="row.id ?? row.name">
                <!-- Not-started placeholder (#3) -->
                <div v-if="!row.timed"
                     class="absolute flex max-w-[45%] items-center gap-1.5 overflow-hidden rounded-md border border-dashed border-default px-2 cursor-pointer text-muted transition-opacity"
                     :style="{
                         top: index * ROW_HEIGHT + (ROW_HEIGHT - BAR_HEIGHT) / 2 + 'px',
                         left: '0',
                         height: BAR_HEIGHT + 'px',
                         opacity: rowOpacity(row.id),
                     }"
                     :title="`${row.name} — ${row.status}`"
                     @click.stop="onBarClick(row.id)">
                    <UIcon :name="row.icon"
                           class="size-3.5 shrink-0" />
                    <span class="truncate text-[11px] font-medium">{{ row.name }}</span>
                </div>

                <!-- Time bar -->
                <div v-else
                     class="absolute flex items-center gap-1.5 overflow-hidden rounded-md px-2 cursor-pointer transition-opacity"
                     :style="{
                         top: index * ROW_HEIGHT + (ROW_HEIGHT - BAR_HEIGHT) / 2 + 'px',
                         left: `${toPercent(row.start)}%`,
                         width: `${Math.max(toPercent(rowEnd(row)) - toPercent(row.start), 0)}%`,
                         minWidth: '2px',
                         height: BAR_HEIGHT + 'px',
                         backgroundColor: getStatusColor(row.status),
                         opacity: rowOpacity(row.id) * 0.95,
                     }"
                     :title="`${row.name} — ${((rowEnd(row) - row.start) / 1000).toFixed(1)}s`"
                     @click.stop="onBarClick(row.id)">
                    <UIcon :name="row.icon"
                           class="size-3.5 shrink-0 text-white" />
                    <span class="truncate text-[11px] font-bold text-white">{{ row.name }}</span>
                </div>
            </template>

            <!-- Marker -->
            <div v-if="markerPercent !== null"
                 class="pointer-events-none absolute top-0 bottom-0 z-10 w-0.5 bg-primary/50"
                 :style="{ left: `${markerPercent}%` }" />
        </div>

        <!-- Empty state -->
        <div v-else
             class="flex h-full items-center justify-center text-sm text-muted">
            No asset executions yet
        </div>
    </div>
</template>
