<script setup lang="ts">
import type { AssetExecution, ExecutionStatus } from '~/types/asset_execution'
import type { ECBasicOption } from 'echarts/types/dist/shared'
import { CustomChart } from 'echarts/charts'
import { GridComponent, TooltipComponent } from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'

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
    refreshRate: 10,
    markerTime: null,
    highlightedAsset: null,
    status: 'pending',
})

const isRunning = computed(() => props.status === 'running')

const assetDisplayName = useAssetDisplayName()

/**********************
 * ECharts Registration
 **********************/
use([
    CanvasRenderer,
    CustomChart,
    GridComponent,
    TooltipComponent,
])

/**********************
 * Colors
 **********************/
const colorMode = useColorMode()
const isDark = computed(() => colorMode.value === 'dark')

function getStatusColor(status: string) {
    switch (status) {
        case 'success': return isDark.value ? '#4ade80' : '#16a34a'
        case 'failed': return isDark.value ? '#f87171' : '#dc2626'
        case 'running': return isDark.value ? '#60a5fa' : '#2563eb'
        case 'canceled': return isDark.value ? '#fbbf24' : '#d97706'
        default: return isDark.value ? '#6b7280' : '#d1d5db'
    }
}

function getAxisColor() {
    return isDark.value ? '#9ca3af' : '#6b7280'
}

function getTextColor() {
    return isDark.value ? '#e5e7eb' : '#1f2937'
}

/**********************
 * Data Processing
 **********************/
interface Execution {
    asset_id: string | null
    asset_key: string
    asset_name: string
    startTime?: number
    endTime?: number
    status: ExecutionStatus
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

const executions = computed<Execution[]>(() => {
    return props.assetExecutions
        .map(transformAssetExecution)
        .sort((a, b) => {
            const wa = STATUS_WEIGHT[a.status] ?? 9
            const wb = STATUS_WEIGHT[b.status] ?? 9
            if (wa !== wb) return wa - wb
            return (a.startTime ?? Infinity) - (b.startTime ?? Infinity)
        })
})

function transformAssetExecution(record: AssetExecution): Execution {
    const displayName = record.asset_id
        ? assetDisplayName.value.get(record.asset_id)
        : undefined
    return {
        asset_id: record.asset_id,
        asset_key: record.asset_key,
        asset_name: displayName ?? record.asset_key,
        startTime: record.started_at ? new Date(record.started_at).getTime() : undefined,
        endTime: record.completed_at ? new Date(record.completed_at).getTime() : undefined,
        status: record.status ?? 'pending',
    }
}

/**********************
 * Chart
 **********************/
const chart = ref<InstanceType<typeof VChart>>()
const now = ref(Date.now())
const runInterval = ref<ReturnType<typeof setInterval> | null>(null)

const baseTime = computed(() => {
    if (executions.value.length === 0) return 0
    const startTimes = executions.value.map(a => a.startTime).filter((t): t is number => t !== undefined)
    if (startTimes.length === 0) return now.value - 5000
    return Math.min(...startTimes)
})

const relativeNow = computed(() => now.value - baseTime.value)

const rawMax = computed(() => {
    if (executions.value.length === 0) return 0

    const times: number[] = []
    for (const a of executions.value) {
        if (a.status === 'running') {
            times.push(now.value - baseTime.value)
        }
        else if (a.endTime) {
            times.push(a.endTime - baseTime.value)
        }
        else if (a.startTime) {
            times.push(a.startTime - baseTime.value)
        }
    }

    return times.length > 0 ? Math.max(...times) : 5000
})

const minVisualDuration = computed(() => rawMax.value * 0.05)

const assetData = computed(() => {
    return executions.value.map((execution, index) => {
        const { start, end } = getAssetTimeRange(execution)
        return {
            id: execution.asset_id,
            name: execution.asset_name,
            value: [index, start, end, end - start],
            itemStyle: { color: getStatusColor(execution.status) },
        }
    })
})

const minMax = computed(() => {
    if (assetData.value.length === 0) return [0, 0]
    const maxEnd = Math.max(...assetData.value.map(d => d.value[2] ?? 0))
    return [0, maxEnd]
})

const markerData = computed(() => {
    return props.markerTime ? [{ value: [props.markerTime.getTime() - baseTime.value] }] : []
})

const chartHeight = computed(() => {
    const rows = Math.max(1, executions.value.length)
    return rows * ROW_HEIGHT + BAR_HEIGHT
})

const options = computed<ECBasicOption>(() => {
    return {
        series: [
            {
                type: 'custom',
                renderItem: renderAsset,
                data: assetData.value,
                encode: { x: [1, 2], y: 0 },
            },
            ...(markerData.value.length
                ? [{
                    type: 'custom' as const,
                    renderItem: renderVerticalMarker,
                    data: markerData.value,
                    z: 10,
                }]
                : []),
        ],
        xAxis: {
            type: 'value',
            position: 'top',
            min: minMax.value[0],
            max: minMax.value[1],
            axisLabel: {
                color: getAxisColor(),
                formatter: (val: number) => {
                    if (val === 0) return '0'
                    if (val < 1000) return `${Math.round(val)}ms`
                    if (val < 60000) return `${(val / 1000).toFixed(1)}s`
                    if (val < 3600000) return `${(val / 60000).toFixed(1)}m`
                    return `${(val / 3600000).toFixed(1)}h`
                },
            },
            axisLine: { lineStyle: { color: isDark.value ? '#374151' : '#e5e7eb' } },
            splitLine: { lineStyle: { color: isDark.value ? '#1f2937' : '#f3f4f6' } },
        },
        yAxis: {
            type: 'category',
            data: executions.value.map(e => e.asset_name),
            show: false,
            inverse: true,
        },
        grid: {
            left: 0,
            right: 0,
            top: BAR_HEIGHT / 2,
            bottom: BAR_HEIGHT / 2,
        },
        tooltip: {
            backgroundColor: isDark.value ? '#1f2937' : '#ffffff',
            borderColor: isDark.value ? '#374151' : '#e5e7eb',
            textStyle: { color: getTextColor() },
            formatter(params: any) {
                const duration = params.value[3]
                return `${params.marker} ${params.name}<br/>Duration: ${(duration / 1000).toFixed(1)}s`
            },
        },
        animationDuration: props.refreshRate,
        animationDurationUpdate: props.refreshRate,
        animationEasingUpdate: 'linear',
    }
})

function getAssetTimeRange(asset: Execution): { start: number; end: number } {
    const relativeStartTime = asset.startTime ? asset.startTime - baseTime.value : 0
    const relativeEndTime = asset.endTime ? asset.endTime - baseTime.value : relativeNow.value

    switch (asset.status) {
        case 'success':
        case 'failed':
            return {
                start: relativeStartTime,
                end: Math.max(relativeEndTime, relativeStartTime + minVisualDuration.value),
            }
        case 'running':
            return {
                start: relativeStartTime,
                end: relativeNow.value,
            }
        default:
            return {
                start: 0,
                end: minVisualDuration.value,
            }
    }
}

const BAR_HEIGHT = 35
const ROW_HEIGHT = 45

function renderAsset(params: any, api: any) {
    const categoryIndex = api.value(0)
    const start = api.coord([api.value(1), categoryIndex])
    const end = api.coord([api.value(2), categoryIndex])
    const width = end[0] - start[0]
    const height = BAR_HEIGHT
    const textPadding = 8
    const execution = executions.value[categoryIndex]
    const focusedAsset = selectedAsset.value ?? props.highlightedAsset
    const isFocused = focusedAsset === execution!.asset_id

    const shape = {
        type: 'rect',
        transition: ['shape'],
        shape: {
            x: start[0],
            y: start[1] - height / 2,
            width,
            height,
            r: 10,
        },
        style: {
            fill: api.visual('color'),
            opacity: focusedAsset ? (isFocused ? 0.8 : 0.3) : 0.8,
        },
    }

    const label = {
        type: 'text',
        position: [start[0] + textPadding, start[1]],
        style: {
            width: width - textPadding * 2,
            text: execution!.asset_name,
            textAlign: 'left' as const,
            textVerticalAlign: 'middle' as const,
            fontSize: 11,
            fontWeight: 'bold' as const,
            fill: '#ffffff',
            overflow: 'truncate',
            ellipsis: '...',
        },
    }

    return {
        type: 'group',
        children: [shape, label],
    }
}

function renderVerticalMarker(params: any, api: any) {
    const markerTime = api.value(0)
    const x = api.coord([markerTime, 0])[0]

    return {
        type: 'line',
        transition: ['shape'],
        shape: { x1: x, y1: 0, x2: x, y2: api.getHeight() },
        style: {
            stroke: isDark.value ? '#60a5fa' : '#2563eb',
            lineWidth: 3,
            opacity: 0.5,
        },
    }
}

function tick() {
    if (chart.value) {
        if (isRunning.value)
            now.value = Date.now()
        chart.value.setOption(options.value, { replaceMerge: ['series'] })
    }
}

function stop() {
    if (runInterval.value) {
        clearInterval(runInterval.value)
        runInterval.value = null
    }
}

function resize() {
    chart.value?.resize()
}

/**********************
 * Handlers
 **********************/
// Selection listens on mousedown, not click: while the run is live, `tick()`
// rebuilds the series every `refreshRate` ms, so the element under the cursor
// at mousedown no longer exists at mouseup and zrender suppresses the
// synthetic click entirely. Mousedown carries no such down/up identity check.
function onAssetMouseDown(params: any) {
    selectedAsset.value = params.data.id
}

function onBlankSpaceMouseDown(e: { target?: unknown }) {
    if (!e.target) selectedAsset.value = null
}

/**********************
 * Lifecycle
 **********************/
onMounted(() => {
    tick()
    if (isRunning.value) {
        runInterval.value = setInterval(tick, props.refreshRate)
    }
    chart.value?.chart?.getZr().on('mousedown', onBlankSpaceMouseDown)
})

onUnmounted(() => {
    stop()
})

watch(isRunning, (running) => {
    if (running) {
        runInterval.value = setInterval(tick, props.refreshRate)
    }
    else {
        stop()
    }
})

watch(selectedAsset, () => tick())
watch(() => props.assetExecutions, () => {
    nextTick(() => {
        resize()
        tick()
    })
}, { deep: true })
watch(() => props.markerTime, () => tick())
watch(() => props.highlightedAsset, () => tick())

/**********************
 * Expose
 **********************/
defineExpose({ tick, stop, resize })
</script>

<!-- The VChart component is not using the `options` prop on purpose as it makes the chart glitchy.
     Instead, we use the `setOption` method to update the chart in the `tick` function. -->
<template>
    <div class="w-full"
         :style="{ height: chartHeight + 'px' }">
        <VChart ref="chart"
                :manual-update="true"
                @mousedown="onAssetMouseDown" />
    </div>
</template>
