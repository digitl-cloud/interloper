<script setup lang="ts">
import VChart from 'vue-echarts'
import { BarChart } from 'echarts/charts'
import { GridComponent, TooltipComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { use } from 'echarts/core'

use([CanvasRenderer, BarChart, GridComponent, TooltipComponent])

interface PartitionData {
    partition: string
    rowCount: number
}

const props = defineProps<{
    data: PartitionData[]
}>()

const theme = useChartTheme()

function formatNumber(value: number): string {
    if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`
    if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`
    return String(value)
}

const chartHeight = computed(() =>
    Math.max(150, Math.min(300, props.data.length * 20)),
)

const option = computed(() => {
    const { text, line, primary, fontFamily, fontSize, tooltip, resolve } = theme.value
    const label = { color: text, fontFamily, fontSize }

    return {
        textStyle: { fontFamily, fontSize },
        grid: {
            left: 'auto',
            right: 16,
            top: 8,
            bottom: props.data.length > 10 ? 60 : 32,
            containLabel: true,
        },
        tooltip: {
            ...tooltip,
            trigger: 'axis',
            axisPointer: { type: 'shadow', shadowStyle: { color: resolve('var(--ui-bg-elevated)') } },
            formatter: (params: any) => {
                const item = params[0]
                if (!item) return ''
                const value = item.value?.toLocaleString?.() ?? item.value
                return `${item.name}<br/>${value} rows`
            },
        },
        xAxis: {
            type: 'category',
            data: props.data.map(d => d.partition),
            axisLabel: { ...label, rotate: props.data.length > 10 ? 45 : 0 },
            axisLine: { lineStyle: { color: line } },
            axisTick: { show: false },
        },
        yAxis: {
            type: 'value',
            axisLabel: { ...label, formatter: formatNumber },
            splitLine: { lineStyle: { color: line } },
        },
        series: [
            {
                type: 'bar',
                data: props.data.map(d => d.rowCount),
                itemStyle: {
                    color: primary,
                    borderRadius: [4, 4, 0, 0],
                },
            },
        ],
    }
})
</script>

<template>
    <VChart :option="option"
            :style="{ height: `${chartHeight}px` }"
            autoresize />
</template>
