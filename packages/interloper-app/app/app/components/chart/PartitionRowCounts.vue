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

const colorMode = useColorMode()

function formatNumber(value: number): string {
    if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`
    if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`
    return String(value)
}

const chartHeight = computed(() =>
    Math.max(150, Math.min(300, props.data.length * 20)),
)

const option = computed(() => {
    const isDark = colorMode.value === 'dark'
    const axisColor = isDark ? CHART_AXIS_COLORS.axis.dark : CHART_AXIS_COLORS.axis.light
    const barColor = isDark ? CHART_AXIS_COLORS.bar.dark : CHART_AXIS_COLORS.bar.light

    return {
        grid: {
            left: 'auto',
            right: 16,
            top: 8,
            bottom: props.data.length > 10 ? 60 : 32,
            containLabel: true,
        },
        tooltip: {
            trigger: 'axis',
            axisPointer: { type: 'shadow' },
            formatter: (params: any) => {
                const item = params[0]
                if (!item) return ''
                const value = item.value?.toLocaleString?.() ?? item.value
                return `<b>${item.name}</b><br/>${value} rows`
            },
        },
        xAxis: {
            type: 'category',
            data: props.data.map(d => d.partition),
            axisLabel: {
                color: axisColor,
                fontSize: 10,
                rotate: props.data.length > 10 ? 45 : 0,
            },
            axisLine: { lineStyle: { color: axisColor } },
            axisTick: { show: false },
        },
        yAxis: {
            type: 'value',
            axisLabel: {
                color: axisColor,
                fontSize: 10,
                formatter: formatNumber,
            },
            splitLine: {
                lineStyle: { color: isDark ? CHART_AXIS_COLORS.grid.dark : CHART_AXIS_COLORS.grid.light },
            },
        },
        series: [
            {
                type: 'bar',
                data: props.data.map(d => d.rowCount),
                itemStyle: {
                    color: barColor,
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
