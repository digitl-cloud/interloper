<script setup lang="ts">
import VChart from 'vue-echarts'
import { BarChart, LineChart, PieChart, ScatterChart } from 'echarts/charts'
import {
    GridComponent,
    TooltipComponent,
    LegendComponent,
    TitleComponent,
    MarkLineComponent,
    MarkAreaComponent,
    DataZoomComponent,
} from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { use } from 'echarts/core'

use([
    CanvasRenderer,
    BarChart, LineChart, PieChart, ScatterChart,
    GridComponent, TooltipComponent, LegendComponent, TitleComponent,
    MarkLineComponent, MarkAreaComponent, DataZoomComponent,
])

definePageMeta({ layout: 'analytics' })

const colorMode = useColorMode()

type Palette10 = [string, string, string, string, string, string, string, string, string, string]

const palette = computed(() => {
    const dark = colorMode.value === 'dark'
    const colors: Palette10 = [
        '#6366f1', '#22d3ee', '#f59e0b', '#ef4444', '#10b981',
        '#8b5cf6', '#ec4899', '#14b8a6', '#f97316', '#64748b',
    ]
    return {
        dark,
        text: dark ? '#e5e7eb' : '#1f2937',
        subtext: dark ? '#9ca3af' : '#6b7280',
        axis: dark ? '#4b5563' : '#d1d5db',
        split: dark ? '#374151' : '#f3f4f6',
        bg: dark ? '#111827' : '#ffffff',
        colors,
    }
})

function base(title: string, extra: Record<string, any> = {}) {
    const p = palette.value
    return {
        backgroundColor: 'transparent',
        title: { text: title, left: 'left', textStyle: { color: p.text, fontSize: 14, fontWeight: 600 } },
        tooltip: { trigger: 'axis', backgroundColor: p.bg, borderColor: p.axis, textStyle: { color: p.text, fontSize: 12 } },
        legend: { textStyle: { color: p.subtext, fontSize: 11 }, bottom: -5, left: 'center' },
        grid: { top: 64, left: 12, right: 12, bottom: 40, containLabel: true },
        ...extra,
    }
}

const weeks = Array.from({ length: 52 }, (_, i) => `W${i + 1}`)

// ---------- fake data generators ----------
function seasonal(base: number, amplitude: number, phase: number, noise: number) {
    return weeks.map((_, i) => Math.max(0, Math.round(base + amplitude * Math.sin((i + phase) * Math.PI / 26) + (Math.random() - 0.5) * noise)))
}

// 1 — Revenue decomposition (stacked area)
const tvContrib = seasonal(120, 40, 0, 20)
const digitalContrib = seasonal(90, 30, 4, 15)
const socialContrib = seasonal(60, 25, 8, 12)
const printContrib = seasonal(30, 10, 2, 8)
const baselineContrib = weeks.map(() => Math.round(200 + (Math.random() - 0.5) * 20))
const totalRevenue = weeks.map((_, i) => tvContrib[i]! + digitalContrib[i]! + socialContrib[i]! + printContrib[i]! + baselineContrib[i]!)

const decompositionOption = computed(() => ({
    ...base('Revenue Decomposition'),
    xAxis: { type: 'category', data: weeks, axisLine: { lineStyle: { color: palette.value.axis } }, axisLabel: { color: palette.value.subtext, fontSize: 9, interval: 3 } },
    yAxis: { type: 'value', splitLine: { lineStyle: { color: palette.value.split } }, axisLabel: { color: palette.value.subtext, fontSize: 10, formatter: (v: number) => `$${(v / 1000).toFixed(0)}k` } },
    series: [
        { name: 'Baseline', type: 'line', stack: 'total', areaStyle: { opacity: 0.6 }, data: baselineContrib, lineStyle: { width: 0 }, symbol: 'none' },
        { name: 'TV', type: 'line', stack: 'total', areaStyle: { opacity: 0.6 }, data: tvContrib, lineStyle: { width: 0 }, symbol: 'none' },
        { name: 'Digital', type: 'line', stack: 'total', areaStyle: { opacity: 0.6 }, data: digitalContrib, lineStyle: { width: 0 }, symbol: 'none' },
        { name: 'Social', type: 'line', stack: 'total', areaStyle: { opacity: 0.6 }, data: socialContrib, lineStyle: { width: 0 }, symbol: 'none' },
        { name: 'Print', type: 'line', stack: 'total', areaStyle: { opacity: 0.6 }, data: printContrib, lineStyle: { width: 0 }, symbol: 'none' },
        { name: 'Actual Revenue', type: 'line', data: totalRevenue.map(v => v + Math.round((Math.random() - 0.5) * 30)), lineStyle: { width: 2, type: 'dashed', color: palette.value.text }, itemStyle: { color: palette.value.text }, symbol: 'none' },
    ],
    color: [palette.value.colors[9], palette.value.colors[0], palette.value.colors[1], palette.value.colors[2], palette.value.colors[3]],
    dataZoom: [{ type: 'slider', start: 0, end: 100, height: 18, bottom: 24, textStyle: { color: palette.value.subtext, fontSize: 9 } }],
}))

// 2 — Channel contribution share (donut)
const contributionOption = computed(() => ({
    ...base('Channel Contribution Share', { tooltip: { trigger: 'item', backgroundColor: palette.value.bg, borderColor: palette.value.axis, textStyle: { color: palette.value.text, fontSize: 12 }, formatter: (p: any) => `<b>${p.name}</b><br/>${p.percent}% of incremental revenue` } }),
    legend: { bottom: -5, left: 'center', textStyle: { color: palette.value.subtext, fontSize: 11 } },
    series: [{
        type: 'pie',
        radius: ['38%', '64%'],
        center: ['50%', '45%'],
        itemStyle: { borderRadius: 6, borderColor: palette.value.bg, borderWidth: 2 },
        label: { show: false },
        emphasis: { label: { show: true, fontSize: 13, fontWeight: 'bold', color: palette.value.text } },
        data: [
            { value: 34, name: 'TV' },
            { value: 28, name: 'Digital' },
            { value: 20, name: 'Social' },
            { value: 12, name: 'Print' },
            { value: 6, name: 'OOH' },
        ],
    }],
    color: palette.value.colors,
}))

// 3 — ROI by channel (horizontal bar)
const roiOption = computed(() => ({
    ...base('Return on Investment by Channel'),
    xAxis: { type: 'value', splitLine: { lineStyle: { color: palette.value.split } }, axisLabel: { color: palette.value.subtext, fontSize: 10, formatter: (v: number) => `${v}x` } },
    yAxis: { type: 'category', data: ['OOH', 'Print', 'Social', 'Digital', 'TV'], axisLine: { lineStyle: { color: palette.value.axis } }, axisLabel: { color: palette.value.subtext, fontSize: 11 } },
    series: [{
        type: 'bar',
        data: [
            { value: 1.8, itemStyle: { color: palette.value.colors[9] } },
            { value: 2.4, itemStyle: { color: palette.value.colors[3] } },
            { value: 3.6, itemStyle: { color: palette.value.colors[2] } },
            { value: 4.2, itemStyle: { color: palette.value.colors[1] } },
            { value: 5.1, itemStyle: { color: palette.value.colors[0] } },
        ],
        barWidth: '50%',
        itemStyle: { borderRadius: [0, 4, 4, 0] },
        label: { show: true, position: 'right', formatter: '{c}x', color: palette.value.subtext, fontSize: 11 },
    }],
}))

// 4 — Adstock / saturation curves (spend vs response)
const saturationOption = computed(() => ({
    ...base('Saturation Curves (Spend vs Incremental Revenue)', { tooltip: { trigger: 'item', backgroundColor: palette.value.bg, borderColor: palette.value.axis, textStyle: { color: palette.value.text, fontSize: 12 }, formatter: (p: any) => `<b>${p.seriesName}</b><br/>Spend: $${p.data[0].toLocaleString()}<br/>Revenue: $${p.data[1].toLocaleString()}` } }),
    xAxis: { name: 'Weekly Spend ($)', nameLocation: 'center', nameGap: 28, nameTextStyle: { color: palette.value.subtext, fontSize: 11 }, type: 'value', splitLine: { lineStyle: { color: palette.value.split } }, axisLabel: { color: palette.value.subtext, fontSize: 10, formatter: (v: number) => `$${(v / 1000).toFixed(0)}k` } },
    yAxis: { name: 'Incremental Revenue ($)', nameLocation: 'center', nameGap: 40, nameTextStyle: { color: palette.value.subtext, fontSize: 11 }, type: 'value', splitLine: { lineStyle: { color: palette.value.split } }, axisLabel: { color: palette.value.subtext, fontSize: 10, formatter: (v: number) => `$${(v / 1000).toFixed(0)}k` } },
    series: ['TV', 'Digital', 'Social', 'Print'].map((name, idx) => {
        const maxSpend = [50000, 40000, 30000, 20000][idx]!
        const maxResp = [250000, 180000, 110000, 50000][idx]!
        const hill = [0.7, 0.8, 0.6, 0.5][idx]!
        const data = Array.from({ length: 30 }, (_, i) => {
            const spend = (i + 1) * (maxSpend / 30)
            const x = spend / maxSpend
            const response = maxResp * (Math.pow(x, hill) / (Math.pow(x, hill) + Math.pow(0.5, hill)))
            return [Math.round(spend), Math.round(response)]
        })
        return { name, type: 'line', smooth: true, data, symbol: 'none', lineStyle: { width: 2.5 } }
    }),
    color: [palette.value.colors[0], palette.value.colors[1], palette.value.colors[2], palette.value.colors[3]],
}))

// 5 — Adstock decay visualization
const adstockOption = computed(() => {
    const decayRates = [0.85, 0.70, 0.50, 0.30]
    const channels = ['TV', 'Digital', 'Social', 'Print']
    const weeksShort = Array.from({ length: 12 }, (_, i) => `W${i}`)
    return {
        ...base('Adstock Decay Effect'),
        xAxis: { type: 'category', data: weeksShort, axisLine: { lineStyle: { color: palette.value.axis } }, axisLabel: { color: palette.value.subtext, fontSize: 10 } },
        yAxis: { type: 'value', min: 0, max: 100, splitLine: { lineStyle: { color: palette.value.split } }, axisLabel: { color: palette.value.subtext, fontSize: 10, formatter: '{value}%' } },
        series: channels.map((name, idx) => ({
            name,
            type: 'line',
            smooth: true,
            data: weeksShort.map((_, w) => Math.round(100 * Math.pow(decayRates[idx]!, w))),
            lineStyle: { width: 2 },
            symbol: 'circle',
            symbolSize: 4,
        })),
        color: [palette.value.colors[0], palette.value.colors[1], palette.value.colors[2], palette.value.colors[3]],
    }
})

// 6 — Budget optimization: current vs optimal (grouped bar)
const budgetChannels = ['TV', 'Digital', 'Social', 'Print', 'OOH']
const budgetOptOption = computed(() => ({
    ...base('Budget Allocation: Current vs Optimized'),
    xAxis: { type: 'category', data: budgetChannels, axisLine: { lineStyle: { color: palette.value.axis } }, axisLabel: { color: palette.value.subtext, fontSize: 11 } },
    yAxis: { type: 'value', splitLine: { lineStyle: { color: palette.value.split } }, axisLabel: { color: palette.value.subtext, fontSize: 10, formatter: (v: number) => `$${(v / 1000).toFixed(0)}k` } },
    series: [
        { name: 'Current', type: 'bar', data: [45000, 35000, 25000, 15000, 10000], itemStyle: { borderRadius: [4, 4, 0, 0] } },
        { name: 'Optimized', type: 'bar', data: [38000, 42000, 30000, 8000, 12000], itemStyle: { borderRadius: [4, 4, 0, 0] } },
    ],
    color: [palette.value.colors[9], palette.value.colors[4]],
}))

// 7 — Model fit: actual vs predicted (line)
const modelFitOption = computed(() => {
    const actual = seasonal(500, 150, 0, 40)
    const predicted = actual.map(v => v + Math.round((Math.random() - 0.5) * 30))
    return {
        ...base('Model Fit: Actual vs Predicted Revenue'),
        xAxis: { type: 'category', data: weeks, axisLine: { lineStyle: { color: palette.value.axis } }, axisLabel: { color: palette.value.subtext, fontSize: 9, interval: 3 } },
        yAxis: { type: 'value', splitLine: { lineStyle: { color: palette.value.split } }, axisLabel: { color: palette.value.subtext, fontSize: 10, formatter: (v: number) => `$${(v / 1000).toFixed(0)}k` } },
        series: [
            { name: 'Actual', type: 'line', data: actual, lineStyle: { width: 2 }, symbol: 'none' },
            { name: 'Predicted', type: 'line', data: predicted, lineStyle: { width: 2, type: 'dashed' }, symbol: 'none' },
        ],
        color: [palette.value.colors[0], palette.value.colors[3]],
        dataZoom: [{ type: 'inside', start: 0, end: 100 }],
    }
})

// ---------- KPI cards ----------
const kpis = [
    { label: 'Model R-squared', value: '0.94', delta: 'Excellent fit', icon: 'i-lucide-brain' },
    { label: 'Total Incremental', value: '$8.2M', delta: '62% of revenue', icon: 'i-lucide-trending-up' },
    { label: 'Avg MROI', value: '3.8x', delta: 'Across all channels', icon: 'i-lucide-calculator' },
    { label: 'Optimization Lift', value: '+14%', delta: 'With same budget', icon: 'i-lucide-sparkles' },
]
</script>

<template>
    <div class="h-full overflow-y-auto">
        <div class="max-w-[1600px] mx-auto px-4 py-6 space-y-6">
            <!-- Header -->
            <div class="flex items-center justify-between">
                <div>
                    <h1 class="text-2xl font-bold">Marketing Mix Modeling</h1>
                    <p class="text-sm text-muted mt-1">Media contribution analysis &amp; budget optimization</p>
                </div>
                <div class="flex items-center gap-2">
                    <UBadge color="primary" variant="subtle" size="lg">Bayesian MMM</UBadge>
                    <UBadge variant="subtle" size="lg">52-week window</UBadge>
                </div>
            </div>

            <!-- KPI row -->
            <div class="grid grid-cols-2 lg:grid-cols-4 gap-3">
                <div v-for="kpi in kpis"
                    :key="kpi.label"
                    class="rounded-xl border border-default bg-default/50 backdrop-blur px-4 py-3 space-y-1"
                >
                    <div class="flex items-center gap-2 text-muted">
                        <UIcon :name="kpi.icon" class="size-4" />
                        <span class="text-xs font-medium uppercase tracking-wide">{{ kpi.label }}</span>
                    </div>
                    <p class="text-xl font-bold">{{ kpi.value }}</p>
                    <p class="text-xs text-muted">{{ kpi.delta }}</p>
                </div>
            </div>

            <!-- Row 1: Revenue decomposition (full width) -->
            <div class="rounded-xl border border-default bg-default/50 backdrop-blur p-4">
                <VChart :option="decompositionOption" style="height: 340px" autoresize />
            </div>

            <!-- Row 2: Contribution share + ROI -->
            <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
                <div class="rounded-xl border border-default bg-default/50 backdrop-blur p-4">
                    <VChart :option="contributionOption" style="height: 300px" autoresize />
                </div>
                <div class="lg:col-span-2 rounded-xl border border-default bg-default/50 backdrop-blur p-4">
                    <VChart :option="roiOption" style="height: 300px" autoresize />
                </div>
            </div>

            <!-- Row 3: Saturation + Adstock -->
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
                <div class="rounded-xl border border-default bg-default/50 backdrop-blur p-4">
                    <VChart :option="saturationOption" style="height: 320px" autoresize />
                </div>
                <div class="rounded-xl border border-default bg-default/50 backdrop-blur p-4">
                    <VChart :option="adstockOption" style="height: 320px" autoresize />
                </div>
            </div>

            <!-- Row 4: Budget optimization -->
            <div class="rounded-xl border border-default bg-default/50 backdrop-blur p-4">
                <VChart :option="budgetOptOption" style="height: 300px" autoresize />
            </div>

            <!-- Row 5: Model fit (full width) -->
            <div class="rounded-xl border border-default bg-default/50 backdrop-blur p-4">
                <VChart :option="modelFitOption" style="height: 300px" autoresize />
            </div>
        </div>
    </div>
</template>
