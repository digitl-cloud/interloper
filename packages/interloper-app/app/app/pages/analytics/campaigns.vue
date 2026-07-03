<script setup lang="ts">
import VChart from 'vue-echarts'
import { BarChart, LineChart, PieChart, RadarChart, ScatterChart, HeatmapChart, GaugeChart } from 'echarts/charts'
import {
    GridComponent,
    TooltipComponent,
    LegendComponent,
    TitleComponent,
    VisualMapComponent,
    CalendarComponent,
    DataZoomComponent,
    MarkLineComponent,
    MarkAreaComponent,
    RadarComponent,
} from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { use } from 'echarts/core'

use([
    CanvasRenderer,
    BarChart, LineChart, PieChart, RadarChart, ScatterChart, HeatmapChart, GaugeChart,
    GridComponent, TooltipComponent, LegendComponent, TitleComponent,
    VisualMapComponent, CalendarComponent, DataZoomComponent,
    MarkLineComponent, MarkAreaComponent, RadarComponent,
])

definePageMeta({ layout: 'analytics' })

const colorMode = useColorMode()

// ---------- palette helpers ----------
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
        cardBg: dark ? 'rgba(17,24,39,0.6)' : 'rgba(255,255,255,0.6)',
        colors,
    }
})

function base(title: string, extra: Record<string, any> = {}) {
    const p = palette.value
    return {
        backgroundColor: 'transparent',
        title: {
            text: title,
            left: 'left',
            textStyle: { color: p.text, fontSize: 14, fontWeight: 600 },
        },
        tooltip: { trigger: 'axis', backgroundColor: p.bg, borderColor: p.axis, textStyle: { color: p.text, fontSize: 12 } },
        legend: { textStyle: { color: p.subtext, fontSize: 11 }, bottom: -5, left: 'center' },
        grid: { top: 64, left: 12, right: 12, bottom: 40, containLabel: true },
        ...extra,
    }
}

// ---------- fake data ----------
const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
const channels = ['Email', 'Social', 'Paid Search', 'Display', 'Referral', 'Organic']

// 1 — Revenue over time (line + area)
const revenueOption = computed(() => ({
    ...base('Revenue by Channel'),
    xAxis: { type: 'category', data: months, axisLine: { lineStyle: { color: palette.value.axis } }, axisLabel: { color: palette.value.subtext, fontSize: 10 } },
    yAxis: { type: 'value', splitLine: { lineStyle: { color: palette.value.split } }, axisLabel: { color: palette.value.subtext, fontSize: 10, formatter: (v: number) => `$${(v / 1000).toFixed(0)}k` } },
    series: [
        { name: 'Email', type: 'line', smooth: true, areaStyle: { opacity: 0.15 }, data: [42, 48, 55, 53, 61, 72, 69, 81, 87, 92, 98, 110].map(v => v * 1000), lineStyle: { width: 2 }, symbol: 'circle', symbolSize: 4 },
        { name: 'Paid Search', type: 'line', smooth: true, areaStyle: { opacity: 0.15 }, data: [31, 35, 40, 45, 42, 55, 60, 58, 67, 72, 80, 88].map(v => v * 1000), lineStyle: { width: 2 }, symbol: 'circle', symbolSize: 4 },
        { name: 'Social', type: 'line', smooth: true, areaStyle: { opacity: 0.15 }, data: [18, 22, 28, 32, 38, 35, 42, 48, 52, 58, 61, 70].map(v => v * 1000), lineStyle: { width: 2 }, symbol: 'circle', symbolSize: 4 },
    ],
    color: [palette.value.colors[0], palette.value.colors[1], palette.value.colors[2]],
    dataZoom: [{ type: 'inside', start: 0, end: 100 }],
}))

// 2 — Campaign spend vs conversions (scatter)
const scatterOption = computed(() => ({
    ...base('Spend vs Conversions', { tooltip: { trigger: 'item', backgroundColor: palette.value.bg, borderColor: palette.value.axis, textStyle: { color: palette.value.text, fontSize: 12 }, formatter: (p: any) => `<b>${p.data[3]}</b><br/>Spend: $${p.data[0].toLocaleString()}<br/>Conversions: ${p.data[1]}<br/>CPA: $${p.data[2].toFixed(2)}` } }),
    xAxis: { name: 'Spend ($)', nameLocation: 'center', nameGap: 28, nameTextStyle: { color: palette.value.subtext, fontSize: 11 }, type: 'value', splitLine: { lineStyle: { color: palette.value.split } }, axisLabel: { color: palette.value.subtext, fontSize: 10, formatter: (v: number) => `$${(v / 1000).toFixed(0)}k` } },
    yAxis: { name: 'Conversions', nameLocation: 'center', nameGap: 36, nameTextStyle: { color: palette.value.subtext, fontSize: 11 }, type: 'value', splitLine: { lineStyle: { color: palette.value.split } }, axisLabel: { color: palette.value.subtext, fontSize: 10 } },
    series: [{
        type: 'scatter',
        symbolSize: (d: number[]) => Math.max(8, Math.min(32, (d[2] ?? 0) * 0.6)),
        data: [
            [12000, 340, 35.3, 'Spring Sale'],
            [8500, 210, 40.5, 'Brand Awareness'],
            [25000, 890, 28.1, 'Product Launch'],
            [6200, 180, 34.4, 'Newsletter Push'],
            [18000, 620, 29.0, 'Retargeting'],
            [9800, 275, 35.6, 'Influencer Collab'],
            [15000, 510, 29.4, 'Holiday Promo'],
            [4500, 95, 47.4, 'Webinar Series'],
            [21000, 780, 26.9, 'Summer Blitz'],
            [7200, 190, 37.9, 'Content Series'],
            [30000, 1100, 27.3, 'Black Friday'],
            [11000, 320, 34.4, 'Loyalty Program'],
        ],
        itemStyle: { color: palette.value.colors[0], opacity: 0.8 },
    }],
}))

// 3 — Channel mix (pie / donut)
const channelMixOption = computed(() => ({
    ...base('Channel Mix (Budget)', { tooltip: { trigger: 'item', backgroundColor: palette.value.bg, borderColor: palette.value.axis, textStyle: { color: palette.value.text, fontSize: 12 }, formatter: (p: any) => `<b>${p.name}</b><br/>$${p.value.toLocaleString()} (${p.percent}%)` } }),
    legend: { bottom: -5, left: 'center', textStyle: { color: palette.value.subtext, fontSize: 11 } },
    series: [{
        type: 'pie',
        radius: ['38%', '64%'],
        center: ['50%', '45%'],
        avoidLabelOverlap: true,
        itemStyle: { borderRadius: 6, borderColor: palette.value.bg, borderWidth: 2 },
        label: { show: false },
        emphasis: { label: { show: true, fontSize: 13, fontWeight: 'bold', color: palette.value.text } },
        data: [
            { value: 42000, name: 'Paid Search' },
            { value: 28000, name: 'Social' },
            { value: 18000, name: 'Email' },
            { value: 15000, name: 'Display' },
            { value: 9000, name: 'Referral' },
            { value: 6000, name: 'Organic' },
        ],
    }],
    color: palette.value.colors,
}))

// 4 — Funnel-style stacked bar (impressions → clicks → conversions)
const funnelBarOption = computed(() => ({
    ...base('Campaign Funnel by Channel'),
    xAxis: { type: 'value', splitLine: { lineStyle: { color: palette.value.split } }, axisLabel: { color: palette.value.subtext, fontSize: 10, formatter: (v: number) => v >= 1000 ? `${(v / 1000).toFixed(0)}k` : String(v) } },
    yAxis: { type: 'category', data: channels, axisLine: { lineStyle: { color: palette.value.axis } }, axisLabel: { color: palette.value.subtext, fontSize: 10 } },
    series: [
        { name: 'Impressions', type: 'bar', stack: 'funnel', data: [580, 420, 310, 260, 180, 150].map(v => v * 1000), itemStyle: { borderRadius: [0, 0, 0, 0] } },
        { name: 'Clicks', type: 'bar', stack: 'funnel', data: [29, 21, 18, 10, 9, 12].map(v => v * 1000) },
        { name: 'Conversions', type: 'bar', stack: 'funnel', data: [2900, 2100, 1500, 800, 720, 980], itemStyle: { borderRadius: [0, 4, 4, 0] } },
    ],
    color: [palette.value.colors[4], palette.value.colors[1], palette.value.colors[0]],
}))

// 5 — Radar: channel strength
const radarOption = computed(() => ({
    ...base('Channel Strength Index', { tooltip: { trigger: 'item', backgroundColor: palette.value.bg, borderColor: palette.value.axis, textStyle: { color: palette.value.text, fontSize: 12 } } }),
    radar: {
        center: ['50%', '45%'],
        radius: '60%',
        indicator: [
            { name: 'Reach', max: 100 },
            { name: 'Engagement', max: 100 },
            { name: 'Conversion', max: 100 },
            { name: 'Retention', max: 100 },
            { name: 'ROI', max: 100 },
            { name: 'Brand Lift', max: 100 },
        ],
        axisName: { color: palette.value.subtext, fontSize: 10 },
        splitArea: { areaStyle: { color: palette.value.dark ? ['rgba(99,102,241,0.04)', 'rgba(99,102,241,0.08)'] : ['rgba(99,102,241,0.02)', 'rgba(99,102,241,0.06)'] } },
        splitLine: { lineStyle: { color: palette.value.split } },
        axisLine: { lineStyle: { color: palette.value.axis } },
    },
    series: [{
        type: 'radar',
        data: [
            { value: [92, 68, 75, 82, 88, 70], name: 'Email', areaStyle: { opacity: 0.15 } },
            { value: [85, 90, 60, 55, 62, 88], name: 'Social', areaStyle: { opacity: 0.15 } },
            { value: [78, 55, 88, 70, 92, 50], name: 'Paid Search', areaStyle: { opacity: 0.15 } },
        ],
        lineStyle: { width: 2 },
        symbol: 'circle',
        symbolSize: 5,
    }],
    color: [palette.value.colors[0], palette.value.colors[2], palette.value.colors[1]],
}))

// 6 — Conversion rate trend (line with mark areas)
const conversionTrendOption = computed(() => ({
    ...base('Conversion Rate Trend (%)'),
    xAxis: { type: 'category', data: months, axisLine: { lineStyle: { color: palette.value.axis } }, axisLabel: { color: palette.value.subtext, fontSize: 10 } },
    yAxis: { type: 'value', min: 0, max: 8, splitLine: { lineStyle: { color: palette.value.split } }, axisLabel: { color: palette.value.subtext, fontSize: 10, formatter: '{value}%' } },
    series: [{
        name: 'CVR',
        type: 'line',
        smooth: true,
        data: [3.2, 3.5, 3.8, 4.1, 3.9, 4.5, 4.8, 5.2, 5.0, 5.6, 6.1, 6.4],
        lineStyle: { width: 3, color: palette.value.colors[4] },
        itemStyle: { color: palette.value.colors[4] },
        areaStyle: { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: 'rgba(16,185,129,0.25)' }, { offset: 1, color: 'rgba(16,185,129,0)' }] } },
        markLine: { silent: true, data: [{ yAxis: 5, label: { formatter: 'Target 5%', color: palette.value.subtext, fontSize: 10 }, lineStyle: { type: 'dashed', color: palette.value.colors[3], opacity: 0.6 } }] },
        markArea: { silent: true, data: [[{ xAxis: 'Sep', itemStyle: { color: 'rgba(16,185,129,0.06)' } }, { xAxis: 'Dec' }]] },
    }],
}))

// 7 — Weekly heatmap
const days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
const hours = Array.from({ length: 24 }, (_, i) => `${i}:00`)
const heatmapData: number[][] = []
for (let d = 0; d < 7; d++) {
    for (let h = 0; h < 24; h++) {
        const peak = (h >= 9 && h <= 11) || (h >= 19 && h <= 21)
        const weekday = d < 5
        const base = weekday ? (peak ? 80 : 40) : (peak ? 50 : 20)
        heatmapData.push([h, d, Math.round(base + Math.random() * 30)])
    }
}

const heatmapOption = computed(() => ({
    ...base('Engagement Heatmap (Hour × Day)', { tooltip: { trigger: 'item', backgroundColor: palette.value.bg, borderColor: palette.value.axis, textStyle: { color: palette.value.text, fontSize: 12 }, formatter: (p: any) => `<b>${days[p.data[1]]} ${hours[p.data[0]]}</b><br/>Engagement: ${p.data[2]}` } }),
    grid: { top: 36, left: 56, right: 48, bottom: 8 },
    xAxis: { type: 'category', data: hours, splitArea: { show: true, areaStyle: { color: ['transparent'] } }, axisLabel: { color: palette.value.subtext, fontSize: 9, interval: 2 } },
    yAxis: { type: 'category', data: days, splitArea: { show: true, areaStyle: { color: ['transparent'] } }, axisLabel: { color: palette.value.subtext, fontSize: 10 } },
    visualMap: { min: 10, max: 110, calculable: true, orient: 'vertical', right: 0, top: 48, textStyle: { color: palette.value.subtext, fontSize: 10 }, inRange: { color: palette.value.dark ? ['#1e1b4b', '#4338ca', '#818cf8', '#c7d2fe'] : ['#eef2ff', '#a5b4fc', '#6366f1', '#3730a3'] } },
    series: [{ name: 'Engagement', type: 'heatmap', data: heatmapData, emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.3)' } } }],
}))

// 8 — ROI gauge
const gaugeOption = computed(() => ({
    backgroundColor: 'transparent',
    title: { text: 'Overall ROAS', left: 'left', textStyle: { color: palette.value.text, fontSize: 14, fontWeight: 600 } },
    series: [{
        type: 'gauge',
        startAngle: 200,
        endAngle: -20,
        min: 0,
        max: 8,
        center: ['50%', '60%'],
        radius: '90%',
        progress: { show: true, width: 14, roundCap: true, itemStyle: { color: palette.value.colors[0] } },
        pointer: { show: false },
        axisLine: { lineStyle: { width: 14, color: [[1, palette.value.dark ? '#1f2937' : '#e5e7eb']] } },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: { distance: 20, color: palette.value.subtext, fontSize: 10, formatter: (v: number) => `${v}x` },
        title: { show: true, offsetCenter: [0, '25%'], color: palette.value.subtext, fontSize: 12 },
        detail: { valueAnimation: true, fontSize: 32, fontWeight: 700, color: palette.value.colors[0], offsetCenter: [0, '-5%'], formatter: '{value}x' },
        data: [{ value: 4.7, name: 'Return on Ad Spend' }],
    }],
}))

// 9 — Daily spend bar chart
const dailySpendOption = computed(() => {
    const daysInMonth = Array.from({ length: 30 }, (_, i) => `Mar ${i + 1}`)
    const emailSpend = Array.from({ length: 30 }, () => Math.round(400 + Math.random() * 300))
    const socialSpend = Array.from({ length: 30 }, () => Math.round(300 + Math.random() * 250))
    const paidSpend = Array.from({ length: 30 }, () => Math.round(500 + Math.random() * 400))
    return {
        ...base('Daily Ad Spend (March)'),
        xAxis: { type: 'category', data: daysInMonth, axisLine: { lineStyle: { color: palette.value.axis } }, axisLabel: { color: palette.value.subtext, fontSize: 9, rotate: 45, interval: 2 } },
        yAxis: { type: 'value', splitLine: { lineStyle: { color: palette.value.split } }, axisLabel: { color: palette.value.subtext, fontSize: 10, formatter: (v: number) => `$${v}` } },
        series: [
            { name: 'Email', type: 'bar', stack: 'spend', data: emailSpend, itemStyle: { borderRadius: [0, 0, 0, 0] } },
            { name: 'Social', type: 'bar', stack: 'spend', data: socialSpend },
            { name: 'Paid Search', type: 'bar', stack: 'spend', data: paidSpend, itemStyle: { borderRadius: [2, 2, 0, 0] } },
        ],
        color: [palette.value.colors[0], palette.value.colors[2], palette.value.colors[1]],
        dataZoom: [{ type: 'slider', start: 0, end: 100, height: 18, bottom: 24, textStyle: { color: palette.value.subtext, fontSize: 9 } }],
    }
})

// ---------- KPI cards ----------
const kpis = [
    { label: 'Total Spend', value: '$118.2K', delta: '+12.4%', up: true, icon: 'i-lucide-wallet' },
    { label: 'Impressions', value: '2.4M', delta: '+8.1%', up: true, icon: 'i-lucide-eye' },
    { label: 'Conversions', value: '9,230', delta: '+24.6%', up: true, icon: 'i-lucide-target' },
    { label: 'Avg CPA', value: '$12.81', delta: '-6.3%', up: false, icon: 'i-lucide-trending-down' },
    { label: 'ROAS', value: '4.7x', delta: '+18.2%', up: true, icon: 'i-lucide-rocket' },
    { label: 'CTR', value: '3.82%', delta: '+0.4pp', up: true, icon: 'i-lucide-mouse-pointer-click' },
]
</script>

<template>
    <div class="h-full overflow-y-auto">
        <div class="max-w-[1600px] mx-auto px-4 py-6 space-y-6">
            <!-- Header -->
            <div class="flex items-center justify-between">
                <div>
                    <h1 class="text-2xl font-bold">Campaign Performance</h1>
                    <p class="text-sm text-muted mt-1">Marketing analytics &mdash; March 2026</p>
                </div>
                <div class="flex items-center gap-2">
                    <UBadge color="primary" variant="subtle" size="lg">Live</UBadge>
                    <UBadge variant="subtle" size="lg">Last 30 days</UBadge>
                </div>
            </div>

            <!-- KPI row -->
            <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
                <div v-for="kpi in kpis"
                    :key="kpi.label"
                    class="rounded-xl border border-default bg-default/50 backdrop-blur px-4 py-3 space-y-1"
                >
                    <div class="flex items-center gap-2 text-muted">
                        <UIcon :name="kpi.icon" class="size-4" />
                        <span class="text-xs font-medium uppercase tracking-wide">{{ kpi.label }}</span>
                    </div>
                    <p class="text-xl font-bold">{{ kpi.value }}</p>
                    <p class="text-xs" :class="kpi.up ? 'text-green-500' : 'text-red-500'">
                        {{ kpi.delta }} vs prior period
                    </p>
                </div>
            </div>

            <!-- Row 1: Revenue + Scatter -->
            <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
                <div class="lg:col-span-2 rounded-xl border border-default bg-default/50 backdrop-blur p-4">
                    <VChart :option="revenueOption" style="height: 320px" autoresize />
                </div>
                <div class="rounded-xl border border-default bg-default/50 backdrop-blur p-4">
                    <VChart :option="channelMixOption" style="height: 320px" autoresize />
                </div>
            </div>

            <!-- Row 2: Funnel + Radar + Gauge -->
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                <div class="rounded-xl border border-default bg-default/50 backdrop-blur p-4">
                    <VChart :option="funnelBarOption" style="height: 300px" autoresize />
                </div>
                <div class="rounded-xl border border-default bg-default/50 backdrop-blur p-4">
                    <VChart :option="radarOption" style="height: 300px" autoresize />
                </div>
                <div class="rounded-xl border border-default bg-default/50 backdrop-blur p-4">
                    <VChart :option="gaugeOption" style="height: 300px" autoresize />
                </div>
            </div>

            <!-- Row 3: Scatter + Conversion trend -->
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
                <div class="rounded-xl border border-default bg-default/50 backdrop-blur p-4">
                    <VChart :option="scatterOption" style="height: 300px" autoresize />
                </div>
                <div class="rounded-xl border border-default bg-default/50 backdrop-blur p-4">
                    <VChart :option="conversionTrendOption" style="height: 300px" autoresize />
                </div>
            </div>

            <!-- Row 4: Heatmap (full width) -->
            <div class="rounded-xl border border-default bg-default/50 backdrop-blur p-4">
                <VChart :option="heatmapOption" style="height: 280px" autoresize />
            </div>

            <!-- Row 5: Daily spend (full width with zoom) -->
            <div class="rounded-xl border border-default bg-default/50 backdrop-blur p-4">
                <VChart :option="dailySpendOption" style="height: 320px" autoresize />
            </div>
        </div>
    </div>
</template>
