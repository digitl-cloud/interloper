<script setup lang="ts">
import type { KindInventory } from '~/types/overview'

const props = defineProps<{ rows: KindInventory[] }>()

const KIND_META: Record<string, { label: string, icon: string, to: string }> = {
    source: { label: 'Sources', icon: 'i-lucide-plug', to: kindPath('source') },
    asset: { label: 'Assets', icon: 'i-lucide-box', to: '/collection' },
    destination: { label: 'Destinations', icon: 'i-lucide-database', to: kindPath('destination') },
    connection: { label: 'Connections', icon: 'i-lucide-key-round', to: kindPath('connection') },
    job: { label: 'Jobs', icon: 'i-lucide-calendar-clock', to: kindPath('job') },
    hook: { label: 'Hooks', icon: 'i-carbon-lightning', to: kindPath('hook') },
}

const STATES = [
    { key: 'failing', label: 'failing', legend: 'Failing', class: 'bg-error' },
    { key: 'attention', label: 'needs attention', legend: 'Needs attention', class: 'bg-warning' },
    { key: 'healthy', label: 'healthy', legend: 'Healthy', class: 'bg-success' },
    { key: 'disabled', label: 'disabled', legend: 'Disabled', class: 'bg-accented' },
] as const

const table = computed(() => props.rows.map(row => ({
    ...row,
    meta: KIND_META[row.kind] ?? { label: kindLabel(row.kind), icon: 'i-lucide-box', to: kindPath(row.kind) },
    segments: STATES.filter(s => row[s.key]).map(s => ({
        ...s,
        pct: (100 * row[s.key]) / row.total,
        title: `${row[s.key]} ${s.label}`,
    })),
    issues: STATES.filter(s => s.key !== 'healthy' && row[s.key]).map(s => `${row[s.key]} ${s.label}`).join(' · ') || 'all healthy',
    issuesClass: row.failing ? 'text-error' : row.attention ? 'text-warning' : 'text-dimmed',
})))

const summary = computed(() => {
    const total = props.rows.reduce((n, r) => n + r.total, 0)
    const problems = props.rows.reduce((n, r) => n + r.failing + r.attention, 0)
    return `${total} in the collection · ${problems} need attention`
})
</script>

<template>
    <OverviewSection title="Components"
                     :meta="summary"
                     link-label="Open collection"
                     link-to="/collection">
        <div class="overflow-hidden rounded-lg border border-default">
            <div class="grid grid-cols-[minmax(130px,180px)_44px_minmax(0,1fr)_230px] items-center gap-x-5 border-b border-default bg-muted px-[18px] py-[9px] text-[11px] font-semibold uppercase tracking-[.06em] text-dimmed">
                <span>Kind</span><span class="text-right">Count</span><span>State</span><span class="text-right">Issues</span>
            </div>
            <NuxtLink v-for="row in table"
                      :key="row.kind"
                      :to="row.meta.to"
                      class="grid h-11 grid-cols-[minmax(130px,180px)_44px_minmax(0,1fr)_230px] items-center gap-x-5 border-b border-muted px-[18px] text-highlighted transition-colors hover:bg-muted">
                <span class="flex min-w-0 items-center gap-2.5 text-[13.5px] font-medium">
                    <UIcon :name="row.meta.icon"
                           class="size-4 shrink-0 text-dimmed" />{{ row.meta.label }}
                </span>
                <span class="text-right text-sm font-semibold tabular-nums">{{ row.total }}</span>
                <div class="flex h-[9px] gap-0.5 overflow-hidden rounded-full bg-accented">
                    <div v-for="segment in row.segments"
                         :key="segment.key"
                         :class="segment.class"
                         :style="{ width: `${segment.pct}%` }"
                         :title="segment.title" />
                </div>
                <span class="text-right text-xs leading-snug"
                      :class="row.issuesClass">{{ row.issues }}</span>
            </NuxtLink>
            <div class="flex items-center gap-4 bg-muted px-[18px] py-2.5 text-[11.5px] text-dimmed">
                <span v-for="state in STATES"
                      :key="state.key"
                      class="inline-flex items-center gap-1.5"><span class="size-[9px] rounded-sm"
                                                                     :class="state.class" />{{ state.legend }}</span>
            </div>
        </div>
    </OverviewSection>
</template>
