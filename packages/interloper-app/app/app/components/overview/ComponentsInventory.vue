<script setup lang="ts">
import { h } from 'vue'
import { UIcon } from '#components'
import type { KindInventory } from '~/types/overview'

const organisationStore = useOrganisationStore()

const props = defineProps<{ rows: KindInventory[] | null }>()

const KIND_META: Record<string, { label: string, icon: string, to: string }> = {
    source: { label: 'Sources', icon: 'i-lucide-plug', to: kindPath('source') },
    asset: { label: 'Assets', icon: 'i-lucide-box', to: '/collection' },
    destination: { label: 'Destinations', icon: 'i-lucide-database', to: kindPath('destination') },
    connection: { label: 'Connections', icon: 'i-lucide-key-round', to: kindPath('connection') },
    job: { label: 'Jobs', icon: 'i-lucide-calendar-clock', to: kindPath('job') },
    hook: { label: 'Hooks', icon: 'i-carbon-lightning', to: kindPath('hook') },
}

/** A kind's row: its label, icon and list page in the organisation. */
function kindMeta(kind: string): { label: string, icon: string, to: string } {
    const meta = KIND_META[kind] ?? { label: kindLabel(kind), icon: 'i-lucide-box', to: kindPath(kind) }
    return { ...meta, to: organisationStore.path(meta.to) }
}

/** In bar and legend order: from healthy to failing, then the inactive ones. */
const STATES = [
    { key: 'healthy', label: 'healthy', legend: 'Healthy', class: 'bg-success' },
    { key: 'attention', label: 'needs attention', legend: 'Needs attention', class: 'bg-warning' },
    { key: 'failing', label: 'failing', legend: 'Failing', class: 'bg-error' },
    { key: 'disabled', label: 'disabled', legend: 'Disabled', class: 'bg-accented' },
] as const
/** The issues read worst first. */
const ISSUES = ['failing', 'attention', 'disabled'] as const

const table = computed(() => (props.rows ?? []).map(row => ({
    ...row,
    meta: kindMeta(row.kind),
    segments: STATES.filter(s => row[s.key]).map(s => ({
        ...s,
        pct: (100 * row[s.key]) / row.total,
        title: `${row[s.key]} ${s.label}`,
    })),
    issues: ISSUES.filter(key => row[key]).map(key => `${row[key]} ${STATES.find(s => s.key === key)!.label}`).join(' · ') || 'all healthy',
    issuesClass: row.failing ? 'text-error' : row.attention ? 'text-warning' : 'text-dimmed',
})))

const editor = useCanEdit()

const empty = computed(() => !!props.rows && props.rows.every(r => !r.total))

const summary = computed(() => {
    if (!props.rows || empty.value) return undefined
    const total = props.rows.reduce((n, r) => n + r.total, 0)
    const problems = props.rows.reduce((n, r) => n + r.failing + r.attention, 0)
    return `${total} in the collection · ${problems} need attention`
})

const columns = withSkeletons<(typeof table.value)[number]>([
    {
        id: 'kind',
        header: 'Kind',
        cell: ({ row }) => h('span', { class: 'flex items-center gap-2.5 font-medium text-highlighted' }, [
            h(UIcon, { name: row.original.meta.icon, class: 'size-4 shrink-0 text-dimmed' }),
            row.original.meta.label,
        ]),
    },
    {
        accessorKey: 'total',
        header: 'Count',
        meta: { class: { th: 'text-right', td: 'text-right font-semibold tabular-nums text-highlighted' } },
    },
    {
        id: 'state',
        header: 'State',
        meta: { class: { td: 'w-full' } },
        cell: ({ row }) => h('div', { class: 'flex h-2 gap-0.5 overflow-hidden rounded-full bg-accented' },
            row.original.segments.map(s => h('div', { key: s.key, class: s.class, style: { width: `${s.pct}%` }, title: s.title }))),
    },
    {
        id: 'issues',
        header: 'Issues',
        meta: { class: { th: 'text-right', td: 'text-right text-xs' } },
        cell: ({ row }) => h('span', { class: row.original.issuesClass }, row.original.issues),
    },
])
</script>

<template>
    <UCard>
        <template #header>
            <CardHeader title="Components"
                        :description="summary"
                        :loading="!rows">
                <UButton label="Open collection"
                         :to="organisationStore.path('/collection')"
                         color="neutral"
                         variant="outline"
                         size="sm" />
            </CardHeader>
        </template>
        <UEmpty v-if="empty"
                icon="i-lucide-library"
                title="Nothing in the collection yet"
                description="Add a source to start pulling data. Its assets, connections and jobs are counted here."
                :actions="editor ? [{ label: 'Add a source', icon: 'i-lucide-plus', to: organisationStore.path(kindPath('source')) }] : undefined"
                variant="naked" />
        <UTable v-else
                :data="rows ? table : skeletonRows(Object.keys(KIND_META).length)"
                :columns="columns"
                :ui="{ tr: 'cursor-pointer' }"
                @select="(_e: Event, row: any) => isSkeletonRow(row.original) || navigateTo(row.original.meta.to)" />
        <template v-if="!empty"
                  #footer>
            <div class="flex items-center gap-4 text-xs text-muted">
                <span v-for="state in STATES"
                      :key="state.key"
                      class="inline-flex items-center gap-1.5"><span class="size-2 rounded-sm"
                                                                     :class="state.class" />{{ state.legend }}</span>
            </div>
        </template>
    </UCard>
</template>
