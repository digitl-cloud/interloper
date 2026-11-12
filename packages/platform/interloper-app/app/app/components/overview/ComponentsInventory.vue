<script setup lang="ts">
import { h } from 'vue'
import { UIcon, UProgressGroup, UTooltip } from '#components'
import type { KindInventory } from '~/types/overview'

const organisationStore = useOrganisationStore()
const overviewStore = useOverviewStore()
const { error } = storeToRefs(overviewStore)
const { ui, icons } = useAppConfig()

const props = defineProps<{ rows: KindInventory[] | null }>()

const KIND_META: Record<string, { label: string, icon: string, to: string }> = {
    source: { label: 'Sources', icon: icons.source, to: kindPath('source') },
    asset: { label: 'Assets', icon: icons.asset, to: '/collection' },
    destination: { label: 'Destinations', icon: icons.destination, to: kindPath('destination') },
    connection: { label: 'Connections', icon: icons.connection, to: kindPath('connection') },
    job: { label: 'Jobs', icon: icons.job, to: kindPath('job') },
    hook: { label: 'Hooks', icon: icons.hook, to: kindPath('hook') },
}

/** A kind's row: its label, icon and list page in the organisation. */
function kindMeta(kind: string): { label: string, icon: string, to: string } {
    const meta = KIND_META[kind] ?? { label: kindLabel(kind), icon: icons.components, to: kindPath(kind) }
    return { ...meta, to: organisationStore.path(meta.to) }
}

/** In bar and legend order: from healthy to failing, then the inactive ones; colours as the graph's node states. */
const STATES = [
    { key: 'healthy', label: 'healthy', legend: 'Healthy', color: nodeStateColor('idle') },
    { key: 'attention', label: 'needs attention', legend: 'Needs attention', color: nodeStateColor('attention') },
    { key: 'failing', label: 'failing', legend: 'Failing', color: nodeStateColor('failed') },
    { key: 'disabled', label: 'disabled', legend: 'Disabled', color: nodeStateColor('paused') },
] as const
/** The issues read worst first. */
const ISSUES = ['failing', 'attention', 'disabled'] as const

const table = computed(() => (props.rows ?? []).map(row => ({
    ...row,
    meta: kindMeta(row.kind),
    segments: STATES.map(s => ({ value: row[s.key], color: STATUS_CSS[s.color] })),
    breakdown: STATES.filter(s => row[s.key]).map(s => `${row[s.key]} ${s.label}`).join(' · '),
    issues: ISSUES.filter(key => row[key]).map(key => `${row[key]} ${STATES.find(s => s.key === key)!.label}`).join(' · ') || 'All healthy',
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
        cell: ({ row }) => h('span', { class: 'flex items-center gap-2 font-medium text-highlighted' }, [
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
        cell: ({ row }) => h(UTooltip, { text: row.original.breakdown, delayDuration: 0 }, () =>
            h(UProgressGroup, { items: row.original.segments, max: row.original.total || 1 })),
    },
    {
        id: 'issues',
        header: 'Issues',
        meta: { class: { th: 'text-right', td: 'text-right text-xs tabular-nums' } },
        cell: ({ row }) => h('span', { class: row.original.issuesClass }, row.original.issues),
    },
])
</script>

<template>
    <UCard>
        <template #header>
            <CardHeader title="Components"
                        :description="summary"
                        :loading="!rows && !error">
                <UButton label="Open collection"
                         :to="organisationStore.path('/collection')"
                         color="neutral"
                         variant="outline"
                         size="sm" />
            </CardHeader>
        </template>
        <LoadError v-if="!rows && error"
                   title="Couldn't load the collection's components"
                   :error="error"
                   @retry="overviewStore.fetchOverview()" />
        <UEmpty v-else-if="empty"
                :icon="icons.collection"
                title="Nothing in the collection yet"
                description="Add a source to start pulling data. Its assets, connections and jobs are counted here."
                :actions="editor ? [{ label: 'New source', icon: ui.icons.plus, size: 'sm', to: { path: organisationStore.path(kindPath('source')), query: { new: '1' } } }] : undefined"
                variant="naked"
                size="sm" />
        <UTable v-else
                :data="rows ? table : skeletonRows(Object.keys(KIND_META).length)"
                :columns="columns"
                :ui="{ tr: 'cursor-pointer' }"
                @select="(_e: Event, row: any) => isSkeletonRow(row.original) || navigateTo(row.original.meta.to)" />
        <template v-if="!empty && !(!rows && error)"
                  #footer>
            <div class="flex flex-wrap items-center gap-4 text-xs text-muted">
                <span v-for="state in STATES"
                      :key="state.key"
                      class="inline-flex items-center gap-1.5">
                    <StatusDot :color="state.color" />{{ state.legend }}
                </span>
            </div>
        </template>
    </UCard>
</template>
