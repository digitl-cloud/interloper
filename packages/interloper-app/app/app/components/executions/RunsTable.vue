<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { Run } from '~/types/run'

const UBadge = resolveComponent('UBadge')
const UButton = resolveComponent('UButton')
const EntityBadge = resolveComponent('EntityBadge')
const StatusBadge = resolveComponent('StatusBadge')
const UProgressGroup = resolveComponent('UProgressGroup')

type RunRow = Run & { children?: Run[] }

const route = useRoute()
const router = useRouter()
const runsStore = useRunsStore()
const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()
const { runs, stacks, loading, total, pageIndex, pageSize, filters, filtered } = storeToRefs(runsStore)

/**
 * A row is a stack at its latest attempt; expanding it lists the attempts
 * before, which load on first expand. The latest is left out of its own
 * children so no attempt appears twice, which also keeps row ids unique.
 */
const rows = computed<RunRow[]>(() => runs.value.map((run) => {
    const attempts = stacks.value[run.root_run_id ?? run.id]
    return attempts ? { ...run, children: attempts.filter(attempt => attempt.id !== run.id) } : run
}))

const expanded = ref<Record<string, boolean>>({})
// Stable references: fresh ones on every render make TanStack rebuild the row
// model, and the default auto-reset would collapse a stack the moment its
// attempts finish loading.
const getRowId = (run: RunRow) => run.id
const getSubRows = (run: RunRow) => run.children
const expandedOptions = {
    autoResetExpanded: false,
    getRowCanExpand: (row: { depth: number, original: RunRow }) => row.depth === 0 && row.original.attempt > 1,
}
// UTable renders a detail row under every expanded row for its `#expanded`
// slot, even with sub-rows; with no slot its single spanning cell is empty.
const tableUi = { tr: 'cursor-pointer [&:has(>td[colspan]:empty)]:hidden' }

/**
 * Filters live in the store (the fetch, the pagination and the realtime gate
 * read them) and mirror to the route query so a filtered view is linkable.
 * The query is written, never watched: the
 * store is the single source of truth while this table is mounted.
 */
const search = ref(String(route.query.q ?? ''))
const kind = computed({
    get: () => filters.value.kind,
    set: (value: string | null) => applyFilters({ kind: value, key: null }),
})
const type = computed({
    get: () => filters.value.key,
    set: (value: string | null) => applyFilters({ key: value }),
})
const status = computed({
    get: () => filters.value.status,
    set: (value: string | null) => applyFilters({ status: value }),
})

const RUN_STATUSES = ['pending', 'queued', 'dispatched', 'running', 'success', 'failed', 'canceled']

watchDebounced(search, value => applyFilters({ q: value.trim() }), { debounce: 300 })

async function applyFilters(next: Partial<RunFilters>) {
    await runsStore.setFilters(next)
    const { q, kind, key, status } = filters.value
    router.replace({
        query: { ...route.query, q: q || undefined, kind: kind ?? undefined, type: key ?? undefined, status: status ?? undefined },
    })
}

function clearFilters() {
    search.value = ''
    applyFilters({ q: '', kind: null, key: null, status: null })
}

/** Type choices follow the kind in view, so the two filters never contradict. */
const typeChoices = computed(() => filters.value.kind
    ? componentsStore.byKind(filters.value.kind)
    : componentsStore.all)

onMounted(async () => {
    // Target icons and type names read the catalog; the filter choices read the collection.
    if (!catalogStore.loaded) catalogStore.fetchCatalog()
    if (componentsStore.all.length === 0) componentsStore.fetchAll()
    const { q, kind, type, status } = route.query
    await runsStore.setFilters({
        q: typeof q === 'string' ? q : '',
        kind: typeof kind === 'string' ? kind : null,
        key: typeof type === 'string' ? type : null,
        status: typeof status === 'string' ? status : null,
    })
})

onUnmounted(runsStore.clearFilters)

/** A stack's attempt count on its own row, an earlier attempt's ordinal on its; none for a single attempt. */
function attemptBadge(run: Run, depth: number) {
    if (depth > 0) {
        return h(UBadge, { color: 'neutral', variant: 'outline', size: 'sm', title: `Attempt ${run.attempt}` }, () => `#${run.attempt}`)
    }
    if (run.attempt <= 1) return null
    return h(UBadge, {
        color: 'neutral',
        size: 'sm',
        icon: 'i-lucide-rotate-ccw',
        title: `${run.attempt} attempts`,
    }, () => String(run.attempt))
}

const columns: TableColumn<RunRow>[] = [
    {
        accessorKey: 'id',
        header: 'ID',
        cell: ({ row }) => {
            const run = row.original
            const open = row.getIsExpanded()
            return h('div', { class: 'flex items-center gap-1.5', style: { paddingInlineStart: `${row.depth * 1.5}rem` } }, [
                h(UButton, {
                    'color': 'neutral',
                    'variant': 'ghost',
                    'size': 'xs',
                    'icon': open ? 'i-lucide-chevron-down' : 'i-lucide-chevron-right',
                    'class': row.getCanExpand() ? undefined : 'invisible',
                    'aria-label': open ? 'Hide earlier attempts' : 'Show earlier attempts',
                    'onClick': (event: Event) => {
                        event.stopPropagation()
                        if (!open) runsStore.loadStack(run.root_run_id ?? run.id)
                        row.toggleExpanded()
                    },
                }),
                h('span', { class: 'font-mono text-xs' }, run.id.substring(0, 8)),
                attemptBadge(run, row.depth),
            ])
        },
    },
    {
        id: 'target',
        header: 'Target',
        cell: ({ row }) => {
            const run = row.original as Run
            return h(EntityBadge, { label: targetLabel(run), icon: targetIcon(run) })
        },
    },
    {
        accessorKey: 'partition_key',
        header: 'Partition',
        cell: ({ row }) => h('span', { class: 'text-muted' }, row.getValue<string>('partition_key') || '—'),
    },
    {
        accessorKey: 'status',
        header: 'Status',
        cell: ({ row }) => h(StatusBadge, { status: row.getValue<string>('status') }),
    },
    {
        id: 'assets',
        header: 'Assets',
        cell: ({ row }) => {
            const stats = runStats(row.original, row.original.execution_counts ?? {})
            if (!stats.total) return h('span', { class: 'text-muted' }, '—')
            return h('div', { class: 'flex w-40 items-center gap-2', title: outcomeSummary(stats).join(' · ') }, [
                h(UProgressGroup, { items: progressSegments(stats), max: stats.total, size: 'sm', class: 'flex-1' }),
                h('span', { class: 'text-xs text-muted tabular-nums' }, `${stats.succeeded}/${stats.total}`),
            ])
        },
    },
    {
        accessorKey: 'created_at',
        header: 'Created',
        cell: ({ row }) => h('span', { class: 'text-muted' }, formatDate(row.getValue<string>('created_at')) || '—'),
    },
    {
        accessorKey: 'started_at',
        header: 'Started',
        cell: ({ row }) => h('span', { class: 'text-muted' }, formatDate(row.getValue<string>('started_at')) || '—'),
    },
    {
        id: 'elapsed',
        header: 'Elapsed',
        cell: ({ row }) => {
            const run = row.original as Run
            return h('span', { class: 'text-muted' }, formatElapsed(run.started_at, run.completed_at) || '—')
        },
    },
]


function onPageChange(page: number) {
    runsStore.goToPage(page - 1)
}
</script>

<template>
    <div class="flex flex-col flex-1 min-h-0">
        <div v-if="!loading && runs.length === 0 && !filtered"
             class="w-full max-w-[1040px] mx-auto">
            <EmptyState icon="i-lucide-activity"
                        title="No executions yet"
                        description="Executions are the run history of your pipelines. Every time a job runs — on schedule or triggered manually — each materialized partition appears here with its status and timing.">
                <UButton icon="i-lucide-calendar-plus"
                         label="Create a job"
                         class="mt-5"
                         :to="kindPath('job')" />
            </EmptyState>
        </div>

        <UCard v-else
               :ui="FILL_CARD_UI">
            <template #header>
                <div class="flex flex-wrap items-center gap-3">
                    <UInput v-model="search"
                            placeholder="Search runs by target..."
                            icon="i-lucide-search"
                            class="max-w-sm" />
                    <KindFilter v-model="kind"
                                :components="componentsStore.all" />
                    <TypeFilter v-model="type"
                                :components="typeChoices" />
                    <StatusFilter v-model="status"
                                  :statuses="RUN_STATUSES" />
                </div>
            </template>

            <div v-if="!loading && runs.length === 0"
                 class="flex flex-col items-center gap-3 py-16 text-sm text-muted">
                No runs match these filters.
                <UButton variant="ghost"
                         icon="i-lucide-x"
                         label="Clear filters"
                         @click="clearFilters" />
            </div>
            <UTable v-else
                    v-model:expanded="expanded"
                    :data="rows"
                    :columns="columns"
                    :get-row-id="getRowId"
                    :get-sub-rows="getSubRows"
                    :expanded-options="expandedOptions"
                    :loading="loading"
                    :sorting="[{ id: 'created_at', desc: true }]"
                    sticky
                    class="flex-1 min-h-0"
                    :ui="tableUi"
                    @select="(_e: Event, row: any) => navigateTo(`/executions/runs/${row.original.id}`)" />

            <template #footer>
                <TableFooter :page="pageIndex + 1"
                             :total="total"
                             :page-size="pageSize"
                             @update:page="onPageChange">
                    {{ total }} run(s) total.
                </TableFooter>
            </template>
        </UCard>
    </div>
</template>
