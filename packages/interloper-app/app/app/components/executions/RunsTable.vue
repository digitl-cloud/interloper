<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { Run } from '~/types/run'

const UBadge = resolveComponent('UBadge')
const EntityBadge = resolveComponent('EntityBadge')

const route = useRoute()
const router = useRouter()
const runsStore = useRunsStore()
const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()
const { runs, loading, total, pageIndex, pageSize, filters, filtered } = storeToRefs(runsStore)

/**
 * Filters live in the store (the fetch, the pagination and the realtime gate
 * read them) and mirror to the route query so a filtered view is linkable
 * and survives the tab switch. The query is written, never watched: the
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

watchDebounced(search, value => applyFilters({ q: value.trim() }), { debounce: 300 })

async function applyFilters(next: Partial<RunFilters>) {
    await runsStore.setFilters(next)
    const { q, kind, key } = filters.value
    router.replace({ query: { ...route.query, q: q || undefined, kind: kind ?? undefined, type: key ?? undefined } })
}

function clearFilters() {
    search.value = ''
    applyFilters({ q: '', kind: null, key: null })
}

/** Type choices follow the kind in view, so the two filters never contradict. */
const typeChoices = computed(() => filters.value.kind
    ? componentsStore.byKind(filters.value.kind)
    : componentsStore.components)

onMounted(async () => {
    // Target icons and type names read the catalog; the filter choices read the collection.
    if (!catalogStore.loaded) catalogStore.fetchCatalog()
    if (componentsStore.components.length === 0) componentsStore.fetchAll()
    const { q, kind, type } = route.query
    await runsStore.setFilters({
        q: typeof q === 'string' ? q : '',
        kind: typeof kind === 'string' ? kind : null,
        key: typeof type === 'string' ? type : null,
    })
})

onUnmounted(runsStore.clearFilters)

const columns: TableColumn<Run>[] = [
    {
        accessorKey: 'id',
        header: 'ID',
        cell: ({ row }) => h('span', { class: 'font-mono text-xs' }, row.getValue<string>('id').substring(0, 8)),
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
        cell: ({ row }) => {
            const status = row.getValue<string>('status')
            return h(UBadge, { color: statusColor(status) }, () => statusLabel(status))
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
    <div class="flex flex-col flex-1 min-h-0 gap-2">
        <div class="flex items-center gap-3">
            <UInput v-model="search"
                    placeholder="Search runs by target..."
                    icon="i-lucide-search"
                    class="max-w-sm" />
            <KindFilter v-model="kind"
                        :components="componentsStore.components" />
            <TypeFilter v-model="type"
                        :components="typeChoices" />
        </div>

        <div v-if="!loading && runs.length === 0 && filtered"
             class="flex flex-col items-center gap-3 py-16 text-sm text-muted">
            No runs match these filters.
            <UButton variant="ghost"
                     icon="i-lucide-x"
                     label="Clear filters"
                     @click="clearFilters" />
        </div>

        <div v-else-if="!loading && runs.length === 0"
             class="w-full max-w-[1040px] mx-auto">
            <EmptyState icon="i-lucide-activity"
                        title="No executions yet"
                        description="Executions are the run history of your pipelines. Every time a job runs — on schedule or triggered manually — each materialized partition appears here with its status and timing.">
                <UButton icon="i-lucide-calendar-plus"
                         label="Create a job"
                         class="mt-5"
                         to="/jobs" />
            </EmptyState>
        </div>

        <template v-else>
            <UTable :data="runs"
                    :columns="columns"
                    :loading="loading"
                    :sorting="[{ id: 'created_at', desc: true }]"
                    sticky
                    :ui="{ tr: 'cursor-pointer' }"
                    @select="(_e: Event, row: any) => navigateTo(`/executions/runs/${row.original.id}`)" />

            <TableFooter class="py-3"
                         :page="pageIndex + 1"
                         :total="total"
                         :page-size="pageSize"
                         @update:page="onPageChange">
                {{ total }} run(s) total.
            </TableFooter>
        </template>
    </div>
</template>
