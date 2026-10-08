<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import { getPaginationRowModel } from '@tanstack/vue-table'
import type { Backfill } from '~/types/backfill'

const PAGE_SIZE = 50

const EntityBadge = resolveComponent('EntityBadge')
const StatusBadge = resolveComponent('StatusBadge')
const UProgressGroup = resolveComponent('UProgressGroup')

const BACKFILL_STATUSES = ['queued', 'running', 'success', 'failed', 'canceled']

const backfillsStore = useBackfillsStore()
const catalogStore = useCatalogStore()
const { backfills, loading } = storeToRefs(backfillsStore)

onMounted(async () => {
    // Target icons read the catalog's per-type icon.
    if (!catalogStore.loaded) catalogStore.fetchCatalog()
    if (!loading.value) await backfillsStore.fetch()
})

/**
 * The list is loaded whole, so filtering is local. Single-partition
 * backfills are what a manual partition run creates; they outnumber real
 * range backfills and hide them, so they stay out unless asked for.
 */
const search = useQueryParam('q', '')
const status = useQueryParam('status', null)
const showSinglePartition = useQueryParam('single', false, booleanQuery)
const shown = computed(() => {
    const needle = search.value.trim().toLowerCase()
    return backfills.value.filter(backfill =>
        (showSinglePartition.value || backfill.partitions !== 1)
        && (!status.value || backfill.status === status.value)
        && (!needle || [backfill.component_name, backfill.component_key].some(text => text?.toLowerCase().includes(needle))),
    )
})

const pagination = ref({ pageIndex: 0, pageSize: PAGE_SIZE })
const sorting = ref([{ id: 'started_at', desc: true }])
watch(shown, () => { pagination.value = { ...pagination.value, pageIndex: 0 } })

const columns: TableColumn<Backfill>[] = withSortableHeaders([
    {
        accessorKey: 'id',
        header: 'ID',
        cell: ({ row }) => h('span', { class: 'font-mono text-xs' }, row.getValue<string>('id').substring(0, 8)),
    },
    {
        id: 'target',
        header: 'Target',
        cell: ({ row }) => {
            const backfill = row.original as Backfill
            return h(EntityBadge, { label: targetLabel(backfill), icon: targetIcon(backfill) })
        },
    },
    {
        accessorKey: 'status',
        header: 'Status',
        cell: ({ row }) => h(StatusBadge, { status: row.getValue<string>('status') }),
    },
    {
        id: 'range',
        header: 'Range',
        cell: ({ row }) => {
            const backfill = row.original as Backfill
            return h('span', { class: 'text-muted text-xs' }, `${backfill.start_key} → ${backfill.end_key}`)
        },
    },
    {
        accessorKey: 'partitions',
        header: 'Partitions',
        cell: ({ row }) => {
            const stats = runStats(null, row.original.run_counts ?? {})
            if (!stats.total) return h('span', { class: 'text-muted' }, '—')
            return h('div', { class: 'flex w-40 items-center gap-2', title: outcomeSummary(stats).join(' · ') }, [
                h(UProgressGroup, { items: progressSegments(stats), max: stats.total, size: 'sm', class: 'flex-1' }),
                h('span', { class: 'text-xs text-muted tabular-nums' }, `${stats.succeeded}/${stats.total}`),
            ])
        },
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
            const backfill = row.original as Backfill
            return h('span', { class: 'text-muted' }, formatElapsed(backfill.started_at, backfill.completed_at) || '—')
        },
    },
])
</script>

<template>
    <div class="flex flex-col flex-1 min-h-0">
        <div v-if="!loading && backfills.length === 0"
             class="w-full max-w-[1040px] mx-auto">
            <EmptyState icon="i-lucide-history"
                        title="No backfills yet"
                        description="Backfills re-run historical partitions to fill gaps or reprocess past data. Trigger one from a job and every partition it replays shows up here.">
                <UButton icon="i-lucide-calendar-plus"
                         label="Go to Jobs"
                         class="mt-5"
                         :to="kindPath('job')" />
            </EmptyState>
        </div>

        <UCard v-else
               :ui="FILL_CARD_UI">
            <template #header>
                <div class="flex flex-wrap items-center gap-3">
                    <UInput v-model="search"
                            placeholder="Search backfills by target..."
                            icon="i-lucide-search"
                            class="max-w-sm" />
                    <StatusFilter v-model="status"
                                  :statuses="BACKFILL_STATUSES" />
                    <UCheckbox v-model="showSinglePartition"
                               label="Show single partition" />
                </div>
            </template>

            <UTable v-model:pagination="pagination"
                    v-model:sorting="sorting"
                    :data="shown"
                    :columns="columns"
                    :loading="loading"
                    :pagination-options="{ getPaginationRowModel: getPaginationRowModel() }"
                    sticky
                    class="flex-1 min-h-0"
                    :ui="{ tr: 'cursor-pointer' }"
                    @select="(_e: Event, row: any) => navigateTo(`/executions/backfills/${row.original.id}`)" />

            <template #footer>
                <TableFooter :page="pagination.pageIndex + 1"
                             :total="shown.length"
                             :page-size="PAGE_SIZE"
                             @update:page="(p: number) => pagination = { ...pagination, pageIndex: p - 1 }">
                    {{ shown.length }} backfill(s) total.
                </TableFooter>
            </template>
        </UCard>
    </div>
</template>
