<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import { getPaginationRowModel } from '@tanstack/vue-table'
import type { Backfill } from '~/types/backfill'

const PAGE_SIZE = 20

const UBadge = resolveComponent('UBadge')
const EntityBadge = resolveComponent('EntityBadge')

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
const search = ref('')
const showSinglePartition = ref(false)
const shown = computed(() => {
    const needle = search.value.trim().toLowerCase()
    return backfills.value.filter(backfill =>
        (showSinglePartition.value || backfill.partitions !== 1)
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
        cell: ({ row }) => {
            const status = row.getValue<string>('status')
            return h(UBadge, { color: statusColor(status) }, () => statusLabel(status))
        },
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
        cell: ({ row }) => h('span', { class: 'text-muted' }, row.getValue<number | null>('partitions') ?? '—'),
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
    <div class="flex flex-col flex-1 min-h-0 gap-2">
        <div class="flex items-center gap-3">
            <UInput v-model="search"
                    placeholder="Search backfills by target..."
                    icon="i-lucide-search"
                    class="max-w-sm" />
            <UCheckbox v-model="showSinglePartition"
                       label="Show single partition"
                       class="ml-auto" />
        </div>

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

        <template v-else>
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

            <TableFooter class="shrink-0"
                         :page="pagination.pageIndex + 1"
                         :total="shown.length"
                         :page-size="PAGE_SIZE"
                         @update:page="(p: number) => pagination = { ...pagination, pageIndex: p - 1 }">
                {{ shown.length }} backfill(s) total.
            </TableFooter>
        </template>
    </div>
</template>
