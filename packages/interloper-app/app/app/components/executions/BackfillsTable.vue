<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import { getPaginationRowModel } from '@tanstack/vue-table'
import type { Backfill } from '~/types/backfill'

const PAGE_SIZE = 20

const UBadge = resolveComponent('UBadge')

const backfillsStore = useBackfillsStore()
const componentsStore = useComponentsStore()
const { backfills, loading } = storeToRefs(backfillsStore)

/** Reactive map so cell render functions pick up changes. */
const jobNameMap = computed(() => {
    const map = new Map<string, string>()
    for (const job of componentsStore.byKind('job')) {
        map.set(job.id, job.name ?? job.key)
    }
    return map
})

onMounted(async () => {
    await Promise.all([
        !loading.value ? backfillsStore.fetch() : Promise.resolve(),
        componentsStore.fetchAll(['job']),
    ])
})

function jobName(backfill: Backfill): string {
    return backfill.job?.name ?? jobNameMap.value.get(backfill.job_id ?? '') ?? 'Deleted'
}

const pagination = ref({ pageIndex: 0, pageSize: PAGE_SIZE })
const sorting = ref([{ id: 'started_at', desc: true }])

const columns: TableColumn<Backfill>[] = withSortableHeaders([
    {
        accessorKey: 'id',
        header: 'ID',
        cell: ({ row }) => h('span', { class: 'font-mono text-xs' }, row.getValue<string>('id').substring(0, 8)),
    },
    {
        id: 'job',
        header: 'Job',
        cell: ({ row }) => {
            const backfill = row.original as Backfill
            return h(UBadge, { color: 'neutral', variant: 'subtle' }, () => jobName(backfill))
        },
    },
    {
        accessorKey: 'status',
        header: 'Status',
        cell: ({ row }) => {
            const status = row.getValue<string>('status')
            return h(UBadge, { color: statusColor(status), variant: 'subtle' }, () => statusLabel(status))
        },
    },
    {
        id: 'range',
        header: 'Range',
        cell: ({ row }) => {
            const backfill = row.original as Backfill
            return h('span', { class: 'text-muted text-xs' }, `${backfill.start_date} → ${backfill.end_date}`)
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
    <div class="flex flex-col flex-1 min-h-0">
        <div v-if="!loading && backfills.length === 0"
             class="w-full max-w-[1040px] mx-auto">
            <EmptyState icon="i-lucide-history"
                        title="No backfills yet"
                        description="Backfills re-run historical partitions to fill gaps or reprocess past data. Trigger one from a job and every partition it replays shows up here.">
                <UButton icon="i-lucide-calendar-plus"
                         label="Go to Jobs"
                         class="mt-5"
                         to="/jobs" />
            </EmptyState>
        </div>

        <template v-else>
            <UTable v-model:pagination="pagination"
                    v-model:sorting="sorting"
                    :data="backfills"
                    :columns="columns"
                    :loading="loading"
                    :pagination-options="{ getPaginationRowModel: getPaginationRowModel() }"
                    sticky
                    @select="(_e: Event, row: any) => navigateTo(`/executions/backfills/${row.original.id}`)" />

            <TableFooter class="py-3"
                         :page="pagination.pageIndex + 1"
                         :total="backfills.length"
                         :page-size="PAGE_SIZE"
                         @update:page="(p: number) => pagination = { ...pagination, pageIndex: p - 1 }">
                {{ backfills.length }} backfill(s) total.
            </TableFooter>
        </template>
    </div>
</template>
