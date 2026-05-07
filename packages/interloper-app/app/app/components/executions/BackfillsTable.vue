<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import { getPaginationRowModel } from '@tanstack/vue-table'
import type { Backfill } from '~/types/backfill'

const PAGE_SIZE = 20

const UBadge = resolveComponent('UBadge')

const backfillsStore = useBackfillsStore()
const jobsStore = useJobsStore()
const { backfills, loading } = storeToRefs(backfillsStore)

/** Reactive map so cell render functions pick up changes. */
const jobNameMap = computed(() => {
    const map = new Map<string, string>()
    for (const job of jobsStore.jobs) {
        map.set(job.id, job.name)
    }
    return map
})

onMounted(async () => {
    await Promise.all([
        !loading.value ? backfillsStore.fetch() : Promise.resolve(),
        jobsStore.fetch(),
    ])
})

function jobName(backfill: Backfill): string {
    return backfill.job?.name ?? jobNameMap.value.get(backfill.job_id ?? '') ?? 'Deleted'
}

const pagination = ref({ pageIndex: 0, pageSize: PAGE_SIZE })
const totalPages = computed(() => Math.ceil(backfills.value.length / PAGE_SIZE))

const columns: TableColumn<Backfill>[] = [
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
]
</script>

<template>
    <div class="flex flex-col flex-1 min-h-0">
        <UTable v-model:pagination="pagination"
                :data="backfills"
                :columns="columns"
                :loading="loading"
                :sorting="[{ id: 'started_at', desc: true }]"
                :pagination-options="{ getPaginationRowModel: getPaginationRowModel() }"
                sticky
                @select="(_e: Event, row: any) => navigateTo(`/executions/backfills/${row.original.id}`)" />

        <div class="flex items-center justify-between text-sm text-muted px-4 py-3">
            <span>{{ backfills.length }} backfill(s) total.</span>
            <UPagination v-if="totalPages > 1"
                         :page="pagination.pageIndex + 1"
                         :total="backfills.length"
                         :items-per-page="PAGE_SIZE"
                         :sibling-count="1"
                         show-edges
                         @update:page="(p: number) => pagination = { ...pagination, pageIndex: p - 1 }" />
        </div>
    </div>
</template>
