<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { Run } from '~/types/run'

const UBadge = resolveComponent('UBadge')

const runsStore = useRunsStore()
const jobsStore = useJobsStore()
const { runs, loading, total, pageIndex, pageSize } = storeToRefs(runsStore)

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
        !loading.value ? runsStore.fetch() : Promise.resolve(),
        jobsStore.fetch(),
    ])
})

function jobName(run: Run): string {
    return run.job?.name ?? jobNameMap.value.get(run.job_id ?? '') ?? 'Deleted'
}

const columns: TableColumn<Run>[] = [
    {
        accessorKey: 'id',
        header: 'ID',
        cell: ({ row }) => h('span', { class: 'font-mono text-xs' }, row.getValue<string>('id').substring(0, 8)),
    },
    {
        id: 'job',
        header: 'Job',
        cell: ({ row }) => {
            const run = row.original as Run
            return h(UBadge, { color: 'neutral', variant: 'subtle' }, () => jobName(run))
        },
    },
    {
        accessorKey: 'partition_date',
        header: 'Partition',
        cell: ({ row }) => h('span', { class: 'text-muted' }, row.getValue<string>('partition_date') || '—'),
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

const totalPages = computed(() => Math.ceil(total.value / pageSize.value))

function onPageChange(page: number) {
    runsStore.goToPage(page - 1)
}
</script>

<template>
    <div class="flex flex-col flex-1 min-h-0">
        <UTable :data="runs"
                :columns="columns"
                :loading="loading"
                :sorting="[{ id: 'created_at', desc: true }]"
                sticky
                @select="(_e: Event, row: any) => navigateTo(`/executions/runs/${row.original.id}`)" />

        <div class="flex items-center justify-between text-sm text-muted px-4 py-3">
            <span>{{ total }} run(s) total.</span>
            <UPagination v-if="totalPages > 1"
                         :page="pageIndex + 1"
                         :total="total"
                         :items-per-page="pageSize"
                         :sibling-count="1"
                         show-edges
                         @update:page="onPageChange" />
        </div>
    </div>
</template>
