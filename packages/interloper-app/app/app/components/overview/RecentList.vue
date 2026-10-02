<script setup lang="ts">
import { h } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import { StatusPill } from '#components'
import type { Run } from '~/types/run'

const props = defineProps<{ runs: Run[] }>()

const DOT: Record<ReturnType<typeof statusPillColor>, string> = {
    success: 'bg-success',
    error: 'bg-error',
    primary: 'bg-primary',
    warning: 'bg-warning',
    neutral: 'bg-accented',
}

const rows = computed(() => props.runs.map((run) => {
    const value = run.completed_at ?? run.started_at ?? run.created_at
    const at = value ? new Date(value) : null
    return {
        run,
        color: statusPillColor(run.status),
        rel: at ? relativeTime(at) : '',
        at: at ? `${formatShortDay(at)} ${formatClockTime(at)}` : '',
    }
}))

type Row = (typeof rows.value)[number]

const columns: TableColumn<Row>[] = [
    {
        id: 'target',
        header: 'Target',
        cell: ({ row }) => h('span', { class: 'flex min-w-0 items-center gap-3' }, [
            h('span', { class: ['size-2 shrink-0 rounded-full', DOT[row.original.color]] }),
            h('span', { class: 'truncate font-mono text-xs text-highlighted' },
                row.original.run.component_name ?? row.original.run.component_key ?? 'Deleted target'),
        ]),
    },
    {
        id: 'status',
        header: 'Status',
        cell: ({ row }) => h(StatusPill, { label: row.original.run.status, color: row.original.color, dot: false, class: 'capitalize' }),
    },
    {
        id: 'when',
        header: 'When',
        cell: ({ row }) => h('span', { class: 'font-semibold text-highlighted' }, row.original.rel),
    },
    {
        id: 'at',
        header: 'Time',
        meta: { class: { th: 'text-right', td: 'text-right text-xs tabular-nums text-dimmed' } },
        cell: ({ row }) => row.original.at,
    },
]
</script>

<template>
    <UCard>
        <template #header>
            <CardHeader title="Just happened">
                <UButton label="All executions"
                         to="/executions/runs"
                         color="neutral"
                         variant="outline"
                         size="sm" />
            </CardHeader>
        </template>
        <UTable :data="rows"
                :columns="columns"
                empty="No runs yet."
                :ui="{ tr: 'cursor-pointer' }"
                @select="(_e: Event, row: any) => navigateTo(`/executions/runs/${row.original.run.id}`)" />
    </UCard>
</template>
