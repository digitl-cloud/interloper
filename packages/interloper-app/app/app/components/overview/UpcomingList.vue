<script setup lang="ts">
import { h } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import { UIcon } from '#components'
import type { UpcomingRun } from '~/types/overview'

const props = defineProps<{ items: UpcomingRun[] }>()
const LIMIT = 4

const userStore = useUserStore()
const timezone = computed(() => userStore.user?.timezone ?? Intl.DateTimeFormat().resolvedOptions().timeZone)

const rows = computed(() => props.items.slice(0, LIMIT).map((item) => {
    const at = new Date(item.next_run_at)
    return {
        ...item,
        rel: relativeTime(at),
        at: `${formatShortDay(at)} ${formatClockTime(at)}`,
    }
}))

type Row = (typeof rows.value)[number]

const columns: TableColumn<Row>[] = [
    {
        id: 'job',
        header: 'Job',
        cell: ({ row }) => h('span', { class: 'flex min-w-0 items-center gap-3' }, [
            h(UIcon, { name: 'i-lucide-calendar-clock', class: 'size-4 shrink-0 text-dimmed' }),
            h('span', { class: 'truncate font-mono text-xs text-highlighted' }, row.original.job_name),
        ]),
    },
    {
        id: 'next',
        header: 'Next run',
        cell: ({ row }) => h('span', { class: 'font-semibold text-highlighted' }, row.original.rel),
    },
    {
        id: 'at',
        header: 'Scheduled',
        meta: { class: { th: 'text-right', td: 'text-right text-xs tabular-nums text-dimmed' } },
        cell: ({ row }) => row.original.at,
    },
]
</script>

<template>
    <UCard>
        <template #header>
            <CardHeader title="Coming up"
                        :description="timezone">
                <UButton label="All jobs"
                         :to="kindPath('job')"
                         color="neutral"
                         variant="outline"
                         size="sm" />
            </CardHeader>
        </template>
        <UTable :data="rows"
                :columns="columns"
                empty="Nothing scheduled."
                :ui="{ tr: 'cursor-pointer' }"
                @select="() => navigateTo(kindPath('job'))" />
    </UCard>
</template>
