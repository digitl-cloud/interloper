<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { Run } from '~/types/run'
import type { Backfill } from '~/types/backfill'

definePageMeta({ title: 'Backfill' })

const UBadge = resolveComponent('UBadge')

const route = useRoute()
const backfillId = route.params.backfill!.toString()

const { apiFetch } = useApi()
const backfillsStore = useBackfillsStore()
const jobsStore = useJobsStore()

const backfill = ref<Backfill | null>(null)
const backfillRuns = ref<Run[]>([])
const runsLoading = ref(false)

const backfillJobName = computed(() =>
    backfill.value?.job?.name ?? jobsStore.findById(backfill.value?.job_id ?? '')?.name ?? 'Deleted job',
)

onMounted(async () => {
    runsLoading.value = true
    jobsStore.fetch()
    const [fetchedBackfill, runs] = await Promise.all([
        backfillsStore.fetchOne(backfillId),
        apiFetch<Run[]>(`/runs?backfill_id=${backfillId}`),
    ])
    backfill.value = fetchedBackfill
    backfillRuns.value = runs
    runsLoading.value = false
})

const columns: TableColumn<Run>[] = [
    {
        accessorKey: 'id',
        header: 'ID',
        cell: ({ row }) => h('span', { class: 'font-mono text-xs' }, row.getValue<string>('id').substring(0, 8)),
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
</script>

<template>
    <div>
        <div class="flex items-center gap-3 mb-4 shrink-0 px-4 pt-4">
            <NuxtLink to="/executions?tab=backfills"
                      class="text-sm text-muted hover:text-default">
                Backfills
            </NuxtLink>
            <span class="text-sm text-muted">/</span>
            <span class="text-sm font-medium font-mono">{{ backfillId.substring(0, 8) }}</span>
            <UBadge v-if="backfill"
                    :color="statusColor(backfill.status)"
                    variant="subtle">
                {{ statusLabel(backfill.status) }}
            </UBadge>
        </div>

        <div v-if="backfill"
             class="flex items-center gap-4 px-4 mb-4 text-sm text-muted">
            <div class="flex items-center gap-1.5">
                <UIcon name="i-lucide-briefcase"
                       class="size-4" />
                <span>{{ backfillJobName }}</span>
            </div>
            <div class="flex items-center gap-1.5">
                <UIcon name="i-lucide-calendar-range"
                       class="size-4" />
                <span>{{ backfill.start_date }} → {{ backfill.end_date }}</span>
            </div>
            <div class="flex items-center gap-1.5">
                <UIcon name="i-lucide-layers"
                       class="size-4" />
                <span>{{ backfill.partitions }} partitions</span>
            </div>
            <div v-if="backfill.fail_fast"
                 class="flex items-center gap-1.5">
                <UIcon name="i-lucide-zap"
                       class="size-4" />
                <span>Fail fast</span>
            </div>
        </div>

        <UTable :data="backfillRuns"
                :columns="columns"
                :loading="runsLoading"
                :sorting="[{ id: 'partition_date', desc: false }]"
                sticky
                class="flex-1"
                @select="(_e: Event, row: any) => navigateTo(`/executions/runs/${row.original.id}`)" />
    </div>
</template>
