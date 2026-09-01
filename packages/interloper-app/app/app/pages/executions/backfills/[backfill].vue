<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { Run } from '~/types/run'
import type { Backfill } from '~/types/backfill'

// orgSwitchTarget: this page is bespoke to one org's backfill — switching org
// from the nav lands on the backfills list instead.
definePageMeta({ title: 'Backfill', orgSwitchTarget: '/executions?tab=backfills', customNavbar: true })

const UBadge = resolveComponent('UBadge')

const route = useRoute()
const backfillId = route.params.backfill!.toString()

const { apiFetch } = useApi()
const backfillsStore = useBackfillsStore()
const toast = useToast()
const { confirm } = useConfirm()

const backfill = ref<Backfill | null>(null)
const backfillRuns = ref<Run[]>([])
const runsLoading = ref(false)
const sorting = ref([{ id: 'partition_key', desc: false }])

const cancellable = computed(() => backfill.value != null && ['running', 'queued'].includes(backfill.value.status))
const cancelling = ref(false)

async function onCancel() {
    const confirmed = await confirm({
        title: 'Cancel backfill',
        description: 'Runs that have not started yet will be canceled. Runs already in flight finish on their own.',
        confirmLabel: 'Cancel backfill',
        confirmColor: 'error',
        icon: 'i-lucide-ban',
    })
    if (!confirmed) return

    cancelling.value = true
    try {
        backfill.value = await backfillsStore.cancelBackfill(backfillId)
        backfillRuns.value = await apiFetch<Run[]>(`/runs?backfill_id=${backfillId}`)
        toast.add({ title: 'Backfill canceled', color: 'success' })
    }
    catch (e) {
        toast.add(errorToast(e, 'Failed to cancel backfill'))
    }
    finally {
        cancelling.value = false
    }
}

const backfillTargetName = computed(() => backfill.value ? targetLabel(backfill.value) : '')

const fetchError = ref<unknown>(null)

onMounted(async () => {
    runsLoading.value = true
    try {
        const [fetchedBackfill, runs] = await Promise.all([
            backfillsStore.fetchOne(backfillId),
            apiFetch<Run[]>(`/runs?backfill_id=${backfillId}`),
        ])
        backfill.value = fetchedBackfill
        backfillRuns.value = runs
    }
    catch (e) {
        fetchError.value = e
    }
    finally {
        runsLoading.value = false
    }
})

const columns: TableColumn<Run>[] = withSortableHeaders([
    {
        accessorKey: 'id',
        header: 'ID',
        cell: ({ row }) => h('span', { class: 'font-mono text-xs' }, row.getValue<string>('id').substring(0, 8)),
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
])
</script>

<template>
    <OrganizationGate :org-id="backfill?.org_id"
             :error="fetchError"
             back-to="/executions?tab=backfills"
             resource-label="backfill">
        <div>
            <NavTitle>
                <ULink to="/executions?tab=backfills"
                       class="text-[15px] font-medium text-muted hover:text-highlighted">Backfills</ULink>
                <span class="text-[15px] text-dimmed">/</span>
                <span class="truncate font-mono text-[15px] font-semibold">{{ backfillId.substring(0, 8) }}</span>
                <StatusPill v-if="backfill"
                            :label="statusLabel(backfill.status)"
                            :color="statusPillColor(backfill.status)" />
            </NavTitle>
            <NavActions v-if="cancellable">
                <UButton color="error"
                         variant="subtle"
                         size="sm"
                         icon="i-lucide-ban"
                         :loading="cancelling"
                         @click="onCancel">
                    Cancel
                </UButton>
            </NavActions>

        <div v-if="backfill"
             class="flex items-center gap-4 mb-4 text-sm text-muted">
            <div class="flex items-center gap-1.5">
                <UIcon name="i-lucide-briefcase"
                       class="size-4" />
                <span>{{ backfillTargetName }}</span>
            </div>
            <div class="flex items-center gap-1.5">
                <UIcon name="i-lucide-calendar-range"
                       class="size-4" />
                <span>{{ backfill.start_key }} → {{ backfill.end_key }}</span>
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

        <UTable v-model:sorting="sorting"
                :data="backfillRuns"
                :columns="columns"
                :loading="runsLoading"
                sticky
                :ui="{ tr: 'cursor-pointer' }"
                class="flex-1"
                @select="(_e: Event, row: any) => navigateTo(`/executions/runs/${row.original.id}`)" />
        </div>
    </OrganizationGate>
</template>
