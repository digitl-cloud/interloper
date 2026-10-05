<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { Page } from '~/composables/api'
import type { Run } from '~/types/run'
import type { Backfill } from '~/types/backfill'

// orgSwitchTarget: this page is bespoke to one org's backfill — switching org
// from the nav lands on the backfills list instead.
definePageMeta({ orgSwitchTarget: '/executions/backfills' })

const PAGE_SIZE = 50

const UBadge = resolveComponent('UBadge')

const route = useRoute()
const backfillId = route.params.backfill!.toString()

const { apiFetch } = useApi()
const backfillsStore = useBackfillsStore()
const toast = useToast()
const { confirm } = useConfirm()

const backfill = ref<Backfill | null>(null)
const backfillRuns = ref<Run[]>([])
const runsTotal = ref(0)
const runsPage = ref(0)
const runsLoading = ref(false)
const sorting = ref([{ id: 'partition_key', desc: false }])
// The server orders the runs, so a sort spans every page rather than the one loaded.
const sortingOptions = { manualSorting: true }

async function fetchRuns() {
    runsLoading.value = true
    try {
        const params = new URLSearchParams({
            backfill_id: backfillId,
            limit: String(PAGE_SIZE),
            offset: String(runsPage.value * PAGE_SIZE),
        })
        const [order] = sorting.value
        if (order) params.set('sort', `${order.desc ? '-' : ''}${order.id}`)
        const page = await apiFetch<Page<Run>>(`/runs?${params}`)
        backfillRuns.value = page.items
        runsTotal.value = page.total
    }
    finally {
        runsLoading.value = false
    }
}

function reloadRuns() {
    fetchRuns().catch(e => toast.add(errorToast(e, 'Failed to load runs')))
}

function onPageChange(page: number) {
    runsPage.value = page - 1
    reloadRuns()
}

watch(sorting, () => {
    runsPage.value = 0
    reloadRuns()
})

const { mismatch } = useOrgGate(() => backfill.value?.org_id)

const cancellable = computed(() =>
    !mismatch.value && backfill.value != null && ['running', 'queued'].includes(backfill.value.status),
)
const cancelling = ref(false)

async function onCancel() {
    const confirmed = await confirm({
        title: 'Cancel backfill',
        description: 'Every run that has not finished is canceled. Runs already in flight stop within seconds.',
        confirmLabel: 'Cancel backfill',
        confirmColor: 'error',
        icon: 'i-lucide-ban',
    })
    if (!confirmed) return

    cancelling.value = true
    try {
        backfill.value = await backfillsStore.cancelBackfill(backfillId)
        await fetchRuns()
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
    try {
        const [fetchedBackfill] = await Promise.all([backfillsStore.fetchOne(backfillId), fetchRuns()])
        backfill.value = fetchedBackfill
    }
    catch (e) {
        fetchError.value = e
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
    <UDashboardPanel id="backfill">
        <template #header>
            <AppNavbar>
                <template #title>
                    <ULink to="/executions/backfills"
                           class="text-base font-medium text-muted hover:text-highlighted">Backfills</ULink>
                    <span class="text-base text-dimmed">/</span>
                    <span class="truncate font-mono text-base font-semibold">{{ backfillId.substring(0, 8) }}</span>
                    <StatusPill v-if="backfill && !mismatch"
                                :label="statusLabel(backfill.status)"
                                :color="statusPillColor(backfill.status)" />
                </template>
            </AppNavbar>
            <UDashboardToolbar v-if="cancellable">
                <template #right>
                    <UButton color="error"
                             variant="subtle"
                             icon="i-lucide-ban"
                             :loading="cancelling"
                             @click="onCancel">
                        Cancel
                    </UButton>
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <OrganizationGate :org-id="backfill?.org_id"
                              :error="fetchError"
                              back-to="/executions/backfills"
                              resource-label="backfill">
                <div class="flex flex-1 min-h-0 flex-col gap-4">
                    <UCard v-if="backfill"
                           title="Backfill">
                        <div class="flex flex-wrap items-center gap-4 text-sm text-muted">
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
                    </UCard>

                    <UCard :ui="FILL_CARD_UI">
                        <UTable v-model:sorting="sorting"
                                :data="backfillRuns"
                                :columns="columns"
                                :loading="runsLoading"
                                :sorting-options="sortingOptions"
                                sticky
                                :ui="{ tr: 'cursor-pointer' }"
                                class="flex-1 min-h-0"
                                @select="(_e: Event, row: any) => navigateTo(`/executions/runs/${row.original.id}`)" />
                        <template #footer>
                            <TableFooter :page="runsPage + 1"
                                         :total="runsTotal"
                                         :page-size="PAGE_SIZE"
                                         @update:page="onPageChange">
                                {{ runsTotal }} run(s) total.
                            </TableFooter>
                        </template>
                    </UCard>
                </div>
            </OrganizationGate>
        </template>
    </UDashboardPanel>
</template>
