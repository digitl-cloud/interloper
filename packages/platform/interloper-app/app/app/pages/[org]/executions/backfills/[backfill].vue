<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { BreadcrumbItem, TableColumn } from '@nuxt/ui'
import type { Page } from '~/composables/api'
import type { Run } from '~/types/run'
import type { Backfill } from '~/types/backfill'

const PAGE_SIZE = 50

const StatusBadge = resolveComponent('StatusBadge')

const route = useRoute()
const backfillId = route.params.backfill!.toString()

const { apiFetch } = useOrgApi()
const backfillsStore = useBackfillsStore()
const organisationStore = useOrganisationStore()
const toast = useToast()
const appConfig = useAppConfig()
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
    fetchRuns().catch(e => toast.add(errorToast(e, "Couldn't load the runs")))
}

function onPageChange(page: number) {
    runsPage.value = page - 1
    reloadRuns()
}

watch(sorting, () => {
    runsPage.value = 0
    reloadRuns()
})

const cancellable = computed(() =>
    backfill.value != null && ['running', 'queued'].includes(backfill.value.status),
)
const cancelling = ref(false)

async function onCancel() {
    const confirmed = await confirm({
        title: 'Cancel backfill',
        description: 'Every run that has not finished is canceled. Runs already in flight stop within seconds.',
        confirmLabel: 'Cancel backfill',
        icon: appConfig.icons.cancel,
    })
    if (!confirmed) return

    cancelling.value = true
    try {
        backfill.value = await backfillsStore.cancelBackfill(backfillId)
        await fetchRuns()
        toast.add({ title: 'Backfill canceled', color: 'success' })
    }
    catch (e) {
        toast.add(errorToast(e, "Couldn't cancel the backfill"))
    }
    finally {
        cancelling.value = false
    }
}

const backfillTargetName = computed(() => backfill.value ? targetLabel(backfill.value) : '')

const fetchError = ref<unknown>(null)
const notFound = computed(() => (fetchError.value as { statusCode?: number } | null)?.statusCode === 404)

const crumbs = computed<BreadcrumbItem[]>(() => [
    { label: 'Backfills', to: organisationStore.path('/executions/backfills') },
    { label: shortId(backfillId) },
])

async function load() {
    fetchError.value = null
    try {
        const [fetchedBackfill] = await Promise.all([backfillsStore.fetchOne(backfillId), fetchRuns()])
        backfill.value = fetchedBackfill
    }
    catch (e) {
        fetchError.value = e
    }
}

onMounted(load)

const columns: TableColumn<Run>[] = withSortableHeaders([
    {
        accessorKey: 'id',
        header: 'ID',
        cell: ({ row }) => idCell(row.getValue<string>('id')),
    },
    {
        accessorKey: 'partition_key',
        header: 'Partition',
        cell: ({ row }) => h('span', { class: 'text-muted tabular-nums' }, row.getValue<string>('partition_key') || '—'),
    },
    {
        accessorKey: 'status',
        header: 'Status',
        cell: ({ row }) => h(StatusBadge, { status: row.getValue<string>('status') }),
    },
    {
        accessorKey: 'created_at',
        header: 'Created',
        cell: ({ row }) => h('span', { class: 'text-muted tabular-nums' }, formatDate(row.getValue<string>('created_at')) || '—'),
    },
    {
        accessorKey: 'started_at',
        header: 'Started',
        cell: ({ row }) => h('span', { class: 'text-muted tabular-nums' }, formatDate(row.getValue<string>('started_at')) || '—'),
    },
    {
        id: 'elapsed',
        header: 'Elapsed',
        cell: ({ row }) => {
            const run = row.original as Run
            return h('span', { class: 'text-muted tabular-nums' }, formatElapsed(run.started_at, run.completed_at) || '—')
        },
    },
])
</script>

<template>
    <UDashboardPanel id="backfill">
        <template #header>
            <AppNavbar>
                <template #title>
                    <UBreadcrumb :items="crumbs">
                        <template #item-label="{ item, active }">
                            <UTooltip v-if="active"
                                      :text="backfillId">
                                <span class="tabular-nums slashed-zero">{{ item.label }}</span>
                            </UTooltip>
                            <template v-else>{{ item.label }}</template>
                        </template>
                    </UBreadcrumb>
                    <StatusBadge v-if="backfill"
                                 :status="backfill.status" />
                </template>
            </AppNavbar>
            <UDashboardToolbar v-if="cancellable">
                <template #right>
                    <UButton label="Cancel backfill"
                             :icon="appConfig.icons.cancel"
                             color="error"
                             variant="soft"
                             :loading="cancelling"
                             @click="onCancel" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <UEmpty v-if="notFound"
                    :icon="appConfig.ui.icons.search"
                    title="Backfill not found"
                    :description="`No backfill ${shortId(backfillId)} in ${organisationStore.organisation?.name}.`"
                    :actions="[{
                        label: 'Back to backfills',
                        color: 'neutral',
                        variant: 'outline',
                        to: organisationStore.path('/executions/backfills'),
                    }]" />
            <LoadError v-else-if="fetchError"
                       title="Couldn't load the backfill"
                       :error="fetchError"
                       @retry="load" />
            <template v-else>
                <UCard v-if="backfill"
                       title="Backfill">
                    <div class="flex flex-wrap items-center gap-4 text-sm text-muted">
                        <EntityBadge :icon="targetIcon(backfill)"
                                     :label="backfillTargetName" />
                        <div class="flex items-center gap-1.5">
                            <UIcon :name="appConfig.icons.window"
                                   class="size-4" />
                            <span class="tabular-nums">{{ backfill.start_key }} → {{ backfill.end_key }}</span>
                        </div>
                        <div class="flex items-center gap-1.5">
                            <UIcon :name="appConfig.icons.partition"
                                   class="size-4" />
                            <span class="tabular-nums">{{ backfill.partitions }} {{ backfill.partitions === 1 ? 'partition' : 'partitions' }}</span>
                        </div>
                        <UBadge v-if="backfill.fail_fast"
                                color="neutral"
                                size="sm"
                                label="Fail fast" />
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
                            class="min-h-0 flex-1"
                            @select="(_e: Event, row: any) => navigateTo(organisationStore.path(`/executions/runs/${row.original.id}`))" />
                    <template #footer>
                        <TableFooter :page="runsPage + 1"
                                     :total="runsTotal"
                                     :page-size="PAGE_SIZE"
                                     noun="run"
                                     @update:page="onPageChange" />
                    </template>
                </UCard>
            </template>
        </template>
    </UDashboardPanel>
</template>
