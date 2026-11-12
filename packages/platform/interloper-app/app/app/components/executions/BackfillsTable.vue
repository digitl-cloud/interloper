<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import { getPaginationRowModel } from '@tanstack/vue-table'
import type { Backfill } from '~/types/backfill'

const organisationStore = useOrganisationStore()

const PAGE_SIZE = 50

const EntityBadge = resolveComponent('EntityBadge')
const StatusBadge = resolveComponent('StatusBadge')
const UProgressGroup = resolveComponent('UProgressGroup')
const UTooltip = resolveComponent('UTooltip')

const BACKFILL_STATUSES = ['queued', 'running', 'success', 'failed', 'canceled']

const backfillsStore = useBackfillsStore()
const { backfills, loading, error } = storeToRefs(backfillsStore)
const appConfig = useAppConfig()

/** No backfill at all, as opposed to none matching the filters: the page explains backfills instead. */
const showEmpty = computed(() => !loading.value && !error.value && backfills.value.length === 0)
/** A failed fetch with nothing to show: its alert stands alone, rows from an earlier fetch stay listed under it. */
const showTable = computed(() => !showEmpty.value && !(error.value && backfills.value.length === 0))

onMounted(async () => {
    if (!loading.value) await backfillsStore.fetch()
})

/**
 * The list is loaded whole, so filtering is local. Single-partition
 * backfills are what a manual partition run creates; they outnumber real
 * range backfills and hide them, so they stay out unless asked for.
 */
const search = useQueryParam('q', '')
const status = useQueryParam('status', null)
const showSinglePartition = useQueryParam('single', false, booleanQuery)
const shown = computed(() => {
    const needle = search.value.trim().toLowerCase()
    return backfills.value.filter(backfill =>
        (showSinglePartition.value || backfill.partitions !== 1)
        && (!status.value || backfill.status === status.value)
        && (!needle || [backfill.component_name, backfill.component_key].some(text => text?.toLowerCase().includes(needle))),
    )
})

/** What can bring rows back when the filters leave none: dropping the search and status, or showing the single-partition ones. */
const filteredEmptyActions = computed(() => [
    ...(search.value || status.value
        ? [{ label: 'Clear filters', icon: appConfig.ui.icons.close, onClick: () => { search.value = ''; status.value = null } }]
        : []),
    ...(showSinglePartition.value
        ? []
        : [{ label: 'Show single partition', icon: appConfig.ui.icons.eye, onClick: () => { showSinglePartition.value = true } }]),
].map(action => ({ ...action, color: 'neutral' as const, variant: 'outline' as const, size: 'sm' as const })))

const pagination = ref({ pageIndex: 0, pageSize: PAGE_SIZE })
const sorting = ref([{ id: 'started_at', desc: true }])
watch(shown, () => { pagination.value = { ...pagination.value, pageIndex: 0 } })

const columns: TableColumn<Backfill>[] = withSortableHeaders([
    {
        accessorKey: 'id',
        header: 'ID',
        cell: ({ row }) => idCell(row.getValue<string>('id')),
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
        cell: ({ row }) => h(StatusBadge, { status: row.getValue<string>('status') }),
    },
    {
        id: 'range',
        header: 'Range',
        cell: ({ row }) => {
            const backfill = row.original as Backfill
            return h('span', { class: 'tabular-nums text-muted' }, `${backfill.start_key} → ${backfill.end_key}`)
        },
    },
    {
        accessorKey: 'partitions',
        header: 'Partitions',
        cell: ({ row }) => {
            const stats = runStats(null, row.original.run_counts ?? {})
            if (!stats.total) return emptyCell()
            return h(UTooltip, { text: outcomeSummary(stats).join(' · '), delayDuration: 0 }, () => h('div', { class: 'flex w-40 items-center gap-2' }, [
                h(UProgressGroup, { items: progressSegments(stats), max: stats.total, size: 'sm', class: 'flex-1' }),
                h('span', { class: 'text-xs tabular-nums text-muted' }, `${stats.succeeded}/${stats.total}`),
            ]))
        },
    },
    dateColumn<Backfill>('started_at', 'Started'),
    {
        id: 'elapsed',
        header: 'Elapsed',
        cell: ({ row }) => {
            const backfill = row.original as Backfill
            const elapsed = formatElapsed(backfill.started_at, backfill.completed_at)
            return elapsed ? h('span', { class: 'tabular-nums text-muted' }, elapsed) : emptyCell()
        },
    },
])
</script>

<template>
    <div class="flex min-h-0 flex-1 flex-col gap-4">
        <LoadError v-if="error"
                   title="Couldn't load backfills"
                   :error="error"
                   @retry="backfillsStore.fetch()" />

        <UEmpty v-if="showEmpty"
                :icon="appConfig.icons.backfill"
                title="No backfills yet"
                description="Backfills re-run past partitions to fill gaps or reprocess data. Start one from a job and every partition it replays shows up here."
                :actions="[{ label: 'Go to jobs', icon: appConfig.icons.job, to: organisationStore.path(kindPath('job')) }]" />

        <UCard v-else-if="showTable"
               :ui="FILL_CARD_UI">
            <template #header>
                <div class="flex flex-wrap items-center gap-3">
                    <UInput v-model="search"
                            placeholder="Search backfills by target…"
                            :icon="appConfig.ui.icons.search"
                            class="max-w-sm" />
                    <StatusFilter v-model="status"
                                  :statuses="BACKFILL_STATUSES" />
                    <UCheckbox v-model="showSinglePartition"
                               label="Show single partition" />
                </div>
            </template>

            <UTable v-model:pagination="pagination"
                    v-model:sorting="sorting"
                    :data="shown"
                    :columns="columns"
                    :loading="loading"
                    :pagination-options="{ getPaginationRowModel: getPaginationRowModel() }"
                    sticky
                    class="min-h-0 flex-1"
                    :ui="{ tr: 'cursor-pointer' }"
                    @select="(_e: Event, row: any) => navigateTo(organisationStore.path(`/executions/backfills/${row.original.id}`))">
                <template #empty>
                    <UEmpty :icon="appConfig.ui.icons.search"
                            title="No backfills match these filters"
                            :description="showSinglePartition ? undefined : 'Single-partition backfills are hidden.'"
                            variant="naked"
                            size="sm"
                            :actions="filteredEmptyActions" />
                </template>
            </UTable>

            <template #footer>
                <TableFooter :page="pagination.pageIndex + 1"
                             :total="shown.length"
                             :page-size="PAGE_SIZE"
                             noun="backfill"
                             @update:page="(p: number) => pagination = { ...pagination, pageIndex: p - 1 }" />
            </template>
        </UCard>
    </div>
</template>
