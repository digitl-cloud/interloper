<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { Run } from '~/types/run'

const organisationStore = useOrganisationStore()

const UBadge = resolveComponent('UBadge')
const UButton = resolveComponent('UButton')
const UTooltip = resolveComponent('UTooltip')
const EntityBadge = resolveComponent('EntityBadge')
const StatusBadge = resolveComponent('StatusBadge')
const UProgressGroup = resolveComponent('UProgressGroup')

type RunRow = Run & { children?: Run[] }

const runsStore = useRunsStore()
const componentsStore = useComponentsStore()
const { runs, stacks, loading, error, total, pageIndex, pageSize, filtered } = storeToRefs(runsStore)
const appConfig = useAppConfig()
const editor = useCanEdit()

/** Nothing ran yet at all, as opposed to nothing matching the filters: the page explains executions instead. */
const showEmpty = computed(() => !loading.value && !error.value && runs.value.length === 0 && !filtered.value)
/** A failed fetch with nothing to show: its alert stands alone, rows from an earlier fetch stay listed under it. */
const showTable = computed(() => !showEmpty.value && !(error.value && runs.value.length === 0))

/**
 * A row is a stack at its latest attempt; expanding it lists the attempts
 * before, which load on first expand. The latest is left out of its own
 * children so no attempt appears twice, which also keeps row ids unique.
 */
const rows = computed<RunRow[]>(() => runs.value.map((run) => {
    const attempts = stacks.value[run.root_run_id ?? run.id]
    return attempts ? { ...run, children: attempts.filter(attempt => attempt.id !== run.id) } : run
}))

const expanded = ref<Record<string, boolean>>({})
// Stable references: fresh ones on every render make TanStack rebuild the row
// model, and the default auto-reset would collapse a stack the moment its
// attempts finish loading.
const getRowId = (run: RunRow) => run.id
const getSubRows = (run: RunRow) => run.children
const expandedOptions = {
    autoResetExpanded: false,
    getRowCanExpand: (row: { depth: number, original: RunRow }) => row.depth === 0 && row.original.attempt > 1,
}
// UTable renders a detail row under every expanded row for its `#expanded`
// slot, even with sub-rows; with no slot its single spanning cell is empty.
const tableUi = { tr: 'cursor-pointer [&:has(>td[colspan]:empty)]:hidden' }

/**
 * Filters live in the route query so a filtered view is linkable; the store
 * reads them (the fetch, the pagination and the realtime gate), the search
 * once typing settles.
 */
const search = useQueryParam('q', '')
const kindParam = useQueryParam('kind', null)
const type = useQueryParam('type', null)
const status = useQueryParam('status', null)
/** Type choices follow the kind in view, so picking a kind drops a type outside it. */
const kind = computed({
    get: () => kindParam.value,
    set: (value: string | null) => {
        kindParam.value = value
        type.value = null
    },
})
const settledSearch = refDebounced(search, 300)

const RUN_STATUSES = ['pending', 'queued', 'dispatched', 'running', 'success', 'failed', 'canceled']

watch([settledSearch, kindParam, type, status], ([q, kind, key, status]) =>
    runsStore.setFilters({ q: q.trim(), kind, key, status }), { immediate: true })

function clearFilters() {
    search.value = ''
    kindParam.value = null
    type.value = null
    status.value = null
}

const typeChoices = computed(() => kindParam.value
    ? componentsStore.byKind(kindParam.value)
    : componentsStore.all)

onMounted(() => {
    // The filter choices read the collection.
    if (componentsStore.all.length === 0) componentsStore.fetchAll()
})

onUnmounted(runsStore.clearFilters)

/** A stack's attempt count on its own row, an earlier attempt's ordinal on its; none for a single attempt. */
function attemptBadge(run: Run, depth: number) {
    if (depth > 0) {
        return h(UTooltip, { text: `Attempt ${run.attempt}` }, () =>
            h(UBadge, { color: 'neutral', size: 'sm', class: 'tabular-nums' }, () => `#${run.attempt}`))
    }
    if (run.attempt <= 1) return null
    return h(UTooltip, { text: `${run.attempt} attempts` }, () =>
        h(UBadge, { color: 'neutral', size: 'sm', icon: appConfig.ui.icons.reload, class: 'tabular-nums' }, () => String(run.attempt)))
}

const columns: TableColumn<RunRow>[] = [
    {
        accessorKey: 'id',
        header: 'ID',
        cell: ({ row }) => {
            const run = row.original
            const open = row.getIsExpanded()
            return h('div', { class: 'flex items-center gap-1.5', style: { paddingInlineStart: `${row.depth * 1.5}rem` } }, [
                h(UButton, {
                    'color': 'neutral',
                    'variant': 'ghost',
                    'size': 'sm',
                    'icon': open ? appConfig.ui.icons.chevronDown : appConfig.ui.icons.chevronRight,
                    'class': row.getCanExpand() ? undefined : 'invisible',
                    'aria-label': open ? 'Hide earlier attempts' : 'Show earlier attempts',
                    'onClick': (event: Event) => {
                        event.stopPropagation()
                        if (!open) runsStore.loadStack(run.root_run_id ?? run.id)
                        row.toggleExpanded()
                    },
                }),
                idCell(run.id),
                attemptBadge(run, row.depth),
            ])
        },
    },
    {
        id: 'target',
        header: 'Target',
        cell: ({ row }) => {
            const run = row.original as Run
            return h(EntityBadge, { label: targetLabel(run), icon: targetIcon(run) })
        },
    },
    {
        accessorKey: 'partition_key',
        header: 'Partition',
        cell: ({ row }) => {
            const key = row.getValue<string | null>('partition_key')
            return key ? h('span', { class: 'tabular-nums text-muted' }, key) : emptyCell()
        },
    },
    {
        accessorKey: 'status',
        header: 'Status',
        cell: ({ row }) => h(StatusBadge, { status: row.getValue<string>('status') }),
    },
    {
        id: 'assets',
        header: 'Assets',
        cell: ({ row }) => {
            const stats = runStats(row.original, row.original.execution_counts ?? {})
            if (!stats.total) return emptyCell()
            return h(UTooltip, { text: outcomeSummary(stats).join(' · '), delayDuration: 0 }, () => h('div', { class: 'flex w-40 items-center gap-2' }, [
                h(UProgressGroup, { items: progressSegments(stats), max: stats.total, size: 'sm', class: 'flex-1' }),
                h('span', { class: 'text-xs tabular-nums text-muted' }, `${stats.succeeded}/${stats.total}`),
            ]))
        },
    },
    dateColumn<RunRow>('created_at', 'Created'),
    dateColumn<RunRow>('started_at', 'Started'),
    {
        id: 'elapsed',
        header: 'Elapsed',
        cell: ({ row }) => {
            const run = row.original as Run
            const elapsed = formatElapsed(run.started_at, run.completed_at)
            return elapsed ? h('span', { class: 'tabular-nums text-muted' }, elapsed) : emptyCell()
        },
    },
]

function onPageChange(page: number) {
    runsStore.goToPage(page - 1)
}
</script>

<template>
    <div class="flex min-h-0 flex-1 flex-col gap-4">
        <LoadError v-if="error"
                   title="Couldn't load runs"
                   :error="error"
                   @retry="runsStore.fetch()" />

        <UEmpty v-if="showEmpty"
                :icon="appConfig.icons.executions"
                title="No executions yet"
                description="Executions are the run history of your pipelines. Every time a job runs, on schedule or by hand, each partition it materialises shows up here with its status and timing."
                :actions="editor ? [{ label: 'New job', icon: appConfig.ui.icons.plus, to: { path: organisationStore.path(kindPath('job')), query: { new: '1' } } }] : undefined" />

        <UCard v-else-if="showTable"
               :ui="FILL_CARD_UI">
            <template #header>
                <div class="flex flex-wrap items-center gap-3">
                    <UInput v-model="search"
                            placeholder="Search runs by target…"
                            :icon="appConfig.ui.icons.search"
                            class="max-w-sm" />
                    <KindFilter v-model="kind"
                                :components="componentsStore.all" />
                    <TypeFilter v-model="type"
                                :components="typeChoices" />
                    <StatusFilter v-model="status"
                                  :statuses="RUN_STATUSES" />
                </div>
            </template>

            <UTable v-model:expanded="expanded"
                    :data="rows"
                    :columns="columns"
                    :get-row-id="getRowId"
                    :get-sub-rows="getSubRows"
                    :expanded-options="expandedOptions"
                    :loading="loading"
                    :sorting="[{ id: 'created_at', desc: true }]"
                    sticky
                    class="min-h-0 flex-1"
                    :ui="tableUi"
                    @select="(_e: Event, row: any) => navigateTo(organisationStore.path(`/executions/runs/${row.original.id}`))">
                <template #empty>
                    <UEmpty :icon="appConfig.ui.icons.search"
                            title="No runs match these filters"
                            variant="naked"
                            size="sm"
                            :actions="[{ label: 'Clear filters', icon: appConfig.ui.icons.close, color: 'neutral', variant: 'outline', size: 'sm', onClick: clearFilters }]" />
                </template>
            </UTable>

            <template #footer>
                <TableFooter :page="pageIndex + 1"
                             :total="total"
                             :page-size="pageSize"
                             noun="run"
                             @update:page="onPageChange" />
            </template>
        </UCard>
    </div>
</template>
