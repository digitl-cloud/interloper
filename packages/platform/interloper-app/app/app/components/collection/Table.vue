<script setup lang="ts">
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import { getGroupedRowModel } from '@tanstack/vue-table'
import type { GroupingOptions } from '@tanstack/vue-table'
import type { CollectionRow, LastRun } from '~/composables/collection'
import type { AssetWarning } from '~/composables/warnings'

/**
 * The collection as a tree table: source types, their sources, the sources'
 * assets, and each asset's destinations, with the warnings, jobs, connection
 * and last run of every source and asset.
 */

const organisationStore = useOrganisationStore()

const componentsStore = useComponentsStore()
const runsStore = useRunsStore()
const toast = useToast()
const confirmDeleteSource = useConfirmDeleteSource()
const appConfig = useAppConfig()
const { loading, upstreams } = storeToRefs(componentsStore)
const sources = computed(() => componentsStore.byKind('source'))
const destinations = computed(() => componentsStore.byKind('destination'))
const jobs = computed(() => componentsStore.byKind('job'))
const { runs } = storeToRefs(runsStore)
const { latestByAssetId } = storeToRefs(useExecutionsStore())
const { getWarnings, filterByCategory } = useAssetWarnings()
const { statusBadge } = useDrift()
const { data, sourceInfoById, typeInfoByKey, assetCount } = useCollectionRows({
    sources,
    upstreams,
    destinations,
    jobs,
    runs,
    latestExecutions: latestByAssetId,
    getWarnings,
})

function lastRunOf(row: { depth: number; original: CollectionRow }): LastRun | null {
    return row.depth === 1
        ? sourceInfoById.value.get(row.original.sourceId)?.lastRun ?? null
        : row.original.lastRun
}

/** Drift badge meta for a source's rollup status, or null when healthy. */
function sourceDriftBadge(sourceId: string) {
    return statusBadge(sourceInfoById.value.get(sourceId)?.drift ?? 'ok')
}

/** A source or asset row's warnings of one category: what its warning tooltip lists. */
function cellWarnings(row: { depth: number; original: CollectionRow }, category: AssetWarning['category']): AssetWarning[] {
    const warnings = row.depth === 1 ? (sourceInfoById.value.get(row.original.sourceId)?.warnings ?? []) : row.original.warnings
    return filterByCategory(warnings, category)
}

/** The type tier's state, as the graph's dot shows it. */
function typeState(groupKey: string): GraphNodeState {
    return typeInfoByKey.value.get(groupKey)?.state ?? 'pending'
}

const columns: TableColumn<CollectionRow>[] = [
    { id: 'title', header: 'Name' },
    { id: 'source_type', accessorKey: 'groupKey' },
    { id: 'source_id', accessorKey: 'sourceId' },
    { id: 'asset_id', accessorKey: 'assetId' },
    { id: 'lastRun', header: 'Last run' },
    { id: 'created', accessorKey: 'createdAt', header: 'Created' },
    { id: 'tags', header: 'Tags' },
    { id: 'connection', header: 'Connection' },
    { id: 'dependencies', header: 'Dependencies' },
    { id: 'destinations', header: 'Destinations' },
    { id: 'jobs', header: 'Jobs' },
    { id: 'actions', header: '' },
]

// Stable reference: an inline literal would be a "new" grouping on every
// re-render, making TanStack auto-reset the expanded state each time.
const grouping = ['source_type', 'source_id', 'asset_id']

const groupingOptions = ref<GroupingOptions>({
    groupedColumnMode: 'remove',
    getGroupedRowModel: getGroupedRowModel(),
})

// Without this, every data refresh (initial fetch included) wipes the
// expanded state before the default-expanded type rows can show.
const expandedOptions = { autoResetExpanded: false }

// Fully expanded, a large collection mounts every row at once and freezes the
// page. Nuxt UI sizes virtual rows by this estimate and never measures them,
// so every cell stays on one line to hold rows at this height.
const virtualize = { estimateSize: 61, overscan: 10 }

const emit = defineEmits<{
    'edit-source': [sourceId: string]
    'view-asset': [assetId: string, sourceId: string]
}>()

// ── Row actions ────────────────────────────────────────────────

async function deleteSource(sourceId: string) {
    const info = sourceInfoById.value.get(sourceId)
    const confirmed = await confirmDeleteSource({ id: sourceId, name: info?.name ?? 'this source', icon: info?.icon })
    if (!confirmed) return
    try {
        await componentsStore.remove(sourceId)
        toast.add({ title: 'Source deleted', description: info?.name, color: 'success' })
    }
    catch (e) {
        toast.add(inUseToast(e, 'Source') ?? errorToast(e, 'Couldn\'t delete source'))
    }
}

function buildSourceActions(sourceId: string): DropdownMenuItem[][] {
    return [[
        {
            label: 'Edit',
            icon: appConfig.icons.edit,
            onSelect: () => emit('edit-source', sourceId),
        },
        {
            label: 'Delete',
            icon: appConfig.icons.delete,
            color: 'error' as const,
            onSelect: () => deleteSource(sourceId),
        },
    ]]
}

function buildAssetActions(assetId: string, sourceId: string): DropdownMenuItem[][] {
    return [[
        {
            label: 'View',
            icon: appConfig.ui.icons.eye,
            onSelect: () => emit('view-asset', assetId, sourceId),
        },
    ]]
}

// ── Right-click context menu ──────────────────────────────────

const ctxMenuOpen = ref(false)
const ctxMenuVirtual = ref({ getBoundingClientRect: () => new DOMRect() })
const ctxMenuItems = ref<DropdownMenuItem[][]>([])

function onRowContextMenu(e: Event, row: any) {
    const isGrouped = row.getIsGrouped?.()
    const depth = row.depth ?? -1
    if (!isGrouped) return

    let actions: DropdownMenuItem[][] = []
    if (depth === 1) actions = buildSourceActions(row.original.sourceId)
    else if (depth === 2) actions = buildAssetActions(row.original.assetId, row.original.sourceId)
    if (actions.length === 0) return

    const event = e as MouseEvent
    event.preventDefault()

    ctxMenuItems.value = actions
    const { clientX: x, clientY: y } = event
    ctxMenuVirtual.value = { getBoundingClientRect: () => new DOMRect(x, y, 0, 0) }
    ctxMenuOpen.value = true
}

const globalFilter = useQueryParam('q', '')

const filteredData = computed(() => {
    const q = globalFilter.value.trim().toLowerCase()
    if (!q) return data.value

    const matchingAssetIds = new Set<string>()
    for (const d of data.value) {
        const sourceInfo = sourceInfoById.value.get(d.sourceId)
        if (d.name.toLowerCase().includes(q)
            || (sourceInfo?.name.toLowerCase().includes(q) ?? false)
            || (sourceInfo?.discriminator?.toLowerCase().includes(q) ?? false)
            || d.sourceKey.toLowerCase().includes(q)
            || d.tags.some(t => t.toLowerCase().includes(q))
            || (d.connectionName?.toLowerCase().includes(q) ?? false)
            || (d.destinationName?.toLowerCase().includes(q) ?? false)) {
            matchingAssetIds.add(d.assetId)
        }
    }

    return data.value.filter(d => matchingAssetIds.has(d.assetId))
})

// Expanded state keyed by TanStack grouped-row id (`source_type:<key>` /
// `source_type:<key>>source_id:<id>`). Always replace the object — TanStack
// memoizes the row model on its identity, so in-place mutation goes stale.
const expanded = ref<Record<string, boolean>>({})

const allExpanded = ref(false)

function toggleAllExpanded() {
    allExpanded.value = !allExpanded.value

    if (allExpanded.value) {
        const next: Record<string, boolean> = {}
        for (const d of filteredData.value) {
            next[`source_type:${d.groupKey}`] = true
            next[`source_type:${d.groupKey}>source_id:${d.sourceId}`] = true
        }
        expanded.value = next
    }
    else {
        expanded.value = {}
    }
}

function createdLabel(at: string | null): string {
    return at ? relativeTime(new Date(at)) : '—'
}

function onRowClick(row: any) {
    if (!row.getIsGrouped()) return
    if (row.depth < 2) row.toggleExpanded()
    else emit('view-asset', row.original.assetId, row.original.sourceId)
}
</script>

<template>
    <UCard :ui="FILL_CARD_UI">
        <template #header>
            <div class="flex flex-wrap items-center gap-3">
                <UInput v-model="globalFilter"
                        placeholder="Search collection…"
                        :icon="appConfig.ui.icons.search"
                        class="max-w-sm" />
                <UButton variant="ghost"
                         color="neutral"
                         size="sm"
                         :icon="allExpanded ? appConfig.icons.collapseAll : appConfig.icons.expandAll"
                         :label="allExpanded ? 'Collapse all' : 'Expand all'"
                         class="ml-auto"
                         @click="toggleAllExpanded" />
                <slot name="actions" />
            </div>
        </template>

        <UTable v-model:expanded="expanded"
                :data="filteredData"
                :columns="columns"
                :loading="loading"
                :grouping="grouping"
                :grouping-options="groupingOptions"
                :expanded-options="expandedOptions"
                :virtualize="virtualize"
                sticky
                class="flex-1 min-h-0"
                :ui="{
                    th: 'text-center first:text-left',
                    td: 'empty:p-0 text-center first:text-left',
                    tr: 'cursor-pointer',
                }"
                @select="(_e: Event, row: any) => onRowClick(row)"
                @contextmenu="onRowContextMenu">
            <!-- Name / group header -->
            <template #title-cell="{ row }">
                <div class="flex items-center gap-2"
                     :class="['', 'pl-6', 'pl-12', 'pl-12'][row.depth]">
                    <!-- A destination row has no chevron; the spacer lines it up under its asset's icon. -->
                    <span v-if="!row.getIsGrouped()"
                          class="size-7 shrink-0" />
                    <UButton v-else
                             color="neutral"
                             variant="ghost"
                             size="sm"
                             :icon="appConfig.ui.icons.chevronDown"
                             :aria-label="row.getIsExpanded() ? 'Collapse' : 'Expand'"
                             :ui="{ leadingIcon: ['transition-transform duration-200', row.getIsExpanded() && 'rotate-180'] }"
                             @click.stop="row.toggleExpanded()" />

                    <!-- Source type group header (depth 0) -->
                    <template v-if="row.getIsGrouped() && row.depth === 0">
                        <UIcon :name="typeInfoByKey.get(row.original.groupKey)?.icon ?? appConfig.icons.source"
                               class="size-5 shrink-0" />
                        <span class="font-semibold">{{ typeInfoByKey.get(row.original.groupKey)?.name ?? row.original.groupKey }}</span>
                        <UTooltip :text="typeInfoByKey.get(row.original.groupKey)?.note ?? 'Not materialized yet'"
                                  :delay-duration="0">
                            <StatusDot :color="nodeStateColor(typeState(row.original.groupKey))"
                                       :pulse="nodeStateExecuting(typeState(row.original.groupKey))" />
                        </UTooltip>
                        <MaturityBadge :maturity="typeInfoByKey.get(row.original.groupKey)?.maturity" />
                        <UBadge color="neutral"
                                size="sm">
                            <span class="tabular-nums">{{ typeInfoByKey.get(row.original.groupKey)?.sourceCount ?? 0 }}</span>
                            {{ (typeInfoByKey.get(row.original.groupKey)?.sourceCount ?? 0) === 1 ? 'source' : 'sources' }}
                        </UBadge>
                    </template>
                    <!-- Source group header (depth 1) -->
                    <template v-else-if="row.getIsGrouped() && row.depth === 1">
                        <UIcon :name="sourceInfoById.get(row.original.sourceId)?.icon ?? appConfig.icons.source"
                               class="size-5 shrink-0" />
                        <span class="flex min-w-0 items-center gap-2">
                            <span class="truncate font-semibold">{{ sourceInfoById.get(row.original.sourceId)?.name }}</span>
                            <UBadge v-if="sourceInfoById.get(row.original.sourceId)?.discriminator"
                                    color="neutral"
                                    size="sm"
                                    class="shrink-0">
                                <span class="tabular-nums slashed-zero">{{ sourceInfoById.get(row.original.sourceId)!.discriminator }}</span>
                            </UBadge>
                        </span>
                        <UBadge color="neutral"
                                size="sm">
                            <span class="tabular-nums">{{ sourceInfoById.get(row.original.sourceId)?.assetCount ?? 0 }}</span>
                            {{ (sourceInfoById.get(row.original.sourceId)?.assetCount ?? 0) === 1 ? 'asset' : 'assets' }}
                        </UBadge>
                        <UBadge v-if="sourceDriftBadge(row.original.sourceId)"
                                :color="sourceDriftBadge(row.original.sourceId)!.color"
                                :icon="sourceDriftBadge(row.original.sourceId)!.icon">
                            {{ sourceDriftBadge(row.original.sourceId)!.label }}
                        </UBadge>
                    </template>
                    <!-- Asset group header (depth 2) -->
                    <template v-else-if="row.getIsGrouped() && row.depth === 2">
                        <UIcon :name="row.original.icon"
                               class="size-4 shrink-0 text-dimmed" />
                        <span>{{ row.original.name }}</span>
                        <MaturityBadge :maturity="row.original.maturity" />
                        <UBadge v-if="statusBadge(row.original.assetStatus)"
                                :color="statusBadge(row.original.assetStatus)!.color"
                                :icon="statusBadge(row.original.assetStatus)!.icon">
                            {{ statusBadge(row.original.assetStatus)!.label }}
                        </UBadge>
                    </template>
                    <!-- Destination leaf row -->
                    <template v-else-if="row.original.destinationName">
                        <UIcon :name="row.original.destinationIcon ?? appConfig.icons.destination"
                               class="size-4 shrink-0 text-dimmed" />
                        <span class="text-dimmed">{{ row.original.destinationName }}</span>
                    </template>
                    <span v-else
                          class="text-dimmed italic">No destinations</span>
                </div>
            </template>

            <!-- Tags (asset level only) -->
            <template #tags-cell="{ row }">
                <div v-if="row.getIsGrouped() && row.depth === 2 && row.original.tags.length > 0"
                     class="flex items-center justify-center gap-1">
                    <UBadge v-for="tag in row.original.tags"
                            :key="tag"
                            color="neutral"
                            :label="tag" />
                </div>
            </template>

            <!-- Dependencies: warnings at source level, count and warnings at asset level -->
            <template #dependencies-cell="{ row }">
                <div v-if="row.getIsGrouped() && (row.depth === 1 || row.depth === 2)"
                     class="flex items-center justify-center gap-1.5">
                    <template v-if="row.depth === 2">
                        <span v-if="row.original.dependencies.length === 0 && cellWarnings(row, 'dependency').length === 0"
                              class="text-dimmed">&mdash;</span>
                        <UBadge v-else-if="row.original.dependencies.length > 0"
                                color="neutral">
                            <span class="tabular-nums">{{ row.original.dependencies.length }}</span>
                        </UBadge>
                    </template>
                    <UTooltip v-if="cellWarnings(row, 'dependency').length > 0"
                              :delay-duration="0"
                              :content="{ side: 'top', sideOffset: 12 }"
                              :ui="{ content: 'h-auto' }">
                        <UIcon :name="appConfig.ui.icons.warning"
                               class="size-4 shrink-0 text-warning" />
                        <template #content>
                            <CollectionWarnings :warnings="cellWarnings(row, 'dependency')" />
                        </template>
                    </UTooltip>
                </div>
            </template>

            <!-- Destinations (source and asset level) -->
            <template #destinations-cell="{ row }">
                <div v-if="row.getIsGrouped() && row.depth > 0"
                     class="flex items-center justify-center gap-1.5">
                    <span v-if="row.original.destinations.length === 0 && cellWarnings(row, 'destination').length === 0"
                          class="text-dimmed">&mdash;</span>
                    <EntityBadge v-else-if="row.original.destinations.length > 0"
                                 :icon="row.original.destinations[0]!.icon"
                                 :label="row.original.destinations[0]!.name"
                                 :extra="row.original.destinations.length - 1" />
                    <UTooltip v-if="cellWarnings(row, 'destination').length > 0"
                              :delay-duration="0"
                              :content="{ side: 'top', sideOffset: 12 }"
                              :ui="{ content: 'h-auto' }">
                        <UIcon :name="appConfig.ui.icons.warning"
                               class="size-4 shrink-0 text-warning" />
                        <template #content>
                            <CollectionWarnings :warnings="cellWarnings(row, 'destination')" />
                        </template>
                    </UTooltip>
                </div>
            </template>

            <!-- Jobs (source and asset level) -->
            <template #jobs-cell="{ row }">
                <div v-if="row.getIsGrouped() && row.depth > 0"
                     class="flex items-center justify-center gap-1.5">
                    <span v-if="row.original.jobs.length === 0 && cellWarnings(row, 'job').length === 0"
                          class="text-dimmed">&mdash;</span>
                    <EntityBadge v-else-if="row.original.jobs.length > 0"
                                 :icon="appConfig.icons.job"
                                 :label="row.original.jobs[0]!.name"
                                 :extra="row.original.jobs.length - 1" />
                    <UTooltip v-if="cellWarnings(row, 'job').length > 0"
                              :delay-duration="0"
                              :content="{ side: 'top', sideOffset: 12 }"
                              :ui="{ content: 'h-auto' }">
                        <UIcon :name="appConfig.ui.icons.warning"
                               class="size-4 shrink-0 text-warning" />
                        <template #content>
                            <CollectionWarnings :warnings="cellWarnings(row, 'job')" />
                        </template>
                    </UTooltip>
                </div>
            </template>

            <!-- Last run (source and asset level) -->
            <template #lastRun-cell="{ row }">
                <template v-if="row.getIsGrouped() && row.depth > 0">
                    <span v-if="!lastRunOf(row)"
                          class="text-dimmed">&mdash;</span>
                    <div v-else
                         class="flex items-center justify-center gap-2">
                        <UTooltip :text="lastRunOf(row)!.note"
                                  :disabled="!lastRunOf(row)!.note"
                                  :delay-duration="0">
                            <NuxtLink :to="organisationStore.path(`/executions/runs/${lastRunOf(row)!.runId}`)"
                                      class="hover:opacity-75"
                                      @click.stop>
                                <StatusBadge :status="lastRunOf(row)!.status" />
                            </NuxtLink>
                        </UTooltip>
                        <span class="text-xs text-dimmed tabular-nums">
                            {{ relativeTime(new Date(lastRunOf(row)!.at)) }}<template v-if="lastRunOf(row)!.share"> &middot; {{ lastRunOf(row)!.share }}</template>
                        </span>
                    </div>
                </template>
            </template>

            <!-- Connection (all levels) -->
            <template #connection-cell="{ row }">
                <template v-if="row.getIsGrouped() && row.depth > 0">
                    <span v-if="!row.original.connectionName"
                          class="text-dimmed">&mdash;</span>
                    <EntityBadge v-else
                                 :icon="row.original.connectionIcon ?? appConfig.icons.connection"
                                 :label="row.original.connectionName" />
                </template>
                <template v-else-if="!row.getIsGrouped()">
                    <span class="text-dimmed">&mdash;</span>
                </template>
            </template>

            <!-- Created (all levels) -->
            <template #created-cell="{ row }">
                <span v-if="row.getIsGrouped() ? row.depth > 0 : true"
                      class="text-dimmed tabular-nums">
                    {{ createdLabel(row.getIsGrouped() ? row.original.createdAt : row.original.destinationCreatedAt) }}
                </span>
            </template>

            <!-- Actions (source and asset rows) -->
            <template #actions-cell="{ row }">
                <div v-if="row.getIsGrouped() && (row.depth === 1 || row.depth === 2)"
                     class="flex justify-end">
                    <UDropdownMenu :items="row.depth === 1
                        ? buildSourceActions(row.original.sourceId)
                        : buildAssetActions(row.original.assetId, row.original.sourceId)">
                        <UButton :icon="appConfig.ui.icons.ellipsis"
                                 color="neutral"
                                 variant="ghost"
                                 size="sm"
                                 aria-label="Actions"
                                 @click.stop />
                    </UDropdownMenu>
                </div>
            </template>
        </UTable>

        <!-- Right-click context menu -->
        <UDropdownMenu v-model:open="ctxMenuOpen"
                       :items="ctxMenuItems"
                       :modal="false"
                       :content="{ reference: ctxMenuVirtual, side: 'bottom', align: 'start', sideOffset: 4 }">
            <div class="hidden" />
        </UDropdownMenu>

        <template #footer>
            <TableFooter>
                {{ sources.length }} {{ sources.length === 1 ? 'source' : 'sources' }},
                {{ assetCount }} {{ assetCount === 1 ? 'asset' : 'assets' }}
            </TableFooter>
        </template>
    </UCard>
</template>
