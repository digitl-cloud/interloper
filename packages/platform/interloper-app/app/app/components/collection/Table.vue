<script setup lang="ts">
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import { getGroupedRowModel } from '@tanstack/vue-table'
import type { GroupingOptions } from '@tanstack/vue-table'
import type { CollectionRow, CollectionRows, LastRun } from '~/composables/collection'
import type { AssetWarning } from '~/composables/warnings'

/**
 * The collection as a tree table: its assets grouped by source type and source
 * (or by source alone, or not at all, as `groupBy` says), each asset's
 * destinations beneath it, with the warnings, jobs, connection and last run of
 * every group. The page filters the rows; the table shows what it is given.
 */
const props = defineProps<{
    /** The rows to show, already narrowed by the page's filters. */
    rows: CollectionRow[]
    sourceInfoById: CollectionRows['sourceInfoById']['value']
    typeInfoByKey: CollectionRows['typeInfoByKey']['value']
    /** Every asset in the collection, for the footer. */
    assetCount: number
    groupBy: GroupBy
}>()

const componentsStore = useComponentsStore()
const toast = useToast()
const confirmDeleteSource = useConfirmDeleteSource()
const appConfig = useAppConfig()
const canEdit = useCanEdit()
const { loading } = storeToRefs(componentsStore)
const { filterByCategory } = useAssetWarnings()
const { statusBadge } = useDrift()

/** The group a row heads, named after its grouping column. */
type Tier = 'source_type' | 'source_id' | 'asset_id'

// The tiers each grouping nests, outermost first. Constants, so TanStack sees one array per
// grouping: a new one on every render would reset the expanded state each time.
const TIERS: Record<GroupBy, Tier[]> = {
    type: ['source_type', 'source_id', 'asset_id'],
    source: ['source_id', 'asset_id'],
    asset: ['asset_id'],
}
const grouping = computed(() => TIERS[props.groupBy])

// The grouping columns exist to group by; a grouping that leaves one out must not show it as a column.
const columnVisibility = { source_type: false, source_id: false, asset_id: false }

type TableRow = { depth: number; original: CollectionRow; getIsGrouped: () => boolean }

/** The group a row heads, or null for a destination row. */
function tierOf(row: TableRow): Tier | null {
    return row.getIsGrouped() ? grouping.value[row.depth] ?? null : null
}

/** Whether a row heads a source or an asset: the rows that carry their own columns and actions. */
function headsSourceOrAsset(row: TableRow): boolean {
    const tier = tierOf(row)
    return tier === 'source_id' || tier === 'asset_id'
}

function lastRunOf(row: TableRow): LastRun | null {
    return tierOf(row) === 'source_id'
        ? props.sourceInfoById.get(row.original.sourceId)?.lastRun ?? null
        : row.original.lastRun
}

/** Drift badge meta for a source's rollup status, or null when healthy. */
function sourceDriftBadge(sourceId: string) {
    return statusBadge(props.sourceInfoById.get(sourceId)?.drift ?? 'ok')
}

/** A row's warnings of one category: what its warning tooltip lists. A group's are those of everything inside it. */
function cellWarnings(row: TableRow, category: AssetWarning['category']): AssetWarning[] {
    const tier = tierOf(row)
    if (tier === 'source_type') return filterByCategory(props.typeInfoByKey.get(row.original.groupKey)?.warnings ?? [], category)
    if (tier === 'source_id') return filterByCategory(props.sourceInfoById.get(row.original.sourceId)?.warnings ?? [], category)
    return filterByCategory(row.original.warnings, category)
}

/** What a group's count badge counts, for its tooltip ("4 sources"). */
function countLabel(count: number, noun: string): string {
    return `${count} ${count === 1 ? noun : `${noun}s`}`
}

/** A type group's drifted sources, and the colour of the worst. */
function typeDrift(groupKey: string) {
    const drift = props.typeInfoByKey.get(groupKey)?.drift ?? []
    return { drift, color: drift.some(d => d.color === 'error') ? 'text-error' : 'text-warning' }
}

/** The type tier's state, as the graph's dot shows it. */
function typeState(groupKey: string): GraphNodeState {
    return props.typeInfoByKey.get(groupKey)?.state ?? 'pending'
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
    const info = props.sourceInfoById.get(sourceId)
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
    if (!canEdit.value) return []
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
    const tier = tierOf(row)
    if (!tier) return

    let actions: DropdownMenuItem[][] = []
    if (tier === 'source_id') actions = buildSourceActions(row.original.sourceId)
    else if (tier === 'asset_id') actions = buildAssetActions(row.original.assetId, row.original.sourceId)
    if (actions.length === 0) return

    const event = e as MouseEvent
    event.preventDefault()

    ctxMenuItems.value = actions
    const { clientX: x, clientY: y } = event
    ctxMenuVirtual.value = { getBoundingClientRect: () => new DOMRect(x, y, 0, 0) }
    ctxMenuOpen.value = true
}

// Expanded state keyed by TanStack grouped-row id, its tiers' values joined
// (`source_type:<key>>source_id:<id>`). Always replace the object: TanStack
// memoizes the row model on its identity, so in-place mutation goes stale.
const expanded = ref<Record<string, boolean>>({})

const allExpanded = ref(false)

/** A row's value in a grouping column. */
function tierValue(row: CollectionRow, tier: Tier): string {
    if (tier === 'source_type') return row.groupKey
    return tier === 'source_id' ? row.sourceId : row.assetId
}

/** Expand every group above the assets, or fold them all; the assets keep their destinations folded. */
function toggleAllExpanded() {
    allExpanded.value = !allExpanded.value
    const next: Record<string, boolean> = {}
    if (allExpanded.value) {
        const above = grouping.value.slice(0, -1)
        for (const row of props.rows) {
            for (let depth = 1; depth <= above.length; depth++)
                next[above.slice(0, depth).map(tier => `${tier}:${tierValue(row, tier)}`).join('>')] = true
        }
    }
    expanded.value = next
}

// Another grouping keys its rows differently: start it folded.
watch(grouping, () => {
    expanded.value = {}
    allExpanded.value = false
})

function createdLabel(at: string | null): string {
    return at ? relativeTime(new Date(at)) : '—'
}

function onRowClick(row: any) {
    if (!row.getIsGrouped()) return
    if (tierOf(row) !== 'asset_id') row.toggleExpanded()
    else emit('view-asset', row.original.assetId, row.original.sourceId)
}
</script>

<template>
    <UCard :ui="FILL_CARD_UI">
        <UTable v-model:expanded="expanded"
                :data="rows"
                :columns="columns"
                :loading="loading"
                :grouping="grouping"
                :column-visibility="columnVisibility"
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
            <template #title-header>
                <div class="flex items-center gap-2">
                    <UTooltip v-if="groupBy !== 'asset'"
                              :text="allExpanded ? 'Collapse all' : 'Expand all'">
                        <UButton :icon="allExpanded ? appConfig.icons.collapseAll : appConfig.icons.expandAll"
                                 :aria-label="allExpanded ? 'Collapse all' : 'Expand all'"
                                 color="neutral"
                                 variant="ghost"
                                 size="sm"
                                 @click="toggleAllExpanded" />
                    </UTooltip>
                    <span>Name</span>
                </div>
            </template>

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
                    <template v-if="tierOf(row) === 'source_type'">
                        <UIcon :name="typeInfoByKey.get(row.original.groupKey)?.icon ?? appConfig.icons.source"
                               class="size-5 shrink-0" />
                        <span class="font-medium">{{ typeInfoByKey.get(row.original.groupKey)?.name ?? row.original.groupKey }}</span>
                        <MaturityBadge :maturity="typeInfoByKey.get(row.original.groupKey)?.maturity" />
                        <UTooltip :text="countLabel(typeInfoByKey.get(row.original.groupKey)?.sourceCount ?? 0, 'source')">
                            <UBadge :label="String(typeInfoByKey.get(row.original.groupKey)?.sourceCount ?? 0)"
                                    color="neutral"
                                    size="sm" />
                        </UTooltip>
                        <UTooltip :text="typeInfoByKey.get(row.original.groupKey)?.note ?? 'Not materialized yet'"
                                  :delay-duration="0">
                            <StatusDot :color="nodeStateColor(typeState(row.original.groupKey))"
                                       :pulse="nodeStateExecuting(typeState(row.original.groupKey))" />
                        </UTooltip>
                        <UTooltip v-if="typeDrift(row.original.groupKey).drift.length"
                                  :delay-duration="0"
                                  :ui="{ content: 'h-auto' }">
                            <UIcon :name="appConfig.ui.icons.warning"
                                   class="size-4 shrink-0"
                                   :class="typeDrift(row.original.groupKey).color" />
                            <template #content>
                                <CollectionWarnings :warnings="typeDrift(row.original.groupKey).drift" />
                            </template>
                        </UTooltip>
                    </template>
                    <!-- Source group header (depth 1) -->
                    <template v-else-if="tierOf(row) === 'source_id'">
                        <UIcon :name="sourceInfoById.get(row.original.sourceId)?.icon ?? appConfig.icons.source"
                               class="size-5 shrink-0" />
                        <span class="flex min-w-0 items-center gap-2">
                            <span class="truncate font-medium">{{ sourceInfoById.get(row.original.sourceId)?.name }}</span>
                            <UBadge v-if="sourceInfoById.get(row.original.sourceId)?.discriminator"
                                    color="neutral"
                                    size="sm"
                                    class="shrink-0">
                                <span class="tabular-nums slashed-zero">{{ sourceInfoById.get(row.original.sourceId)!.discriminator }}</span>
                            </UBadge>
                        </span>
                        <UTooltip :text="countLabel(sourceInfoById.get(row.original.sourceId)?.assetCount ?? 0, 'asset')">
                            <UBadge :label="String(sourceInfoById.get(row.original.sourceId)?.assetCount ?? 0)"
                                    color="neutral"
                                    size="sm" />
                        </UTooltip>
                        <StatusBadge v-if="sourceDriftBadge(row.original.sourceId)"
                                     :color="sourceDriftBadge(row.original.sourceId)!.color"
                                     :label="sourceDriftBadge(row.original.sourceId)!.label" />
                    </template>
                    <!-- Asset group header (depth 2) -->
                    <template v-else-if="tierOf(row) === 'asset_id'">
                        <UIcon :name="row.original.icon"
                               class="size-4 shrink-0 text-dimmed" />
                        <span>{{ row.original.name }}</span>
                        <span v-if="groupBy === 'asset'"
                              class="truncate text-dimmed">{{ sourceInfoById.get(row.original.sourceId)?.name }}</span>
                        <MaturityBadge :maturity="row.original.maturity" />
                        <StatusBadge v-if="statusBadge(row.original.assetStatus)"
                                     :color="statusBadge(row.original.assetStatus)!.color"
                                     :label="statusBadge(row.original.assetStatus)!.label" />
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
                <span v-if="tierOf(row) === 'asset_id' && row.original.tags.length > 0"
                      class="block text-center text-muted">{{ row.original.tags.join(', ') }}</span>
            </template>

            <!-- Dependencies: warnings at source level, count and warnings at asset level -->
            <template #dependencies-cell="{ row }">
                <div v-if="headsSourceOrAsset(row) || (tierOf(row) && cellWarnings(row, 'dependency').length)"
                     class="flex items-center justify-center gap-1.5">
                    <template v-if="tierOf(row) === 'asset_id'">
                        <span v-if="row.original.dependencies.length === 0 && cellWarnings(row, 'dependency').length === 0"
                              class="text-dimmed">&mdash;</span>
                        <span v-else-if="row.original.dependencies.length > 0"
                              class="tabular-nums">{{ row.original.dependencies.length }}</span>
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
                <div v-if="headsSourceOrAsset(row) || (tierOf(row) && cellWarnings(row, 'destination').length)"
                     class="flex items-center justify-center gap-1.5">
                    <span v-if="headsSourceOrAsset(row) && row.original.destinations.length === 0 && cellWarnings(row, 'destination').length === 0"
                          class="text-dimmed">&mdash;</span>
                    <EntityBadge v-else-if="headsSourceOrAsset(row) && row.original.destinations.length > 0"
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
                <div v-if="headsSourceOrAsset(row) || (tierOf(row) && cellWarnings(row, 'job').length)"
                     class="flex items-center justify-center gap-1.5">
                    <span v-if="headsSourceOrAsset(row) && row.original.jobs.length === 0 && cellWarnings(row, 'job').length === 0"
                          class="text-dimmed">&mdash;</span>
                    <EntityBadge v-else-if="headsSourceOrAsset(row) && row.original.jobs.length > 0"
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
                <template v-if="headsSourceOrAsset(row)">
                    <span v-if="!lastRunOf(row)"
                          class="text-dimmed">&mdash;</span>
                    <div v-else
                         class="flex justify-center">
                        <LastRunBadge :status="lastRunOf(row)!.status"
                                      :at="lastRunOf(row)!.at"
                                      :run-id="lastRunOf(row)!.runId"
                                      :note="lastRunOf(row)!.note" />
                    </div>
                </template>
            </template>

            <!-- Connection (all levels) -->
            <template #connection-cell="{ row }">
                <template v-if="headsSourceOrAsset(row)">
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
                <span v-if="!row.getIsGrouped() || headsSourceOrAsset(row)"
                      class="text-dimmed tabular-nums">
                    {{ createdLabel(row.getIsGrouped() ? row.original.createdAt : row.original.destinationCreatedAt) }}
                </span>
            </template>

            <!-- Actions (source and asset rows) -->
            <template #actions-cell="{ row }">
                <div v-if="headsSourceOrAsset(row) && (tierOf(row) !== 'source_id' || canEdit)"
                     class="flex justify-end opacity-0 transition-opacity group-hover/row:opacity-100 focus-within:opacity-100 pointer-coarse:opacity-100">
                    <UDropdownMenu :items="tierOf(row) === 'source_id'
                        ? buildSourceActions(row.original.sourceId)
                        : buildAssetActions(row.original.assetId, row.original.sourceId)">
                        <UButton :icon="appConfig.icons.options"
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
                {{ sourceInfoById.size }} {{ sourceInfoById.size === 1 ? 'source' : 'sources' }},
                {{ assetCount }} {{ assetCount === 1 ? 'asset' : 'assets' }}
            </TableFooter>
        </template>
    </UCard>
</template>
