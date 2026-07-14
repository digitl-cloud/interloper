<script setup lang="ts">
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import { getGroupedRowModel } from '@tanstack/vue-table'
import type { GroupingOptions } from '@tanstack/vue-table'
import type { CollectionRow } from '~/composables/collection'

const componentsStore = useComponentsStore()
const runsStore = useRunsStore()
const toast = useToast()
const { confirm } = useConfirm()
const { loading, dependencies } = storeToRefs(componentsStore)
const sources = computed(() => componentsStore.byKind('source'))
const destinations = computed(() => componentsStore.byKind('destination'))
const jobs = computed(() => componentsStore.byKind('job'))
const { runs } = storeToRefs(runsStore)
const { getWarnings, filterByCategory } = useAssetWarnings()
const { statusBadge } = useDrift()
const { data, sourceInfoById, assetCount } = useCollectionRows({
    sources,
    dependencies,
    destinations,
    jobs,
    runs,
    getWarnings,
})

/** Drift badge meta for a source's rollup status, or null when healthy. */
function sourceDriftBadge(sourceId: string) {
    return statusBadge(sourceInfoById.value.get(sourceId)?.drift ?? 'ok')
}

const statusColor: Record<string, 'error' | 'primary' | 'secondary' | 'success' | 'info' | 'warning' | 'neutral'> = {
    success: 'success',
    failed: 'error',
    running: 'info',
    pending: 'neutral',
    canceled: 'warning',
}

const statusIcon: Record<string, string> = {
    success: 'i-lucide-check',
    failed: 'i-lucide-x',
    running: 'i-lucide-loader-circle',
    pending: 'i-lucide-clock',
    canceled: 'i-lucide-ban',
}

const columns: TableColumn<CollectionRow>[] = [
    { id: 'title', header: 'Name' },
    { id: 'source_id', accessorKey: 'sourceId' },
    { id: 'asset_id', accessorKey: 'assetId' },
    { id: 'lastRun', header: 'Last Run' },
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

const emit = defineEmits<{
    create: []
    'edit-source': [sourceId: string]
    'view-asset': [assetId: string, sourceId: string]
}>()

// ── Row actions ────────────────────────────────────────────────

async function deleteSource(sourceId: string) {
    const info = sourceInfoById.value.get(sourceId)
    const confirmed = await confirm({
        title: 'Delete source?',
        description: `This will permanently delete "${info?.name ?? 'this source'}" and all its assets.`,
        confirmLabel: 'Delete',
        confirmColor: 'error',
        icon: 'i-lucide-triangle-alert',
    })
    if (!confirmed) return
    try {
        await componentsStore.remove(sourceId)
        toast.add({ title: `Source "${info?.name ?? 'Source'}" deleted`, color: 'success' })
    }
    catch {
        toast.add({ title: 'Failed to delete source', color: 'error' })
    }
}

function buildSourceActions(sourceId: string): DropdownMenuItem[][] {
    return [[
        {
            label: 'Edit',
            icon: 'i-lucide-pencil',
            onSelect: () => emit('edit-source', sourceId),
        },
        {
            label: 'Delete',
            icon: 'i-lucide-trash-2',
            color: 'error' as const,
            onSelect: () => deleteSource(sourceId),
        },
    ]]
}

function buildAssetActions(assetId: string, sourceId: string): DropdownMenuItem[][] {
    return [[
        {
            label: 'View',
            icon: 'i-lucide-eye',
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
    if (depth === 0) actions = buildSourceActions(row.original.sourceId)
    else if (depth === 1) actions = buildAssetActions(row.original.assetId, row.original.sourceId)
    if (actions.length === 0) return

    const event = e as MouseEvent
    event.preventDefault()

    ctxMenuItems.value = actions
    const { clientX: x, clientY: y } = event
    ctxMenuVirtual.value = { getBoundingClientRect: () => new DOMRect(x, y, 0, 0) }
    ctxMenuOpen.value = true
}

const SOURCE_PAGE_SIZE = 20
const sourcePageIndex = ref(0)

const globalFilter = ref('')
const tableRef = useTemplateRef<{ tableApi: any }>('table')

const filteredData = computed(() => {
    const q = globalFilter.value.trim().toLowerCase()
    if (!q) return data.value

    const matchingAssetIds = new Set<string>()
    for (const d of data.value) {
        const sourceName = sourceInfoById.value.get(d.sourceId)?.name ?? ''
        if (d.name.toLowerCase().includes(q)
            || sourceName.toLowerCase().includes(q)
            || d.sourceKey.toLowerCase().includes(q)
            || d.tags.some(t => t.toLowerCase().includes(q))
            || (d.connectionName?.toLowerCase().includes(q) ?? false)
            || (d.destinationName?.toLowerCase().includes(q) ?? false)) {
            matchingAssetIds.add(d.assetId)
        }
    }

    return data.value.filter(d => matchingAssetIds.has(d.assetId))
})

// Reset to first page when search filter changes
watch(globalFilter, () => {
    sourcePageIndex.value = 0
})

const uniqueSourceIds = computed(() => {
    const seen = new Set<string>()
    const ids: string[] = []
    for (const d of filteredData.value) {
        if (!seen.has(d.sourceId)) {
            seen.add(d.sourceId)
            ids.push(d.sourceId)
        }
    }
    return ids
})


const paginatedData = computed(() => {
    const start = sourcePageIndex.value * SOURCE_PAGE_SIZE
    const pageSourceIds = new Set(uniqueSourceIds.value.slice(start, start + SOURCE_PAGE_SIZE))
    return filteredData.value.filter(d => pageSourceIds.has(d.sourceId))
})

const allExpanded = ref(false)

function toggleAllExpanded() {
    const api = tableRef.value?.tableApi
    if (!api) return
    allExpanded.value = !allExpanded.value

    if (allExpanded.value) {
        api.getRowModel().rows.forEach((row: any) => {
            if (row.getIsGrouped() && row.depth === 0 && !row.getIsExpanded()) {
                row.toggleExpanded()
            }
        })
    }
    else {
        api.toggleAllRowsExpanded(false)
    }
}

function onRowClick(row: any) {
    if (!row.getIsGrouped()) return
    if (row.depth === 0) emit('edit-source', row.original.sourceId)
    else if (row.depth === 1) emit('view-asset', row.original.assetId, row.original.sourceId)
}
</script>

<template>
    <div class="w-full flex flex-col gap-4">
        <div class="flex items-center gap-3">
            <UInput v-model="globalFilter"
                    placeholder="Search collection..."
                    icon="i-lucide-search"
                    class="max-w-sm" />
            <UButton variant="ghost"
                     color="neutral"
                     size="xs"
                     :icon="allExpanded ? 'i-lucide-chevrons-down-up' : 'i-lucide-chevrons-up-down'"
                     :label="allExpanded ? 'Collapse all' : 'Expand all'"
                     @click="toggleAllExpanded" />
            <UButton icon="i-lucide-plus"
                     label="New Source"
                     class="ml-auto"
                     @click="emit('create')" />
        </div>

        <UTable ref="table"
                :data="paginatedData"
                :columns="columns"
                :loading="loading"
                :grouping="['source_id', 'asset_id']"
                :grouping-options="groupingOptions"
                sticky
                class="max-h-[calc(100vh-10rem)]"
                :ui="{
                    th: 'text-center first:text-left',
                    td: 'empty:p-0 text-center first:text-left',
                    tr: 'cursor-pointer',
                }"
                @select="(_e: Event, row: any) => onRowClick(row)"
                @contextmenu="onRowContextMenu">
            <!-- Name / group header -->
            <template #title-cell="{ row }">
                <!-- Source group header (depth 0) -->
                <div v-if="row.getIsGrouped() && row.depth === 0"
                     class="flex items-center gap-2">
                    <UButton variant="outline"
                             color="neutral"
                             size="xs"
                             :icon="row.getIsExpanded() ? 'i-lucide-chevron-down' : 'i-lucide-chevron-right'"
                             @click.stop="row.toggleExpanded()" />
                    <UIcon :name="sourceInfoById.get(row.original.sourceId)?.icon ?? 'i-lucide-database'"
                           class="size-5 shrink-0" />
                    <span class="font-semibold">{{ sourceInfoById.get(row.original.sourceId)?.name }}</span>
                    <UBadge color="neutral"
                            variant="subtle">
                        {{ sourceInfoById.get(row.original.sourceId)?.assetCount ?? 0 }}
                    </UBadge>
                    <UBadge v-if="sourceDriftBadge(row.original.sourceId)"
                            :color="sourceDriftBadge(row.original.sourceId)!.color"
                            :icon="sourceDriftBadge(row.original.sourceId)!.icon"
                            variant="subtle">
                        {{ sourceDriftBadge(row.original.sourceId)!.label }}
                    </UBadge>
                </div>
                <!-- Asset group header (depth 1) -->
                <div v-else-if="row.getIsGrouped() && row.depth === 1"
                     class="flex items-center gap-2 pl-8">
                    <UButton variant="outline"
                             color="neutral"
                             size="xs"
                             :icon="row.getIsExpanded() ? 'i-lucide-chevron-down' : 'i-lucide-chevron-right'"
                             @click.stop="row.toggleExpanded()" />
                    <UIcon :name="row.original.icon"
                           class="size-4.5 shrink-0 text-dimmed" />
                    <span>{{ row.original.name }}</span>
                    <UBadge v-if="statusBadge(row.original.assetStatus)"
                            :color="statusBadge(row.original.assetStatus)!.color"
                            :icon="statusBadge(row.original.assetStatus)!.icon"
                            variant="subtle">
                        {{ statusBadge(row.original.assetStatus)!.label }}
                    </UBadge>
                </div>
                <!-- Destination leaf row -->
                <div v-else
                     class="flex items-center gap-2 pl-16">
                    <template v-if="row.original.destinationName">
                        <UIcon :name="row.original.destinationIcon ?? 'i-lucide-hard-drive'"
                               class="size-4 shrink-0 text-dimmed" />
                        <span class="text-dimmed">{{ row.original.destinationName }}</span>
                    </template>
                    <span v-else
                          class="text-dimmed italic">No destinations</span>
                </div>
            </template>

            <!-- Tags (asset level only) -->
            <template #tags-cell="{ row }">
                <div v-if="row.getIsGrouped() && row.depth === 1 && row.original.tags.length > 0"
                     class="flex items-center justify-center gap-1 flex-wrap">
                    <UBadge v-for="tag in row.original.tags"
                            :key="tag"
                            color="neutral"
                            variant="subtle">
                        {{ tag }}
                    </UBadge>
                </div>
            </template>

            <!-- Dependencies -->
            <template #dependencies-cell="{ row }">
                <!-- Source level: show warnings only -->
                <template v-if="row.getIsGrouped() && row.depth === 0">
                    <div v-if="filterByCategory(sourceInfoById.get(row.original.sourceId)?.warnings ?? [], 'dependency').length > 0"
                         class="flex items-center justify-center gap-1.5">
                        <UTooltip :delay-duration="0"
                                  :ui="{ content: 'bg-transparent ring-0 shadow-none p-0 rounded-none' }"
                                  :content="{ side: 'top', sideOffset: 12 }">
                            <UIcon name="i-lucide-triangle-alert"
                                   class="size-4.5 shrink-0 text-warning" />
                            <template #content>
                                <div class="rounded-lg border border-default bg-default shadow-lg overflow-hidden">
                                    <table class="text-xs w-full">
                                        <tbody>
                                            <tr v-for="(w, i) in filterByCategory(sourceInfoById.get(row.original.sourceId)?.warnings ?? [], 'dependency')"
                                                :key="i"
                                                class="border-b border-default last:border-b-0">
                                                <td class="px-3 py-2">
                                                    <div class="flex items-center gap-2">
                                                        <UIcon name="i-lucide-circle-alert"
                                                               class="size-3.5 shrink-0 text-warning" />
                                                        <span>{{ w.message }}</span>
                                                    </div>
                                                </td>
                                            </tr>
                                        </tbody>
                                    </table>
                                </div>
                            </template>
                        </UTooltip>
                    </div>
                </template>
                <!-- Asset level: show count + warnings -->
                <template v-else-if="row.getIsGrouped() && row.depth === 1">
                    <div class="flex items-center justify-center gap-1.5">
                        <span v-if="row.original.dependencies.length === 0 && filterByCategory(row.original.warnings, 'dependency').length === 0"
                              class="text-dimmed">&mdash;</span>
                        <span v-else-if="row.original.dependencies.length > 0"
                              class="text-muted">{{ row.original.dependencies.length }}</span>
                        <UTooltip v-if="filterByCategory(row.original.warnings, 'dependency').length > 0"
                                  :delay-duration="0"
                                  :ui="{ content: 'bg-transparent ring-0 shadow-none p-0 rounded-none' }"
                                  :content="{ side: 'top', sideOffset: 12 }">
                            <UIcon name="i-lucide-triangle-alert"
                                   class="size-4.5 shrink-0 text-warning" />
                            <template #content>
                                <div class="rounded-lg border border-default bg-default shadow-lg overflow-hidden">
                                    <table class="text-xs w-full">
                                        <tbody>
                                            <tr v-for="(w, i) in filterByCategory(row.original.warnings, 'dependency')"
                                                :key="i"
                                                class="border-b border-default last:border-b-0">
                                                <td class="px-3 py-2">
                                                    <div class="flex items-center gap-2">
                                                        <UIcon name="i-lucide-circle-alert"
                                                               class="size-3.5 shrink-0 text-warning" />
                                                        <span>{{ w.message }}</span>
                                                    </div>
                                                </td>
                                            </tr>
                                        </tbody>
                                    </table>
                                </div>
                            </template>
                        </UTooltip>
                    </div>
                </template>
            </template>

            <!-- Destinations (source and asset level) -->
            <template #destinations-cell="{ row }">
                <div v-if="row.getIsGrouped()"
                     class="flex items-center justify-center gap-1.5 flex-wrap">
                    <span v-if="row.original.destinations.length === 0 && filterByCategory(row.depth === 0 ? (sourceInfoById.get(row.original.sourceId)?.warnings ?? []) : row.original.warnings, 'destination').length === 0"
                          class="text-dimmed">&mdash;</span>
                    <UBadge v-else-if="row.original.destinations.length === 1"
                            :icon="row.original.destinations[0]!.icon"
                            color="neutral"
                            variant="subtle">
                        {{ row.original.destinations[0]!.name }}
                    </UBadge>
                    <span v-else-if="row.original.destinations.length > 1"
                          class="text-muted">{{ row.original.destinations.length }}</span>
                    <UTooltip v-if="filterByCategory(row.depth === 0 ? (sourceInfoById.get(row.original.sourceId)?.warnings ?? []) : row.original.warnings, 'destination').length > 0"
                              :delay-duration="0"
                              :ui="{ content: 'bg-transparent ring-0 shadow-none p-0 rounded-none' }"
                              :content="{ side: 'top', sideOffset: 12 }">
                        <UIcon name="i-lucide-triangle-alert"
                               class="size-4.5 shrink-0 text-warning" />
                        <template #content>
                            <div class="rounded-lg border border-default bg-default shadow-lg overflow-hidden">
                                <table class="text-xs w-full">
                                    <tbody>
                                        <tr v-for="(w, i) in filterByCategory(row.depth === 0 ? (sourceInfoById.get(row.original.sourceId)?.warnings ?? []) : row.original.warnings, 'destination')"
                                            :key="i"
                                            class="border-b border-default last:border-b-0">
                                            <td class="px-3 py-2">
                                                <div class="flex items-center gap-2">
                                                    <UIcon name="i-lucide-circle-alert"
                                                           class="size-3.5 shrink-0 text-warning" />
                                                    <span>{{ w.message }}</span>
                                                </div>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </template>
                    </UTooltip>
                </div>
            </template>

            <!-- Jobs (source and asset level) -->
            <template #jobs-cell="{ row }">
                <div v-if="row.getIsGrouped()"
                     class="flex items-center justify-center gap-1.5">
                    <span v-if="row.original.jobs.length === 0 && filterByCategory(row.depth === 0 ? (sourceInfoById.get(row.original.sourceId)?.warnings ?? []) : row.original.warnings, 'job').length === 0"
                          class="text-dimmed">&mdash;</span>
                    <UBadge v-else-if="row.original.jobs.length === 1"
                            icon="i-lucide-clock"
                            color="neutral"
                            variant="subtle">
                        {{ row.original.jobs[0]!.name }}
                    </UBadge>
                    <span v-else-if="row.original.jobs.length > 1"
                          class="text-muted">{{ row.original.jobs.length }}</span>
                    <UTooltip v-if="filterByCategory(row.depth === 0 ? (sourceInfoById.get(row.original.sourceId)?.warnings ?? []) : row.original.warnings, 'job').length > 0"
                              :delay-duration="0"
                              :ui="{ content: 'bg-transparent ring-0 shadow-none p-0 rounded-none' }"
                              :content="{ side: 'top', sideOffset: 12 }">
                        <UIcon name="i-lucide-triangle-alert"
                               class="size-4.5 shrink-0 text-warning" />
                        <template #content>
                            <div class="rounded-lg border border-default bg-default shadow-lg overflow-hidden">
                                <table class="text-xs w-full">
                                    <tbody>
                                        <tr v-for="(w, i) in filterByCategory(row.depth === 0 ? (sourceInfoById.get(row.original.sourceId)?.warnings ?? []) : row.original.warnings, 'job')"
                                            :key="i"
                                            class="border-b border-default last:border-b-0">
                                            <td class="px-3 py-2">
                                                <div class="flex items-center gap-2">
                                                    <UIcon name="i-lucide-circle-alert"
                                                           class="size-3.5 shrink-0 text-warning" />
                                                    <span>{{ w.message }}</span>
                                                </div>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </template>
                    </UTooltip>
                </div>
            </template>

            <!-- Last Run (source and asset level) -->
            <template #lastRun-cell="{ row }">
                <template v-if="row.getIsGrouped()">
                    <span v-if="!row.original.lastRunStatus || !row.original.lastRunAt"
                          class="text-dimmed">&mdash;</span>
                    <div v-else
                         class="flex items-center justify-center">
                        <UBadge :color="statusColor[row.original.lastRunStatus] ?? 'neutral'"
                                :icon="statusIcon[row.original.lastRunStatus] ?? 'i-lucide-circle'"
                                variant="subtle">
                            {{ timeSince(new Date(row.original.lastRunAt)) }} ago
                        </UBadge>
                    </div>
                </template>
            </template>

            <!-- Connection (all levels) -->
            <template #connection-cell="{ row }">
                <template v-if="row.getIsGrouped()">
                    <span v-if="!row.original.connectionName"
                          class="text-dimmed">&mdash;</span>
                    <UBadge v-else
                            :icon="row.original.connectionIcon ?? 'i-lucide-plug'"
                            color="neutral"
                            variant="subtle">
                        {{ row.original.connectionName }}
                    </UBadge>
                </template>
                <template v-else>
                    <span class="text-dimmed">&mdash;</span>
                </template>
            </template>

            <!-- Created (all levels) -->
            <template #created-cell="{ row }">
                <template v-if="row.getIsGrouped()">
                    <span class="text-muted">
                        {{ row.original.createdAt ? timeSince(new Date(row.original.createdAt)) + ' ago' : '—' }}</span>
                </template>
                <template v-else>
                    <span class="text-muted">
                        {{ row.original.destinationCreatedAt ? timeSince(new Date(row.original.destinationCreatedAt)) +
                            ' ago' : '—' }}</span>
                </template>
            </template>

            <!-- Actions (source and asset rows) -->
            <template #actions-cell="{ row }">
                <div v-if="row.getIsGrouped() && row.depth === 0"
                     class="flex justify-end">
                    <UDropdownMenu :items="buildSourceActions(row.original.sourceId)">
                        <UButton icon="i-lucide-ellipsis-vertical"
                                 color="neutral"
                                 variant="ghost"
                                 size="sm"
                                 @click.stop />
                    </UDropdownMenu>
                </div>
                <div v-else-if="row.getIsGrouped() && row.depth === 1"
                     class="flex justify-end">
                    <UDropdownMenu :items="buildAssetActions(row.original.assetId, row.original.sourceId)">
                        <UButton icon="i-lucide-ellipsis-vertical"
                                 color="neutral"
                                 variant="ghost"
                                 size="sm"
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

        <TableFooter :page="sourcePageIndex + 1"
                     :total="uniqueSourceIds.length"
                     :page-size="SOURCE_PAGE_SIZE"
                     @update:page="(p: number) => sourcePageIndex = p - 1">
            {{ sources.length }} {{ sources.length === 1 ? 'source' : 'sources' }},
            {{ assetCount }} {{ assetCount === 1 ? 'asset' : 'assets' }}
        </TableFooter>
    </div>
</template>
