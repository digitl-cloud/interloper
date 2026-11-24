<script setup lang="ts">
/**
 * The Assets page's toolbar, the same for both its views: a search and which
 * view shows, then the sources' status and the grouping behind their menus. The
 * search, status and grouping narrow the graph and the table alike.
 */
const view = defineModel<string>('view', { required: true })
const query = defineModel<string>('query', { required: true })
const groupBy = defineModel<GroupBy>('groupBy', { required: true })
const statusFilter = defineModel<StatusFilter>('statusFilter', { required: true })

const props = defineProps<{
    /** Per-state source counts for the status tabs. */
    counts: Record<StatusFilter, number>
}>()

const appConfig = useAppConfig()

const viewItems = useAssetViews().map(option => ({ label: option.label, value: option.value, icon: option.icon }))

const groupOptions = computed<Array<{ value: GroupBy, label: string, icon: string }>>(() => [
    { value: 'type', label: 'Type', icon: appConfig.icons.type },
    { value: 'source', label: 'Source', icon: appConfig.icons.source },
    { value: 'asset', label: 'Asset', icon: appConfig.icons.asset },
])
const grouping = computed(() => groupOptions.value.find(option => option.value === groupBy.value))

const FILTERS: Array<{ value: StatusFilter; label: string; dot?: GraphNodeState }> = [
    { value: 'all', label: 'All' },
    { value: 'healthy', label: 'Healthy', dot: 'idle' },
    { value: 'attention', label: 'Attention', dot: 'attention' },
    { value: 'paused', label: 'Paused', dot: 'paused' },
]

// Hide a status when it has no members (except All), to avoid dead options.
const filterItems = computed(() => FILTERS
    .filter(f => f.value === 'all' || props.counts[f.value] > 0)
    .map(f => ({ label: f.label, value: f.value, badge: props.counts[f.value], dot: f.dot })))
// The button reads "Filter" until a status narrows the list, then names it.
const statusTrigger = computed(() => statusFilter.value === 'all'
    ? 'Filter'
    : FILTERS.find(f => f.value === statusFilter.value)?.label ?? 'Filter')

function dotOf(item: unknown): GraphNodeState | undefined {
    return (item as { dot?: GraphNodeState }).dot
}
</script>

<template>
    <FilterBar class="shrink-0"
               :active="statusFilter === 'all' ? 0 : 1"
               @reset="statusFilter = 'all'">
        <template #search>
            <UInput v-model="query"
                    placeholder="Search assets…"
                    :icon="appConfig.ui.icons.search"
                    class="w-full sm:w-44" />
        </template>
        <FilterBarField label="View">
            <UTabs v-model="view"
                   :items="viewItems" />
        </FilterBarField>
        <FilterMenu v-model="statusFilter"
                    label="Status"
                    :items="filterItems"
                    :trigger="statusTrigger"
                    :icon="appConfig.icons.filter">
            <template #leading="{ item }">
                <StatusDot v-if="dotOf(item)"
                           :color="nodeStateColor(dotOf(item)!)"
                           :pulse="nodeStateExecuting(dotOf(item)!)" />
            </template>
        </FilterMenu>
        <FilterMenu v-model="groupBy"
                    label="Group by"
                    :items="groupOptions"
                    :trigger="`Grouped by ${grouping?.label.toLowerCase()}`"
                    :icon="grouping?.icon" />
    </FilterBar>
</template>
