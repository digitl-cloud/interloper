<script setup lang="ts">
/**
 * The Assets page's toolbar, the same for both its views: a search, which view
 * shows, the sources' status and the grouping. The search, status and grouping
 * narrow the graph and the table alike.
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

const groupOptions = computed(() => [
    { value: 'type', label: 'Type', icon: appConfig.icons.type },
    { value: 'source', label: 'Source', icon: appConfig.icons.source },
    { value: 'asset', label: 'Asset', icon: appConfig.icons.asset },
])

const FILTERS: Array<{ value: StatusFilter; label: string; dot?: GraphNodeState }> = [
    { value: 'all', label: 'All' },
    { value: 'healthy', label: 'Healthy', dot: 'idle' },
    { value: 'attention', label: 'Attention', dot: 'attention' },
    { value: 'paused', label: 'Paused', dot: 'paused' },
]

// Hide a filter tab when it has no members (except All), to avoid dead options.
const filterItems = computed(() => FILTERS
    .filter(f => f.value === 'all' || props.counts[f.value] > 0)
    .map(f => ({ label: f.label, value: f.value, badge: props.counts[f.value], dot: f.dot })))

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
        <FilterBarField label="Status">
            <UTabs v-model="statusFilter"
                   :items="filterItems">
                <template #leading="{ item }">
                    <StatusDot v-if="dotOf(item)"
                               :color="nodeStateColor(dotOf(item)!)"
                               :pulse="nodeStateExecuting(dotOf(item)!)" />
                </template>
            </UTabs>
        </FilterBarField>
        <FilterBarField label="Group by">
            <UTabs v-model="groupBy"
                   :items="groupOptions" />
        </FilterBarField>
    </FilterBar>
</template>
