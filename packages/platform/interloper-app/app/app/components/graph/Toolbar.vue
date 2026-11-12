<script setup lang="ts">
/**
 * Graph canvas toolbar: the status filter tabs, then the group-by tabs, with
 * the page's `actions` slot at the far end.
 */
const groupBy = defineModel<GroupBy>('groupBy', { default: 'type' })
const statusFilter = defineModel<StatusFilter>('statusFilter', { default: 'all' })

const props = defineProps<{
    /** Per-state source counts for the filter tabs. */
    counts: Record<StatusFilter, number>
}>()

const appConfig = useAppConfig()

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
    <div class="flex shrink-0 flex-wrap items-center gap-3">
        <span class="text-sm text-muted">Status</span>
        <UTabs v-model="statusFilter"
               :items="filterItems"
               variant="pill"
               size="sm"
               :content="false">
            <template #leading="{ item }">
                <StatusDot v-if="dotOf(item)"
                           :color="nodeStateColor(dotOf(item)!)"
                           :pulse="nodeStateExecuting(dotOf(item)!)" />
            </template>
        </UTabs>
        <span class="ms-4 text-sm text-muted">Group by</span>
        <UTabs v-model="groupBy"
               :items="groupOptions"
               variant="pill"
               size="sm"
               :content="false" />
        <div class="ms-auto flex items-center gap-2">
            <slot name="actions" />
        </div>
    </div>
</template>
