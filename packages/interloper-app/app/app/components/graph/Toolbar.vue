<script setup lang="ts">
/**
 * Graph canvas toolbar: status filter pills on the left, the group-by tabs
 * on the right, followed by the page's `actions` slot.
 */
const groupBy = defineModel<GroupBy>('groupBy', { default: 'type' })

const GROUP_OPTIONS = [
    { value: 'type', label: 'Type', icon: 'i-lucide-group' },
    { value: 'source', label: 'Source', icon: 'i-lucide-plug' },
    { value: 'asset', label: 'Asset', icon: 'i-lucide-box' },
]
const statusFilter = defineModel<StatusFilter>('statusFilter', { default: 'all' })

const props = defineProps<{
    /** Per-state source counts for the filter pills. */
    counts: Record<StatusFilter, number>
}>()

const FILTERS: Array<{ value: StatusFilter; label: string; dot?: GraphNodeState }> = [
    { value: 'all', label: 'All' },
    { value: 'healthy', label: 'Healthy', dot: 'idle' },
    { value: 'attention', label: 'Attention', dot: 'attention' },
    { value: 'paused', label: 'Paused', dot: 'paused' },
]

// Hide a filter pill when it has no members (except All), to avoid dead options.
const filterItems = computed(() => FILTERS
    .filter(f => f.value === 'all' || props.counts[f.value] > 0)
    .map(f => ({ label: f.label, value: f.value, badge: props.counts[f.value], dot: f.dot })))
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
                <span v-if="(item as any).dot"
                      class="size-1.5 rounded-full"
                      :class="statusDotClass((item as any).dot)" />
            </template>
        </UTabs>
        <span class="ms-4 text-sm text-muted">Group by</span>
        <UTabs v-model="groupBy"
               :items="GROUP_OPTIONS"
               variant="pill"
               size="sm"
               :content="false" />
        <div class="ml-auto flex items-center gap-2">
            <slot name="actions" />
        </div>
    </div>
</template>
