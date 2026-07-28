<script setup lang="ts">
/**
 * Graph canvas toolbar. Status filter pills on the left with the group-by
 * tabs beside them; the `end` slot holds host actions (e.g. New Source),
 * pushed to the right.
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
    <div class="flex shrink-0 items-center gap-2 border-b border-default px-4 py-2">
        <!-- Status filter -->
        <span class="text-xs text-muted">Status</span>
        <UTabs v-model="statusFilter"
               :items="filterItems"
               variant="pill"
               size="xs"
               :content="false">
            <template #leading="{ item }">
                <span v-if="(item as any).dot"
                      class="size-1.5 rounded-full"
                      :class="statusDotClass((item as any).dot)" />
            </template>
        </UTabs>

        <!-- Group by -->
        <span class="text-xs text-muted">Group by</span>
        <UTabs v-model="groupBy"
               :items="GROUP_OPTIONS"
               variant="pill"
               size="xs"
               :content="false" />

        <div class="ml-auto">
            <slot name="end" />
        </div>
    </div>
</template>
