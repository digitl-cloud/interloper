<script setup lang="ts">
/**
 * Graph canvas toolbar. Status filter, expand-mode and view-mode are all
 * rendered as the same left-aligned segmented control; the `end` slot holds
 * host actions (e.g. New Source), pushed to the right.
 */
const expandMode = defineModel<ExpandMode>('expandMode', { default: 'nodes' })
const viewMode = defineModel<ViewMode>('viewMode', { default: 'topology' })
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

const EXPAND_OPTIONS = [
    { value: 'list', label: 'List', icon: 'i-lucide-list' },
    { value: 'graph', label: 'Graph', icon: 'i-lucide-git-fork' },
    { value: 'nodes', label: 'Nodes', icon: 'i-lucide-box' },
]

const VIEW_OPTIONS = [
    { value: 'topology', label: 'Topology', icon: 'i-lucide-workflow' },
    { value: 'status', label: 'Status', icon: 'i-lucide-activity' },
]

// Hide a filter pill when it has no members (except All), to avoid dead options.
const filterItems = computed(() => FILTERS
    .filter(f => f.value === 'all' || props.counts[f.value] > 0)
    .map(f => ({ label: f.label, value: f.value, badge: props.counts[f.value], dot: f.dot })))
</script>

<template>
    <div class="flex shrink-0 items-center gap-2 border-b border-default px-4 py-2">
        <!-- Status filter -->
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

        <!-- Expand mode -->
        <UTabs v-model="expandMode"
               :items="EXPAND_OPTIONS"
               variant="pill"
               size="xs"
               :content="false" />

        <!-- View mode -->
        <UTabs v-model="viewMode"
               :items="VIEW_OPTIONS"
               variant="pill"
               size="xs"
               :content="false" />

        <div class="ml-auto">
            <slot name="end" />
        </div>
    </div>
</template>
