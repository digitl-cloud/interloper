<script setup lang="ts">
/**
 * Graph canvas toolbar: status filter pills (left), expand-mode + view-mode
 * controls and host actions (right). An `end` slot holds host actions such
 * as New Source.
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

const EXPAND_OPTIONS: Array<{ value: ExpandMode; label: string; icon: string }> = [
    { value: 'list', label: 'List', icon: 'i-lucide-list' },
    { value: 'graph', label: 'Graph', icon: 'i-lucide-git-fork' },
    { value: 'nodes', label: 'Nodes', icon: 'i-lucide-box' },
]

const VIEW_OPTIONS: Array<{ value: ViewMode; label: string; icon: string }> = [
    { value: 'topology', label: 'Topology', icon: 'i-lucide-workflow' },
    { value: 'status', label: 'Status', icon: 'i-lucide-activity' },
]

// Hide a filter pill when it has no members (except All), to avoid dead options.
const visibleFilters = computed(() => FILTERS.filter(f => f.value === 'all' || props.counts[f.value] > 0))
</script>

<template>
    <div class="flex shrink-0 items-center justify-between gap-3 border-b border-default px-4 py-2">
        <!-- Status filter pills -->
        <div class="flex items-center gap-1">
            <button v-for="f in visibleFilters"
                    :key="f.value"
                    type="button"
                    class="flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium transition-colors"
                    :class="statusFilter === f.value
                        ? 'border-default bg-elevated text-highlighted'
                        : 'border-transparent text-muted hover:text-default'"
                    @click="statusFilter = f.value">
                <span v-if="f.dot"
                      class="size-1.5 rounded-full"
                      :class="statusDotClass(f.dot)" />
                {{ f.label }}
                <span class="text-dimmed">{{ counts[f.value] }}</span>
            </button>
        </div>

        <!-- Controls -->
        <div class="flex items-center gap-3">
            <div class="hidden items-center gap-2 sm:flex">
                <span class="text-xs text-muted">Expand as</span>
                <div class="flex items-center gap-0.5 rounded-lg bg-elevated p-0.5">
                    <button v-for="opt in EXPAND_OPTIONS"
                            :key="opt.value"
                            type="button"
                            class="flex items-center gap-1.5 rounded-md px-2.5 py-1 text-xs font-medium transition-colors"
                            :class="expandMode === opt.value
                                ? 'bg-default text-highlighted shadow-sm'
                                : 'text-muted hover:text-default'"
                            @click="expandMode = opt.value">
                        <UIcon :name="opt.icon"
                               class="size-3.5" />
                        {{ opt.label }}
                    </button>
                </div>
            </div>

            <div class="flex items-center gap-0.5 rounded-lg bg-elevated p-0.5">
                <button v-for="opt in VIEW_OPTIONS"
                        :key="opt.value"
                        type="button"
                        class="flex items-center gap-1.5 rounded-md px-2.5 py-1 text-xs font-medium transition-colors"
                        :class="viewMode === opt.value
                            ? 'bg-default text-highlighted shadow-sm'
                            : 'text-muted hover:text-default'"
                        @click="viewMode = opt.value">
                    <UIcon :name="opt.icon"
                           class="size-3.5" />
                    {{ opt.label }}
                </button>
            </div>

            <slot name="end" />
        </div>
    </div>
</template>
