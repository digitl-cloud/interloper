<script setup lang="ts">
/**
 * Graph canvas toolbar. Owns the "Expand as" control today; filters and
 * view modes are added in Phase 3. An `end` slot holds host actions
 * (e.g. New Source).
 */
const expandMode = defineModel<ExpandMode>('expandMode', { default: 'nodes' })

const EXPAND_OPTIONS: Array<{ value: ExpandMode; label: string; icon: string }> = [
    { value: 'list', label: 'List', icon: 'i-lucide-list' },
    { value: 'graph', label: 'Graph', icon: 'i-lucide-git-fork' },
    { value: 'nodes', label: 'Nodes', icon: 'i-lucide-box' },
]
</script>

<template>
    <div class="flex shrink-0 items-center justify-between gap-3 border-b border-default px-4 py-2">
        <div class="flex items-center gap-2">
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
        <slot name="end" />
    </div>
</template>
