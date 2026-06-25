<script setup lang="ts">
import { Handle, Position, useNodeConnections } from '@vue-flow/core'

/**
 * Compact run-graph node: status dot + type icon + name + duration, sized for
 * a dense left-to-right DAG. Left/Right handles match the LR flow (the catalog
 * AssetNode is large and uses Top/Bottom handles, made for the TB catalog).
 */
const props = defineProps<{
    asset: SourceAsset
    assetDefn: AssetDefinition | undefined
    status?: NodeStatus
    viewMode?: ViewMode
    /** VueFlow selection state — drives the selection ring. */
    selected?: boolean
}>()

defineEmits<{ view: [] }>()

const icon = computed(() => props.assetDefn ? componentIcon(props.assetDefn.key) : 'i-lucide-box')
const label = computed(() => props.assetDefn?.name ?? props.asset.key)
const state = computed<GraphNodeState>(() => props.status?.state ?? 'idle')

const sourceConnections = useNodeConnections({ handleType: 'source' })
const targetConnections = useNodeConnections({ handleType: 'target' })
const hasUpstream = computed(() => targetConnections.value.length > 0)
const hasDownstream = computed(() => sourceConnections.value.length > 0)

const ringClass = computed(() => props.selected ? 'ring-2 ring-primary' : statusRingClass(state.value))
</script>

<template>
    <div class="relative w-[190px]">
        <Handle v-if="hasUpstream"
                type="target"
                :position="Position.Left"
                :connectable="false" />

        <div class="flex items-center gap-2 rounded-lg border border-[var(--ui-border-accented)] bg-muted px-3 py-2 shadow-[0_8px_24px_-12px_rgba(0,0,0,0.55)]"
             :class="ringClass">
            <span class="size-2 shrink-0 rounded-full"
                  :class="statusDotClass(state)" />
            <UIcon :name="icon"
                   class="size-4 shrink-0 text-muted" />
            <span class="min-w-0 flex-1 truncate text-xs font-medium text-highlighted">{{ label }}</span>
            <span v-if="status?.label"
                  class="shrink-0 text-[11px] tabular-nums text-muted">{{ status.label }}</span>
        </div>

        <Handle v-if="hasDownstream"
                type="source"
                :position="Position.Right"
                :connectable="false" />
    </div>
</template>
