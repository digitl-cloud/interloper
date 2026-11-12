<script setup lang="ts">
import { Handle, Position, useNodeConnections } from '@vue-flow/core'
import type { ComponentRecord } from '~/types/component'

/**
 * Compact run-graph node: status dot, type icon, name and duration, sized for
 * a dense left-to-right DAG (GraphCanvas's compact asset size). Its status
 * tints the border for the states that tint it.
 */
const props = defineProps<{
    asset: ComponentRecord
    assetDefn: AssetDefinition | undefined
    status?: NodeStatus
    /** The open asset: drives the selection outline. */
    selected?: boolean
}>()

defineEmits<{ view: [] }>()

const appConfig = useAppConfig()
const icon = computed(() => props.assetDefn ? componentIcon(props.assetDefn.key) : appConfig.icons.asset)
const label = computed(() => props.assetDefn?.name ?? props.asset.key)
const state = computed<GraphNodeState>(() => props.status?.state ?? 'idle')

const sourceConnections = useNodeConnections({ handleType: 'source' })
const targetConnections = useNodeConnections({ handleType: 'target' })
const hasUpstream = computed(() => targetConnections.value.length > 0)
const hasDownstream = computed(() => sourceConnections.value.length > 0)

const frameClass = computed(() => [
    statusBorderClass(state.value) || 'border-(--graph-card-line)',
    props.selected && GRAPH_SELECTED_CLASS,
])
</script>

<template>
    <div class="relative w-50">
        <Handle v-if="hasUpstream"
                type="target"
                :position="Position.Left"
                :connectable="false" />

        <div class="graph-asset flex h-11 items-center gap-2 rounded-xl border bg-(--graph-node-bg) px-3"
             :class="frameClass">
            <StatusDot :color="nodeStateColor(state)"
                       :pulse="nodeStateExecuting(state)" />
            <UIcon :name="icon"
                   class="size-4 shrink-0 text-muted" />
            <span class="min-w-0 flex-1 truncate text-sm font-medium text-highlighted">{{ label }}</span>
            <span v-if="status?.label"
                  class="shrink-0 text-xs tabular-nums text-muted">{{ status.label }}</span>
        </div>

        <Handle v-if="hasDownstream"
                type="source"
                :position="Position.Right"
                :connectable="false" />
    </div>
</template>
