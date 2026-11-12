<script setup lang="ts">
import type { ContextMenuItem } from '@nuxt/ui'
import { Handle, Position, useVueFlow, useNodeConnections, useNodeId } from '@vue-flow/core'
import type { Connection } from '@vue-flow/core'
import type { ComponentRecord } from '~/types/component'
import { requiredUpstreams } from '~/types/catalog'

/**
 * An asset card on the graph: name, status dot, description, tags and the
 * latest status label, with corner badges for materializing, drift, warnings
 * and destinations. Its target handle shows while the asset still needs an
 * upstream, and both handles light up as drop targets during a connection drag.
 */
const props = defineProps<{
    asset: ComponentRecord
    assetDefn: AssetDefinition | undefined
    /** Derived node status: the dot, and the border for the states that tint it. */
    status?: NodeStatus
    /** The open asset: drives the selection outline. */
    selected?: boolean
    /** Not owned by a source (a model over other assets): gets the function glyph. */
    standalone?: boolean
}>()

const emit = defineEmits<{
    view: []
}>()

const componentsStore = useComponentsStore()
const { getWarnings } = useAssetWarnings()
const { getBadgeForAssetId } = useDestinationBadge()
const { statusBadge } = useDrift()
const { confirm } = useConfirm()
const toast = useToast()
const appConfig = useAppConfig()

const isMissing = computed(() => props.asset.status === 'missing')
const driftBadge = computed(() => statusBadge(props.asset.status))
const nodeId = useNodeId()
const { connectionStartHandle } = useVueFlow()

const isValidConnection = inject<(connection: Connection) => boolean>('isValidConnection')
const graphReadonly = inject<Ref<boolean>>('graphReadonly', ref(false))
const materializingAssetIds = inject<ComputedRef<Set<string>>>('materializingAssetIds')

const sourceConnections = useNodeConnections({ handleType: 'source' })
const targetConnections = useNodeConnections({ handleType: 'target' })

const hasDownstream = computed(() => sourceConnections.value.length > 0)
const hasUpstream = computed(() => targetConnections.value.length > 0)
const isMaterializing = computed(() => materializingAssetIds?.value?.has(props.asset.id) ?? false)

const label = computed(() => props.asset.key)
const description = computed(() => props.assetDefn?.description ?? '')
const tags = computed(() => props.assetDefn?.tags ?? [])
const state = computed<GraphNodeState>(() => props.status?.state ?? 'idle')
const statusLabel = computed(() => props.status?.label ?? null)
/** The status label takes its state's colour while the state is loud; otherwise it reads as meta. */
const LOUD_TEXT: Partial<Record<StatusColor, string>> = { error: 'text-error', warning: 'text-warning', info: 'text-info' }
const statusTextClass = computed(() => LOUD_TEXT[nodeStateColor(state.value)] ?? 'text-dimmed')
const warnings = computed(() => getWarnings(props.asset.id, props.asset.key))
const destinationBadge = computed(() => getBadgeForAssetId(props.asset.id))

// Is a connection drag currently in progress?
const isDragging = computed(() => connectionStartHandle.value !== null)

// Is this the node that started the drag?
const isDragSource = computed(() => {
    const start = connectionStartHandle.value
    return start !== null && start.nodeId === nodeId
})

// Would this asset's target handle be a valid drop target for the current drag?
const isValidTarget = computed(() => {
    const start = connectionStartHandle.value
    if (!start || start.type !== 'source' || !nodeId) return false
    return isValidConnection?.({
        source: start.nodeId,
        target: nodeId,
        sourceHandle: start.id ?? null,
        targetHandle: null,
    }) ?? false
})

const isValidSource = computed(() => {
    const start = connectionStartHandle.value
    if (!start || start.type !== 'target' || !nodeId) return false
    return isValidConnection?.({
        source: nodeId,
        target: start.nodeId,
        sourceHandle: null,
        targetHandle: start.id ?? null,
    }) ?? false
})

const isCompatible = computed(() => isValidTarget.value || isValidSource.value)
const shouldFade = computed(() => isDragging.value && !isDragSource.value && !isCompatible.value)

// Check if this asset has all required upstream dependencies
const requiredDepCount = computed(() =>
    props.assetDefn ? Object.keys(requiredUpstreams(props.assetDefn)).length : 0,
)
const isRunnable = computed(() => {
    if (requiredDepCount.value === 0) return true
    const upstreams = componentsStore.upstreams.filter(d => d.src_id === props.asset.id)
    return upstreams.length >= requiredDepCount.value
})

const showTargetHandle = computed(() => hasUpstream.value || !isRunnable.value || isValidTarget.value)
const showSourceHandle = computed(() => hasDownstream.value || isValidSource.value)

const frameClass = computed(() => [
    statusBorderClass(state.value) || 'border-(--graph-card-line)',
    props.selected && GRAPH_SELECTED_CLASS,
])

const contextMenuItems = computed<ContextMenuItem[][]>(() => {
    const items: ContextMenuItem[][] = [
        [
            {
                label: 'View',
                icon: appConfig.ui.icons.eye,
                onSelect: () => emit('view'),
            },
        ],
    ]
    if (hasUpstream.value) {
        items.push([
            {
                label: 'Disconnect upstream',
                icon: 'i-lucide-unlink',
                color: 'error' as const,
                onSelect: async () => {
                    const confirmed = await confirm({
                        title: 'Disconnect upstream dependencies',
                        description: `This will remove all upstream dependencies from "${label.value}".`,
                    })
                    if (confirmed) {
                        const deps = componentsStore.upstreams.filter(d => d.src_id === props.asset.id)
                        // Sequential, not concurrent: a non-optional relation refuses to
                        // reach zero bindings, and parallel deletes make which leg keeps
                        // it (and so which call 400s) a race.
                        try {
                            for (const d of deps) await componentsStore.removeRelation(d.src_id, d.name, d.dst_id)
                        }
                        catch (e) {
                            toast.add(errorToast(e, 'Couldn\'t disconnect upstream'))
                        }
                    }
                },
            },
        ])
    }
    return items
})
</script>

<template>
    <UContextMenu :items="graphReadonly ? [] : contextMenuItems">
        <div class="graph-node relative w-64 transition-opacity duration-200"
             :style="shouldFade ? { opacity: GRAPH_DIM_OPACITY } : undefined">
            <Handle v-if="showTargetHandle"
                    type="target"
                    :position="Position.Left"
                    :connectable-start="!graphReadonly"
                    :connectable-end="false"
                    :is-valid-connection="isValidConnection"
                    :class="[
                        'transition-all duration-150',
                        isValidTarget && '!size-3 !bg-transparent !border-2 !border-primary animate-pulse-grow',
                        !isRunnable && !isValidTarget && '!size-2.5 !bg-transparent !border-2 !border-warning',
                    ]" />

            <GraphCornerBadge v-if="isMaterializing"
                              :icon="appConfig.ui.icons.loading"
                              corner="top-left"
                              spin>
                Materializing
            </GraphCornerBadge>

            <!-- Drift: the asset key no longer resolves against the catalog. -->
            <GraphCornerBadge v-if="isMissing"
                              :icon="driftBadge?.icon ?? appConfig.icons.drift"
                              corner="top-right"
                              tone="error">
                {{ driftBadge?.label }}
            </GraphCornerBadge>

            <GraphCornerBadge v-if="warnings.length > 0 && !isMissing"
                              :icon="appConfig.ui.icons.warning"
                              corner="top-right"
                              tone="warning">
                <CollectionWarnings :warnings="warnings" />
            </GraphCornerBadge>

            <GraphCornerBadge v-if="destinationBadge"
                              :icon="destinationBadge.icon"
                              corner="bottom-right"
                              tone="primary"
                              :count="destinationBadge.count">
                {{ destinationBadge.label }}
            </GraphCornerBadge>

            <div class="graph-asset graph-raise graph-stack-floor flex h-32 flex-col overflow-hidden rounded-xl border bg-(--graph-node-bg)"
                 :class="frameClass">
                <div class="flex h-10 shrink-0 items-center gap-2 border-b border-(--graph-head-line) bg-elevated px-3">
                    <UIcon :name="standalone ? 'i-lucide-square-function' : appConfig.icons.asset"
                           class="size-4 shrink-0"
                           :class="standalone ? 'text-(--graph-model)' : 'text-primary'" />
                    <span class="min-w-0 truncate text-sm font-semibold text-highlighted">{{ label }}</span>
                    <StatusDot :color="nodeStateColor(state)"
                               :pulse="nodeStateExecuting(state)" />
                </div>
                <div class="flex min-h-0 flex-1 flex-col justify-between p-3">
                    <p class="line-clamp-2 h-8 text-xs text-muted"
                       :class="isMissing && 'italic text-dimmed'">{{ isMissing ? 'No catalog definition' : description }}</p>
                    <div class="flex h-5 min-w-0 items-center gap-2">
                        <UBadge v-for="tag in tags"
                                :key="tag"
                                color="neutral"
                                size="sm"
                                class="shrink-0"
                                :label="tag" />
                        <span v-if="statusLabel"
                              class="min-w-0 truncate text-xs tabular-nums"
                              :class="statusTextClass">{{ statusLabel }}</span>
                    </div>
                </div>
            </div>

            <Handle v-if="showSourceHandle"
                    type="source"
                    :position="Position.Right"
                    :connectable-start="!graphReadonly"
                    :connectable-end="!graphReadonly"
                    :is-valid-connection="isValidConnection"
                    :class="[
                        'transition-all duration-150',
                        isValidSource && '!size-3 !bg-transparent !border-2 !border-primary animate-pulse-grow',
                    ]" />
        </div>
    </UContextMenu>
</template>
