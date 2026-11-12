<script setup lang="ts">
import type { ContextMenuItem } from '@nuxt/ui'
import type { Connection } from '@vue-flow/core'
import { Handle, Position, useNodeConnections, useVueFlow, useNodeId } from '@vue-flow/core'
import type { ComponentRecord } from '~/types/component'

/**
 * A source card on the graph. Collapsed it stands in for its assets (folded
 * cards under it hint at them) and carries their corner badges; expanded it
 * is the header of a container whose assets render as nested canvas nodes.
 */
const props = withDefaults(defineProps<{
    source: ComponentRecord
    /** Source is expanded (its assets render as nested canvas nodes). */
    open?: boolean
    /** Derived node status: the dot, and the border for the states that tint it. */
    status?: NodeStatus
    /** Rendered inside an expanded type group: an inset card rather than a top-level one. */
    nested?: boolean
    /** Folded cards drawn under the collapsed card. */
    layers?: number
}>(), {
    open: false,
    status: undefined,
    nested: false,
    layers: 1,
})

const emit = defineEmits<{
    edit: [sourceId: string]
    delete: [sourceId: string]
}>()

// container = expanded onto the canvas as child nodes (header-only card);
// collapsed = not open.
const container = computed(() => props.open)
const collapsed = computed(() => !props.open)

const isValidConnection = inject<(connection: Connection) => boolean>('isValidConnection')
const graphReadonly = inject<Ref<boolean>>('graphReadonly', ref(false))
const materializingAssetIds = inject<ComputedRef<Set<string>>>('materializingAssetIds')
const nodeId = useNodeId()
const { connectionStartHandle } = useVueFlow()

const sourceConnections = useNodeConnections({ handleType: 'source' })
const targetConnections = useNodeConnections({ handleType: 'target' })
const hasDownstream = computed(() => sourceConnections.value.length > 0)
const hasUpstream = computed(() => targetConnections.value.length > 0)

const isDragging = computed(() => connectionStartHandle.value !== null)

const isValidTarget = computed(() => {
    if (container.value) return false
    const start = connectionStartHandle.value
    if (!start || start.type !== 'source' || !nodeId) return false
    return isValidConnection?.({
        source: start.nodeId,
        target: nodeId,
        sourceHandle: start.id ?? null,
        targetHandle: 'source-target',
    }) ?? false
})

const isValidSource = computed(() => {
    if (container.value) return false
    const start = connectionStartHandle.value
    if (!start || start.type !== 'target' || !nodeId) return false
    return isValidConnection?.({
        source: nodeId,
        target: start.nodeId,
        sourceHandle: 'source-source',
        targetHandle: start.id ?? null,
    }) ?? false
})

const isCompatible = computed(() => isValidTarget.value || isValidSource.value)
const shouldFade = computed(() => !container.value && isDragging.value && !isCompatible.value)

const confirmDeleteSource = useConfirmDeleteSource()
const catalogStore = useCatalogStore()
const appConfig = useAppConfig()
const { getWarnings } = useAssetWarnings()
const { getBadgeForSource } = useDestinationBadge()
const { sourceDrift, statusBadge } = useDrift()

const driftStatus = computed(() => sourceDrift(props.source))
const driftBadge = computed(() => statusBadge(driftStatus.value))
const isDrift = computed(() => driftStatus.value === 'missing' || driftStatus.value === 'partial')

/** Assets with configuration issues; the collapsed card only counts them, each asset lists its own. */
const issueCount = computed(() => props.source.children.filter(a => getWarnings(a.id, a.key).length > 0).length)

const contextMenuItems = computed<ContextMenuItem[][]>(() => [
    [
        {
            label: 'Edit',
            icon: appConfig.icons.edit,
            onSelect: () => emit('edit', props.source.id),
        },
    ],
    [
        {
            label: 'Delete',
            icon: appConfig.icons.delete,
            color: 'error' as const,
            onSelect: async () => {
                const confirmed = await confirmDeleteSource({ id: props.source.id, name: props.source.name ?? props.source.key, icon: icon.value })
                if (confirmed) emit('delete', props.source.id)
            },
        },
    ],
])

const icon = computed(() => componentIcon(props.source.key, appConfig.icons.source))
const state = computed<GraphNodeState>(() => props.status?.state ?? 'idle')

const assetCount = computed(() => props.source.children?.length ?? 0)
const destinationBadge = computed(() => getBadgeForSource(props.source))
const isMaterializing = computed(() =>
    props.source.children?.some(a => materializingAssetIds?.value?.has(a.id)) ?? false,
)

const showChip = computed(() => !!props.source.discriminator && props.source.discriminator !== props.source.name)
const typeName = computed(() => catalogStore.typeName(props.source.key))
// A top-level source leads with its type like a group card does; a nested one sits
// under its group's header, which already names the type, so it leads with its name.
const title = computed(() => props.nested ? props.source.name : typeName.value)
const showName = computed(() => !props.nested && props.source.name?.toLowerCase() !== typeName.value.toLowerCase())
const frameClass = computed(() => [
    collapsed.value && 'graph-raise',
    // With layers folded under it the card is no longer what the stack casts from.
    collapsed.value && props.layers === 0 && 'graph-stack-floor',
    props.nested
        ? ['rounded-xl border bg-(--graph-nested-bg)', statusBorderClass(state.value) || 'border-(--graph-nested-line)']
        : ['graph-card rounded-2xl border bg-(--graph-node-bg)', statusBorderClass(state.value) || 'border-(--graph-card-line)'],
])
</script>

<template>
    <UContextMenu :items="graphReadonly ? [] : contextMenuItems">
        <div class="graph-node relative h-full w-full transition-opacity duration-200"
             :style="shouldFade ? { opacity: GRAPH_DIM_OPACITY } : undefined">
            <Handle id="source-target"
                    type="target"
                    :position="Position.Left"
                    :connectable-start="false"
                    :connectable-end="false"
                    :is-valid-connection="isValidConnection"
                    :class="[
                        'transition-all duration-150',
                        !hasUpstream && !isValidTarget && 'opacity-0',
                        isValidTarget && '!size-3 !bg-transparent !border-2 !border-primary animate-pulse-grow',
                    ]" />

            <GraphCornerBadge v-if="isMaterializing && collapsed"
                              :icon="appConfig.ui.icons.loading"
                              corner="top-left"
                              spin>
                Materializing
            </GraphCornerBadge>

            <!-- Drift outranks warnings: the source or one of its assets no longer resolves against the catalog. -->
            <GraphCornerBadge v-if="isDrift && collapsed"
                              :icon="driftBadge?.icon ?? appConfig.icons.drift"
                              corner="top-right"
                              :tone="driftStatus === 'missing' ? 'error' : 'warning'">
                {{ driftBadge?.label }}
            </GraphCornerBadge>

            <GraphCornerBadge v-if="issueCount > 0 && !isDrift && collapsed"
                              :icon="appConfig.ui.icons.warning"
                              corner="top-right"
                              tone="warning">
                {{ issueCount }} {{ issueCount === 1 ? 'asset' : 'assets' }} with configuration issues
            </GraphCornerBadge>

            <GraphCornerBadge v-if="destinationBadge && collapsed && !nested"
                              :icon="destinationBadge.icon"
                              corner="bottom-right"
                              tone="primary"
                              :count="destinationBadge.count">
                {{ destinationBadge.label }}
            </GraphCornerBadge>

            <div class="relative isolate w-full"
                 :class="collapsed ? (nested ? 'h-16' : 'h-19') : 'h-full'">
                <GraphCardStack v-if="collapsed && layers > 0"
                                :layers="layers"
                                :nested="nested" />
                <div class="relative z-1 flex h-full w-full flex-col"
                     :class="frameClass">
                    <div class="flex shrink-0 items-center gap-3 px-4"
                         :class="nested ? 'h-16' : 'h-19'">
                        <div class="flex shrink-0 items-center justify-center rounded-lg"
                             :class="nested ? 'size-8 bg-(--graph-node-bg)' : 'size-10 bg-elevated'">
                            <UIcon :name="icon"
                                   :class="nested ? 'size-5' : 'size-6'" />
                        </div>
                        <div class="flex min-w-0 flex-1 flex-col items-start gap-1">
                            <div class="flex max-w-full items-center gap-2">
                                <span class="truncate text-sm font-semibold text-highlighted">{{ title }}</span>
                                <StatusDot :color="nodeStateColor(state)"
                                           :pulse="nodeStateExecuting(state)" />
                            </div>
                            <div v-if="showName || showChip"
                                 class="flex max-w-full items-center gap-1.5">
                                <span v-if="showName"
                                      class="min-w-0 truncate text-xs text-muted"
                                      :class="showChip && 'max-w-3/5 shrink-0'">{{ source.name }}</span>
                                <UBadge v-if="showChip"
                                        color="neutral"
                                        size="sm"
                                        class="min-w-0">
                                    <span class="truncate tabular-nums slashed-zero">{{ source.discriminator }}</span>
                                </UBadge>
                            </div>
                        </div>
                        <UTooltip v-if="isDrift && !collapsed"
                                  :text="driftBadge?.label"
                                  :delay-duration="0"
                                  :content="{ side: 'top', sideOffset: 8 }">
                            <UIcon :name="driftBadge?.icon ?? appConfig.icons.drift"
                                   class="size-4 shrink-0"
                                   :class="driftStatus === 'missing' ? 'text-error' : 'text-warning'" />
                        </UTooltip>
                        <UBadge color="neutral"
                                :size="nested ? 'sm' : 'md'"
                                class="shrink-0">
                            <span class="tabular-nums">{{ assetCount }}</span>
                        </UBadge>
                    </div>
                </div>
            </div>

            <Handle id="source-source"
                    type="source"
                    :position="Position.Right"
                    :connectable-start="!container && !graphReadonly"
                    :connectable-end="!container && !graphReadonly"
                    :is-valid-connection="isValidConnection"
                    :class="[
                        'transition-all duration-150',
                        !hasDownstream && !isValidSource && 'opacity-0',
                        isValidSource && '!size-3 !bg-transparent !border-2 !border-primary animate-pulse-grow',
                    ]" />
        </div>
    </UContextMenu>
</template>
