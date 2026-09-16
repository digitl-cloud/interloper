<script setup lang="ts">
import type { ContextMenuItem } from '@nuxt/ui'
import { Handle, Position, useVueFlow, useNodeConnections, useNodeId } from '@vue-flow/core'
import type { Connection } from '@vue-flow/core'
import type { ComponentRecord } from '~/types/component'
import { requiredUpstreams } from '~/types/catalog'

const props = defineProps<{
    asset: ComponentRecord
    assetDefn: AssetDefinition | undefined
    /** Derived node status — reflected on the card border. */
    status?: NodeStatus
    /** VueFlow selection state — drives the blue selection ring. */
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
const statusLabel = computed(() => props.status?.label ?? null)
const statusTextClass = computed(() => {
    switch (props.status?.state) {
        case 'failed': return 'text-error'
        case 'attention': return 'text-warning'
        case 'running': return 'text-info'
        default: return 'text-dimmed'
    }
})
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

const frameClass = computed(() => ['border-[var(--graph-card-line)]', props.selected && 'outline-2 outline-primary outline-offset-4'])

const contextMenuItems = computed<ContextMenuItem[][]>(() => {
    const items: ContextMenuItem[][] = [
        [
            {
                label: 'View',
                icon: 'i-lucide-eye',
                onSelect: () => emit('view'),
            },
        ],
    ]
    if (hasUpstream.value) {
        items.push([
            {
                label: 'Disconnect upstream',
                icon: 'i-lucide-unplug',
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
                            toast.add(errorToast(e, 'Failed to disconnect upstream'))
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
        <div class="relative w-[256px] transition-opacity duration-200"
             :class="shouldFade && 'opacity-25'">
            <Handle v-if="showTargetHandle"
                    type="target"
                    :position="Position.Left"
                    :connectable-start="!graphReadonly"
                    :connectable-end="false"
                    :is-valid-connection="isValidConnection"
                    :class="[
                        'transition-all duration-150',
                        isValidTarget && '!size-3 !bg-transparent !border-2 !border-warning animate-pulse-grow',
                        !isRunnable && !isValidTarget && '!size-2.5 !bg-transparent !border-2 !border-warning animate-pulse-grow',
                    ]" />

            <GraphCornerBadge v-if="isMaterializing"
                              icon="i-lucide-loader-2"
                              corner="top-left"
                              spin>
                <div class="text-xs">Materializing</div>
            </GraphCornerBadge>

            <!-- Drift: the asset key no longer resolves against the catalog. -->
            <GraphCornerBadge v-if="isMissing"
                              :icon="driftBadge?.icon ?? 'i-lucide-unplug'"
                              corner="top-right"
                              tone="error">
                <div class="text-xs">{{ driftBadge?.label }}</div>
            </GraphCornerBadge>

            <GraphCornerBadge v-if="warnings.length > 0 && !isMissing"
                              icon="i-lucide-triangle-alert"
                              corner="top-right"
                              tone="warning"
                              :tooltip-ui="{ content: 'bg-transparent ring-0 shadow-none p-0 rounded-none' }">
                <div class="rounded-lg border border-default bg-default shadow-lg overflow-hidden">
                    <table class="text-xs w-full">
                        <tbody>
                            <tr v-for="(w, i) in warnings"
                                :key="i"
                                class="border-b border-default last:border-b-0">
                                <td class="px-3 py-2">
                                    <div class="flex items-center gap-2">
                                        <UIcon name="i-lucide-circle-alert"
                                               class="size-3.5 shrink-0 text-warning" />
                                        <span>{{ w.message }}</span>
                                    </div>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </GraphCornerBadge>

            <GraphCornerBadge v-if="destinationBadge"
                              :icon="destinationBadge.icon"
                              corner="bottom-right"
                              tone="primary"
                              :count="destinationBadge.count">
                <div class="text-xs">{{ destinationBadge.label }}</div>
            </GraphCornerBadge>

            <div class="graph-asset overflow-hidden rounded-xl border bg-default"
                 :class="frameClass">
                <div class="flex h-10 items-center gap-[9px] border-b border-[var(--graph-head-line)] bg-elevated px-3.5">
                    <span v-if="standalone"
                          class="w-[15px] shrink-0 text-center font-serif text-[15px] italic leading-none text-[var(--graph-model)]">ƒ</span>
                    <UIcon v-else
                           name="i-lucide-box"
                           class="size-[15px] shrink-0 text-primary" />
                    <span class="min-w-0 truncate font-mono text-[12.5px] font-semibold text-highlighted">{{ label }}</span>
                    <span class="size-[7px] shrink-0 rounded-full"
                          :class="statusDotClass(status?.state ?? 'idle')" />
                </div>
                <div class="flex flex-col gap-[9px] px-3.5 pb-3 pt-2.5">
                    <p class="line-clamp-2 h-[35px] text-xs leading-[1.45] text-muted"
                       :class="isMissing && 'italic text-dimmed'">{{ isMissing ? 'No catalog definition' : description }}</p>
                    <div class="flex min-w-0 items-center gap-2">
                        <UBadge v-for="tag in tags"
                                :key="tag"
                                :color="tagColor(tag)"
                                variant="soft"
                                size="sm"
                                class="shrink-0"
                                :label="tag" />
                        <span v-if="statusLabel"
                              class="min-w-0 truncate text-[10.5px]"
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
                        isValidSource && '!size-3 !bg-transparent !border-2 !border-warning animate-pulse-grow',
                    ]" />
        </div>
    </UContextMenu>
</template>
