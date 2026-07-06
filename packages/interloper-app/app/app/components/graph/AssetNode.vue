<script setup lang="ts">
import type { ContextMenuItem } from '@nuxt/ui'
import { Handle, Position, useVueFlow, useNodeConnections, useNodeId } from '@vue-flow/core'
import type { Connection } from '@vue-flow/core'
import type { ComponentRecord } from '~/types/component'

const props = defineProps<{
    asset: ComponentRecord
    assetDefn: AssetDefinition | undefined
    /** Derived node status (used by the Status view mode). */
    status?: NodeStatus
    viewMode?: ViewMode
    /** VueFlow selection state — drives the blue selection ring. */
    selected?: boolean
}>()

const emit = defineEmits<{
    view: []
}>()

const componentsStore = useComponentsStore()
const { getWarnings } = useAssetWarnings()
const { getBadgeForAssetId } = useDestinationBadge()
const { statusBadge } = useDrift()
const { confirm } = useConfirm()

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

const icon = computed(() => props.assetDefn ? componentIcon(props.assetDefn.key) : 'i-lucide-box')
const label = computed(() => props.assetDefn?.name ?? props.asset.key)
const description = computed(() => props.assetDefn?.description)
const tags = computed(() => props.assetDefn?.tags ?? [])
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
const hasRequires = computed(() => {
    if (!props.assetDefn?.requires) return false
    return Object.keys(props.assetDefn.requires).length > 0
})
const isRunnable = computed(() => {
    if (!hasRequires.value) return true
    const requiredCount = Object.keys(props.assetDefn?.requires ?? {}).length
    const upstreams = componentsStore.dependencies.filter(d => d.src_id === props.asset.id)
    return upstreams.length >= requiredCount
})

const showTargetHandle = computed(() => hasUpstream.value || !isRunnable.value || isValidTarget.value)
const showSourceHandle = computed(() => hasDownstream.value || isValidSource.value)

const ringClass = computed(() => {
    if (props.selected) return 'ring-2 ring-primary'
    if (isMissing.value) return 'ring-1 ring-[var(--ui-error)]/60'
    if (props.viewMode === 'status' && props.status) return statusRingClass(props.status.state)
    return ''
})

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
                        const deps = componentsStore.dependencies.filter(d => d.src_id === props.asset.id)
                        await Promise.all(deps.map(d => componentsStore.removeRelation(d.src_id, 'dependency', d.dst_id)))
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
        <div class="relative w-[220px] transition-opacity duration-200"
             :class="shouldFade && 'opacity-25'">
            <Handle v-if="showTargetHandle"
                    type="target"
                    :position="Position.Top"
                    :connectable-start="!graphReadonly"
                    :connectable-end="false"
                    :is-valid-connection="isValidConnection"
                    :class="[
                        'transition-all duration-150',
                        isValidTarget && '!size-3 !bg-transparent !border-2 !border-warning animate-pulse-grow',
                        !isRunnable && !isValidTarget && '!size-2.5 !bg-transparent !border-2 !border-warning animate-pulse-grow',
                    ]" />

            <!-- Materializing spinner -->
            <div v-if="isMaterializing"
                 class="absolute -left-3 -top-3 z-10">
                <UTooltip :delay-duration="0"
                          :content="{ side: 'top', sideOffset: 6 }">
                    <div class="flex size-7 items-center justify-center rounded-full border border-muted/50 bg-muted/50">
                        <UIcon name="i-lucide-loader-2"
                               class="size-4 shrink-0 animate-spin text-muted" />
                    </div>
                    <template #content>
                        <div class="text-xs">Materializing</div>
                    </template>
                </UTooltip>
            </div>

            <!-- Drift badge — the asset key no longer resolves against the catalog. -->
            <UTooltip v-if="isMissing"
                      :delay-duration="0"
                      :content="{ side: 'top', sideOffset: 6 }"
                      class="absolute -right-3 -top-3 z-10">
                <div class="flex size-7 items-center justify-center rounded-full border border-error/80 bg-error/20">
                    <UIcon :name="driftBadge?.icon ?? 'i-lucide-unplug'"
                           class="size-4 shrink-0 text-error" />
                </div>
                <template #content>
                    <div class="text-xs">{{ driftBadge?.label }}</div>
                </template>
            </UTooltip>

            <!-- Warning badge -->
            <UTooltip v-if="warnings.length > 0 && !isMissing"
                      :delay-duration="0"
                      :content="{ side: 'top', sideOffset: 6 }"
                      :ui="{ content: 'bg-transparent ring-0 shadow-none p-0 rounded-none' }"
                      class="absolute -right-3 -top-3 z-10">
                <div class="flex size-7 items-center justify-center rounded-full border border-warning/80 bg-warning/20">
                    <UIcon name="i-lucide-triangle-alert"
                           class="size-4 shrink-0 text-warning" />
                </div>
                <template #content>
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
                </template>
            </UTooltip>

            <!-- Destination badge — bottom-right corner -->
            <div v-if="destinationBadge"
                 class="absolute -bottom-3 -right-3 z-10">
                <UTooltip :delay-duration="0"
                          :content="{ side: 'bottom', sideOffset: 6 }">
                    <div class="relative flex size-8 items-center justify-center rounded-full border border-primary/80 bg-primary/20">
                        <UIcon :name="destinationBadge.icon"
                               class="size-4 shrink-0 text-primary" />
                        <span v-if="destinationBadge.isMulti"
                              class="absolute right-0.5 bottom-0.5 flex h-3 min-w-3 items-center justify-center rounded-full bg-default px-1 text-[9px] font-semibold leading-none text-primary">
                            {{ destinationBadge.count }}
                        </span>
                    </div>
                    <template #content>
                        <div class="text-xs">
                            {{ destinationBadge.label }}
                        </div>
                    </template>
                </UTooltip>
            </div>

            <div class="overflow-hidden rounded-xl border border-[var(--ui-border-accented)] bg-muted shadow-[0_10px_30px_-10px_rgba(0,0,0,0.55)]"
                 :class="ringClass">
                <div class="flex items-center gap-2.5 px-3.5 py-3">
                    <div class="flex size-9 shrink-0 items-center justify-center rounded-lg bg-elevated">
                        <UIcon :name="icon"
                               class="size-5 text-muted" />
                    </div>
                    <div class="min-w-0 flex-1">
                        <div class="truncate text-sm font-semibold text-highlighted">{{ label }}</div>
                        <div v-if="tags.length"
                             class="truncate text-xs text-muted">{{ tags.join(' · ') }}</div>
                    </div>
                </div>
                <div v-if="description"
                     class="line-clamp-2 border-t border-[var(--ui-border-accented)] bg-default px-3.5 py-2.5 text-xs leading-snug text-muted">
                    {{ description }}
                </div>
                <div v-else-if="isMissing"
                     class="border-t border-[var(--ui-border-accented)] bg-default px-3.5 py-2.5 text-xs italic leading-snug text-dimmed">
                    No catalog definition
                </div>
            </div>

            <Handle v-if="showSourceHandle"
                    type="source"
                    :position="Position.Bottom"
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
