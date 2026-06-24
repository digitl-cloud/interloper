<script setup lang="ts">
import type { ContextMenuItem } from '@nuxt/ui'
import type { Connection } from '@vue-flow/core'
import { Handle, Position, useNodeConnections, useVueFlow, useNodeId } from '@vue-flow/core'

interface MiniGraph {
    width: number
    height: number
    nodes: Array<{ entry: GraphAssetEntry; pos: { x: number; y: number } }>
    edges: Array<{ from: string; to: string }>
}

const props = withDefaults(defineProps<{
    source: Source
    sourceDefn: SourceDefinition | undefined
    /** Source is expanded (showing its assets), in any expand mode. */
    open?: boolean
    /** Active expand mode. */
    mode?: ExpandMode
    /** Child asset entries — for the in-card list/graph modes. */
    children?: GraphAssetEntry[]
    /** Pre-laid-out mini graph — for the in-card graph mode. */
    miniGraph?: MiniGraph
    /** Derived node status (used by the Status view mode; see Phase 3). */
    status?: NodeStatus
    viewMode?: ViewMode
}>(), {
    open: false,
    mode: 'nodes',
    children: () => [],
    miniGraph: undefined,
    status: undefined,
    viewMode: 'topology',
})

const emit = defineEmits<{
    edit: [sourceId: string]
    delete: [sourceId: string]
    'asset-click': [asset: SourceAsset | Asset, assetDefn: AssetDefinition | undefined, source: Source | null]
}>()

// container = expanded onto the canvas as child nodes (header-only card);
// inCard   = expanded inside the card (list / graph);
// collapsed = not open.
const container = computed(() => props.open && props.mode === 'nodes')
const inCard = computed(() => props.open && props.mode !== 'nodes')
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

const { confirm } = useConfirm()
const { getWarnings } = useAssetWarnings()
const { getBadgeForSource } = useDestinationBadge()
const { getSourceSchedule } = useSchedule()

const sourceWarnings = computed(() => {
    const all = props.source.assets.flatMap(a => getWarnings(a.id, a.key))
    const seen = new Set<string>()
    return all.filter((w) => {
        if (seen.has(w.message)) return false
        seen.add(w.message)
        return true
    })
})
const hasWarning = computed(() => sourceWarnings.value.length > 0)

const contextMenuItems = computed<ContextMenuItem[][]>(() => [
    [
        {
            label: 'Edit',
            icon: 'i-lucide-pencil',
            onSelect: () => emit('edit', props.source.id),
        },
    ],
    [
        {
            label: 'Delete',
            icon: 'i-lucide-trash',
            color: 'error' as const,
            onSelect: async () => {
                const confirmed = await confirm({
                    title: 'Delete source',
                    description: `This will permanently delete "${props.source.name}" and all its assets. This action cannot be undone.`,
                })
                if (confirmed) emit('delete', props.source.id)
            },
        },
    ],
])

const icon = computed(() => componentIcon(props.source.key))

const assetCount = computed(() => props.source.assets?.length ?? 0)
const destinationBadge = computed(() => getBadgeForSource(props.source))
const isMaterializing = computed(() =>
    props.source.assets?.some(a => materializingAssetIds?.value?.has(a.id)) ?? false,
)

const schedule = computed(() => getSourceSchedule(props.source))

/** Collapsed meta suffix: "Paused" when scheduled-but-disabled, else the schedule label. */
const metaSuffix = computed(() => {
    const s = schedule.value
    if (!s) return null
    return s.paused ? 'Paused' : s.label
})

function onAssetSelect(entry: GraphAssetEntry) {
    emit('asset-click', entry.asset, entry.assetDefn, entry.source)
}
</script>

<template>
    <UContextMenu :items="graphReadonly ? [] : contextMenuItems">
        <div class="relative h-full w-full transition-opacity duration-200"
             :class="shouldFade && 'opacity-25'">
            <Handle id="source-target"
                    type="target"
                    :position="Position.Top"
                    :connectable-start="false"
                    :connectable-end="false"
                    :is-valid-connection="isValidConnection"
                    :class="[
                        'transition-all duration-150',
                        !hasUpstream && !isValidTarget && 'opacity-0',
                        isValidTarget && '!size-3 !bg-transparent !border-2 !border-warning animate-pulse-grow',
                    ]" />

            <!-- Materializing spinner (collapsed only) -->
            <div v-if="isMaterializing && collapsed"
                 class="absolute -left-2.5 -top-2.5 z-10">
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

            <!-- Warning badge (collapsed only) -->
            <UTooltip v-if="hasWarning && collapsed"
                      :delay-duration="0"
                      :content="{ side: 'top', sideOffset: 6 }"
                      :ui="{ content: 'bg-transparent ring-0 shadow-none p-0 rounded-none' }"
                      class="absolute -right-2.5 -top-2.5 z-10">
                <div class="flex size-7 items-center justify-center rounded-full border border-warning/40 bg-warning/25">
                    <UIcon name="i-lucide-triangle-alert"
                           class="size-4 shrink-0 text-warning" />
                </div>
                <template #content>
                    <div class="rounded-lg border border-default bg-default shadow-lg overflow-hidden">
                        <table class="text-xs w-full">
                            <tbody>
                                <tr v-for="(w, i) in sourceWarnings"
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

            <!-- Destination badge (collapsed only) -->
            <div v-if="destinationBadge && collapsed"
                 class="absolute -bottom-3 -right-3 z-10">
                <UTooltip :delay-duration="0"
                          :content="{ side: 'bottom', sideOffset: 6 }">
                    <div class="relative flex size-8 items-center justify-center rounded-full border border-primary/80 bg-primary/20">
                        <UIcon :name="destinationBadge.icon"
                               class="size-4 shrink-0 text-primary" />
                        <span v-if="destinationBadge.isMulti"
                              class="absolute right-0.5 bottom-0.5 flex h-3.5 min-w-3.5 items-center justify-center rounded-full border border-primary/60 bg-default px-1 text-[9px] font-semibold leading-none text-primary">
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

            <!-- Main card -->
            <div class="relative flex h-full w-full flex-col overflow-hidden rounded-xl border border-default bg-muted">
                <!-- Collapsed: full card with meta line -->
                <div v-if="collapsed"
                     class="flex h-full items-center gap-3 p-4">
                    <UIcon :name="icon"
                           class="size-8 shrink-0" />
                    <div class="min-w-0 flex-1">
                        <div class="truncate text-sm font-semibold">{{ source.name }}</div>
                        <div v-if="sourceDefn"
                             class="truncate text-xs text-muted">
                            {{ sourceDefn.name }}
                        </div>
                        <div class="mt-1 flex items-center gap-1.5 overflow-hidden whitespace-nowrap text-xs text-dimmed">
                            <UIcon name="i-lucide-box"
                                   class="size-3 shrink-0" />
                            <span>{{ assetCount }} {{ assetCount === 1 ? 'asset' : 'assets' }}</span>
                            <template v-if="metaSuffix">
                                <span>·</span>
                                <span :class="schedule?.paused ? 'text-warning' : ''"
                                      class="truncate">{{ metaSuffix }}</span>
                            </template>
                        </div>
                    </div>
                    <UIcon name="i-lucide-chevron-right"
                           class="size-4 shrink-0 text-dimmed" />
                </div>

                <!-- Expanded: compact header + optional in-card body -->
                <template v-else>
                    <div class="flex h-12 shrink-0 items-center gap-2 border-b border-default px-4">
                        <UIcon :name="icon"
                               class="size-5 shrink-0" />
                        <span class="min-w-0 flex-1 truncate text-xs font-semibold">{{ source.name }}</span>
                        <span class="shrink-0 text-[11px] text-dimmed">{{ assetCount }} assets</span>
                        <UIcon name="i-lucide-chevron-down"
                               class="size-4 shrink-0 text-dimmed" />
                    </div>

                    <GraphSourceAssetList v-if="inCard && mode === 'list'"
                                          class="flex-1 min-h-0 overflow-auto"
                                          :assets="children"
                                          @select="onAssetSelect" />

                    <div v-else-if="inCard && mode === 'graph' && miniGraph"
                         class="flex flex-1 min-h-0 items-center justify-center p-4">
                        <GraphSourceMiniGraph :mini-graph="miniGraph"
                                              @select="onAssetSelect" />
                    </div>
                </template>
            </div>

            <Handle id="source-source"
                    type="source"
                    :position="Position.Bottom"
                    :connectable-start="!container && !graphReadonly"
                    :connectable-end="!container && !graphReadonly"
                    :is-valid-connection="isValidConnection"
                    :class="[
                        'transition-all duration-150',
                        !hasDownstream && !isValidSource && 'opacity-0',
                        isValidSource && '!size-3 !bg-transparent !border-2 !border-warning animate-pulse-grow',
                    ]" />
        </div>
    </UContextMenu>
</template>
