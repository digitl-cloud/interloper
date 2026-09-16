<script setup lang="ts">
import type { ContextMenuItem } from '@nuxt/ui'
import type { Connection } from '@vue-flow/core'
import { Handle, Position, useNodeConnections, useVueFlow, useNodeId } from '@vue-flow/core'
import type { ComponentRecord } from '~/types/component'

const props = withDefaults(defineProps<{
    source: ComponentRecord
    /** Source is expanded (its assets render as nested canvas nodes). */
    open?: boolean
    /** Derived node status — reflected on the card border. */
    status?: NodeStatus
    /** VueFlow selection state — drives the blue selection ring. */
    selected?: boolean
    /** Rendered inside an expanded type group: an inset card rather than a top-level one. */
    nested?: boolean
    /** Folded cards drawn under the collapsed card. */
    layers?: number
}>(), {
    open: false,
    status: undefined,
    selected: false,
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

const { confirm } = useConfirm()
const componentsStore = useComponentsStore()
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
                const { blocking, detaching } = componentsStore.deleteImpact(props.source.id)
                const confirmed = await confirm({
                    title: 'Delete source',
                    description: 'This will permanently delete {subject} and all its assets. This action cannot be undone.',
                    subject: { name: props.source.name ?? props.source.key, icon: componentIcon(props.source.key) },
                    blocking,
                    detaching,
                })
                if (confirmed) emit('delete', props.source.id)
            },
        },
    ],
])

const icon = computed(() => componentIcon(props.source.key))

const assetCount = computed(() => props.source.children?.length ?? 0)
const destinationBadge = computed(() => getBadgeForSource(props.source))
const isMaterializing = computed(() =>
    props.source.children?.some(a => materializingAssetIds?.value?.has(a.id)) ?? false,
)

const headHeight = computed(() => props.nested ? 64 : 84)
const showChip = computed(() => !!props.source.discriminator && props.source.discriminator !== props.source.name)
const frameClass = computed(() => props.nested
    ? 'rounded-[14px] border border-[var(--graph-nested-line)] bg-[var(--graph-nested-bg)]'
    : ['graph-card rounded-2xl border border-[var(--graph-card-line)] bg-default', props.selected && 'outline-2 outline-primary outline-offset-4'])
</script>

<template>
    <UContextMenu :items="graphReadonly ? [] : contextMenuItems">
        <div class="relative h-full w-full transition-opacity duration-200"
             :class="shouldFade && 'opacity-25'">
            <Handle id="source-target"
                    type="target"
                    :position="Position.Left"
                    :connectable-start="false"
                    :connectable-end="false"
                    :is-valid-connection="isValidConnection"
                    :class="[
                        'transition-all duration-150',
                        !hasUpstream && !isValidTarget && 'opacity-0',
                        isValidTarget && '!size-3 !bg-transparent !border-2 !border-warning animate-pulse-grow',
                    ]" />

            <GraphCornerBadge v-if="isMaterializing && collapsed"
                              icon="i-lucide-loader-2"
                              corner="top-left"
                              spin>
                <div class="text-xs">Materializing</div>
            </GraphCornerBadge>

            <!-- Drift outranks warnings: the source or one of its assets no longer resolves against the catalog. -->
            <GraphCornerBadge v-if="isDrift && collapsed"
                              :icon="driftBadge?.icon ?? 'i-lucide-unplug'"
                              corner="top-right"
                              :tone="driftStatus === 'missing' ? 'error' : 'warning'">
                <div class="text-xs">{{ driftBadge?.label }}</div>
            </GraphCornerBadge>

            <GraphCornerBadge v-if="issueCount > 0 && !isDrift && collapsed"
                              icon="i-lucide-triangle-alert"
                              corner="top-right"
                              tone="warning">
                <div class="text-xs">{{ issueCount }} {{ issueCount === 1 ? 'asset' : 'assets' }} with configuration issues</div>
            </GraphCornerBadge>

            <GraphCornerBadge v-if="destinationBadge && collapsed && !nested"
                              :icon="destinationBadge.icon"
                              corner="bottom-right"
                              tone="primary"
                              :count="destinationBadge.count">
                <div class="text-xs">{{ destinationBadge.label }}</div>
            </GraphCornerBadge>

            <div class="relative isolate w-full"
                 :style="{ height: collapsed ? `${headHeight}px` : '100%' }">
                <GraphCardStack v-if="collapsed && layers > 0"
                                :layers="layers"
                                :nested="nested" />
                <div class="relative z-[1] flex h-full w-full flex-col"
                     :class="frameClass">
                    <div class="flex shrink-0 items-center"
                         :class="nested ? 'h-16 gap-2.5 px-5' : 'h-[84px] gap-3.5 px-5'">
                        <div class="flex shrink-0 items-center justify-center"
                             :class="nested ? 'size-7 rounded-lg bg-default' : 'size-9 rounded-[10px] bg-elevated'">
                            <UIcon :name="icon"
                                   :class="nested ? 'size-4' : 'size-5'" />
                        </div>
                        <div class="flex min-w-0 flex-1 flex-col items-start"
                             :class="nested ? 'gap-[5px]' : 'gap-[7px]'">
                            <div class="flex max-w-full items-center gap-[7px]">
                                <span class="truncate font-semibold tracking-tight text-highlighted"
                                      :class="nested ? 'text-[13px]' : 'text-sm'">{{ source.name }}</span>
                                <span class="shrink-0 rounded-full"
                                      :class="[nested ? 'size-1.5' : 'size-[7px]', statusDotClass(status?.state ?? 'idle')]" />
                            </div>
                            <span v-if="showChip"
                                  class="id-chip max-w-full truncate">{{ source.discriminator }}</span>
                        </div>
                        <UTooltip v-if="isDrift && !collapsed"
                                  :delay-duration="0"
                                  :content="{ side: 'top', sideOffset: 6 }">
                            <UIcon :name="driftBadge?.icon ?? 'i-lucide-unplug'"
                                   class="size-4 shrink-0"
                                   :class="driftStatus === 'missing' ? 'text-error' : 'text-warning'" />
                            <template #content>
                                <div class="text-xs">{{ driftBadge?.label }}</div>
                            </template>
                        </UTooltip>
                        <GraphCountBadge :count="assetCount"
                                         :emphasis="!nested" />
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
                        isValidSource && '!size-3 !bg-transparent !border-2 !border-warning animate-pulse-grow',
                    ]" />
        </div>
    </UContextMenu>
</template>
