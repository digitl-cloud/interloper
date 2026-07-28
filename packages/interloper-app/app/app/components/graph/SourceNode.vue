<script setup lang="ts">
import type { ContextMenuItem } from '@nuxt/ui'
import type { Connection } from '@vue-flow/core'
import { Handle, Position, useNodeConnections, useVueFlow, useNodeId } from '@vue-flow/core'
import type { ComponentRecord } from '~/types/component'

const props = withDefaults(defineProps<{
    source: ComponentRecord
    sourceDefn: SourceDefinition | undefined
    /** Source is expanded (its assets render as nested canvas nodes). */
    open?: boolean
    /** Derived node status — reflected on the card border. */
    status?: NodeStatus
    /** VueFlow selection state — drives the blue selection ring. */
    selected?: boolean
}>(), {
    open: false,
    status: undefined,
    selected: false,
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

const sourceWarnings = computed(() => {
    const all = props.source.children.flatMap(a => getWarnings(a.id, a.key))
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

const ringClass = computed(() => {
    if (props.selected) return 'ring-2 ring-primary'
    // Expanded, the card is just a frame — its assets carry their own status.
    if (props.open) return ''
    if (props.status) return statusRingClass(props.status.state)
    return ''
})
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

            <!-- Drift badge (collapsed) — takes precedence over warnings: the source
                 or one of its assets no longer resolves against the catalog. -->
            <UTooltip v-if="isDrift && collapsed"
                      :delay-duration="0"
                      :content="{ side: 'top', sideOffset: 6 }"
                      class="absolute -right-2.5 -top-2.5 z-10">
                <div class="flex size-7 items-center justify-center rounded-full border"
                     :class="driftStatus === 'missing' ? 'border-error/40 bg-error/25' : 'border-warning/40 bg-warning/25'">
                    <UIcon :name="driftBadge?.icon ?? 'i-lucide-unplug'"
                           class="size-4 shrink-0"
                           :class="driftStatus === 'missing' ? 'text-error' : 'text-warning'" />
                </div>
                <template #content>
                    <div class="text-xs">{{ driftBadge?.label }}</div>
                </template>
            </UTooltip>

            <!-- Warning badge (collapsed only) -->
            <UTooltip v-if="hasWarning && !isDrift && collapsed"
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

            <!-- Main card: one header row in both states; expanded grows downward
                 (assets render as nested canvas nodes). -->
            <div class="relative flex h-full w-full flex-col overflow-hidden rounded-2xl border border-[var(--ui-border-accented)]"
                 :class="[container ? 'bg-muted' : 'bg-default', ringClass]">
                <div class="flex h-[68px] shrink-0 items-center gap-3 px-4"
                     :class="container && 'border-b border-default bg-default'">
                    <div class="flex size-10 shrink-0 items-center justify-center rounded-xl bg-elevated">
                        <UIcon :name="icon"
                               class="size-5" />
                    </div>
                    <div class="min-w-0 flex-1">
                        <div class="flex items-center gap-2">
                            <span class="truncate text-sm font-semibold text-highlighted">{{ source.name }}</span>
                            <span class="size-2 shrink-0 rounded-full"
                                  :class="statusDotClass(status?.state ?? 'idle')" />
                        </div>
                        <div v-if="sourceDefn"
                             class="truncate text-xs text-muted">
                            {{ sourceDefn.name }}
                        </div>
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
                    <span class="shrink-0 text-sm text-muted">{{ assetCount }}</span>
                    <UIcon :name="collapsed ? 'i-lucide-chevron-right' : 'i-lucide-chevron-down'"
                           class="size-4 shrink-0 text-dimmed" />
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
