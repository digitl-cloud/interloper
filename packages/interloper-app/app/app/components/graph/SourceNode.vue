<script setup lang="ts">
import type { ContextMenuItem } from '@nuxt/ui'
import type { Connection } from '@vue-flow/core'
import { Handle, Position, useNodeConnections, useVueFlow, useNodeId } from '@vue-flow/core'

const props = withDefaults(defineProps<{
    source: Source
    sourceDefn: SourceDefinition | undefined
    expanded?: boolean
}>(), {
    expanded: false,
})

const emit = defineEmits<{
    edit: [sourceId: string]
    delete: [sourceId: string]
}>()

const isValidConnection = inject<(connection: Connection) => boolean>('isValidConnection')
const graphReadonly = inject<Ref<boolean>>('graphReadonly', ref(false))
const materializingAssetIds = inject<ComputedRef<Set<string>>>('materializingAssetIds')
const nodeId = useNodeId()
const { connectionStartHandle } = useVueFlow()

const sourceConnections = useNodeConnections({ handleType: 'source' })
const targetConnections = useNodeConnections({ handleType: 'target' })
const hasDownstream = computed(() => sourceConnections.value.length > 0)
const hasUpstream = computed(() => targetConnections.value.length > 0)

// Connection drag awareness (collapsed only)
const isDragging = computed(() => connectionStartHandle.value !== null)

const isValidTarget = computed(() => {
    if (props.expanded) return false
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
    if (props.expanded) return false
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
const shouldFade = computed(() => !props.expanded && isDragging.value && !isCompatible.value)

const { confirm } = useConfirm()
const { getWarnings } = useAssetWarnings()
const { getBadgeForSource } = useDestinationBadge()

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

const COLLAPSED_W = 232
const COLLAPSED_H = 76

const assetCount = computed(() => props.source.assets?.length ?? 0)
const destinationBadge = computed(() => getBadgeForSource(props.source))
const isMaterializing = computed(() =>
    props.source.assets?.some(a => materializingAssetIds?.value?.has(a.id)) ?? false,
)
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

            <!-- Materializing spinner -->
            <div v-if="isMaterializing && !props.expanded"
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

            <!-- Warning badge — visible when collapsed and some assets have warnings -->
            <UTooltip v-if="hasWarning && !props.expanded"
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

            <!-- Destination badge — bottom-right corner, visible when collapsed -->
            <div v-if="destinationBadge && !props.expanded"
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

            <!-- Stack layers behind (visible when collapsed) -->
            <div class="absolute rounded-xl border border-default bg-muted transition-all duration-300 ease-out"
                 :class="props.expanded ? 'opacity-0' : 'opacity-100'"
                 :style="{ width: `${COLLAPSED_W - 16}px`, height: `${COLLAPSED_H}px`, left: '8px', top: '16px' }" />
            <div class="absolute rounded-xl border border-default bg-muted transition-all duration-300 ease-out"
                 :class="props.expanded ? 'opacity-0' : 'opacity-100'"
                 :style="{ width: `${COLLAPSED_W - 8}px`, height: `${COLLAPSED_H}px`, left: '4px', top: '8px' }" />

            <!-- Main card -->
            <div class="relative h-full w-full rounded-xl border border-default bg-muted">
                <div class="flex items-center transition-[gap,padding] duration-300"
                     :class="props.expanded ? 'h-12 gap-2 border-b border-default px-4' : 'gap-3 p-5'">
                    <UIcon :name="icon"
                           class="shrink-0 transition-all duration-300"
                           :class="props.expanded ? 'size-5' : 'size-8'" />
                    <div class="min-w-0 flex-1">
                        <div class="truncate font-semibold"
                             :class="props.expanded ? 'text-xs' : 'text-sm'">
                            {{ props.source.name }}
                        </div>
                        <div v-if="!props.expanded && sourceDefn"
                             class="truncate text-xs text-muted">
                            {{ sourceDefn.name }}
                        </div>
                    </div>
                    <UBadge v-if="!props.expanded && assetCount > 0"
                            color="neutral"
                            variant="subtle"
                            size="sm">
                        {{ assetCount }}
                    </UBadge>
                </div>
            </div>

            <Handle id="source-source"
                    type="source"
                    :position="Position.Bottom"
                    :connectable-start="!props.expanded && !graphReadonly"
                    :connectable-end="!props.expanded && !graphReadonly"
                    :is-valid-connection="isValidConnection"
                    :class="[
                        'transition-all duration-150',
                        !hasDownstream && !isValidSource && 'opacity-0',
                        isValidSource && '!size-3 !bg-transparent !border-2 !border-warning animate-pulse-grow',
                    ]" />
        </div>
    </UContextMenu>
</template>
