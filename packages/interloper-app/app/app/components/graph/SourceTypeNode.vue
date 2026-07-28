<script setup lang="ts">
import { Handle, Position, useNodeConnections } from '@vue-flow/core'

/**
 * Group node for sources sharing a catalog type. Collapsed it stands in for
 * all member sources (edges attach to its group-* handles); expanded it is a
 * dashed container whose members render as nested source nodes stacked
 * vertically. Not connectable and no CRUD — those stay per-source.
 */
const props = withDefaults(defineProps<{
    /** Catalog key shared by the member sources. */
    groupKey: string
    sourceDefn: SourceDefinition | undefined
    members: GraphSourceEntry[]
    /** Group is expanded (members shown as nested nodes). */
    open?: boolean
    /** Aggregated member status — reflected on the card border. */
    status?: NodeStatus
}>(), {
    open: false,
    status: undefined,
})

const sourceConnections = useNodeConnections({ handleType: 'source' })
const targetConnections = useNodeConnections({ handleType: 'target' })
const hasDownstream = computed(() => sourceConnections.value.length > 0)
const hasUpstream = computed(() => targetConnections.value.length > 0)

const icon = computed(() => componentIcon(props.groupKey))
const label = computed(() => props.sourceDefn?.name ?? props.groupKey)
const sourceCount = computed(() => props.members.length)
const assetCount = computed(() => props.members.reduce((sum, m) => sum + (m.source.children?.length ?? 0), 0))
const meta = computed(() =>
    `${sourceCount.value} ${sourceCount.value === 1 ? 'account' : 'accounts'}`
    + ` · ${assetCount.value} ${assetCount.value === 1 ? 'asset' : 'assets'}`,
)

const ringClass = computed(() => {
    // Expanded, the dashed outline is the only frame — members carry their own status.
    if (props.open) return ''
    if (props.status) return statusRingClass(props.status.state)
    return ''
})
</script>

<template>
    <div class="relative h-full w-full">
        <Handle id="group-target"
                type="target"
                :position="Position.Left"
                :connectable-start="false"
                :connectable-end="false"
                :class="!hasUpstream && 'opacity-0'" />

        <!-- Collapsed: solid card standing in for the members.
             Expanded: dashed outline over the canvas, members nest inside. -->
        <div class="relative flex h-full w-full flex-col rounded-2xl"
             :class="[
                 open
                     ? 'border-2 border-dashed border-[var(--ui-text-dimmed)]/50 bg-transparent'
                     : 'overflow-hidden border border-[var(--ui-border-accented)] bg-default',
                 ringClass,
             ]">
            <div class="flex h-[68px] shrink-0 items-center gap-3 px-4">
                <div class="flex size-10 shrink-0 items-center justify-center rounded-xl bg-elevated">
                    <UIcon :name="icon"
                           class="size-5" />
                </div>
                <div class="min-w-0 flex-1">
                    <div class="flex items-center gap-2">
                        <span class="truncate text-sm font-semibold text-highlighted">{{ label }}</span>
                        <span class="size-2 shrink-0 rounded-full"
                              :class="statusDotClass(status?.state ?? 'idle')" />
                    </div>
                    <div class="truncate text-xs text-muted">{{ meta }}</div>
                </div>
                <UIcon :name="open ? 'i-lucide-chevron-down' : 'i-lucide-chevron-right'"
                       class="size-4 shrink-0 text-dimmed" />
            </div>
        </div>

        <Handle id="group-source"
                type="source"
                :position="Position.Right"
                :connectable-start="false"
                :connectable-end="false"
                :class="!hasDownstream && 'opacity-0'" />
    </div>
</template>
