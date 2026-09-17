<script setup lang="ts">
import { Handle, Position, useNodeConnections } from '@vue-flow/core'

/**
 * Group node for sources sharing a catalog type. Collapsed it stands in for
 * all member sources (edges attach to its group-* handles); expanded it is a
 * card whose members render as nested source nodes stacked vertically. Not
 * connectable and no CRUD — those stay per-source.
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
    /** Folded cards drawn under the collapsed card. */
    layers?: number
}>(), {
    open: false,
    status: undefined,
    layers: 1,
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

const { getWarnings } = useAssetWarnings()
/** Assets with configuration issues across the members; shown while the group hides them. */
const issueCount = computed(() =>
    props.members.reduce((n, m) => n + m.source.children.filter(a => getWarnings(a.id, a.key).length > 0).length, 0),
)
</script>

<template>
    <div class="graph-node relative h-full w-full">
        <Handle id="group-target"
                type="target"
                :position="Position.Left"
                :connectable-start="false"
                :connectable-end="false"
                :class="!hasUpstream && 'opacity-0'" />

        <GraphCornerBadge v-if="issueCount > 0 && !open"
                          icon="i-lucide-triangle-alert"
                          corner="top-right"
                          tone="warning">
            <div class="text-xs">{{ issueCount }} {{ issueCount === 1 ? 'asset' : 'assets' }} with configuration issues</div>
        </GraphCornerBadge>

        <div class="relative isolate h-full w-full">
            <GraphCardStack v-if="!open"
                            :layers="layers" />
            <div class="graph-card relative z-[1] flex h-full w-full flex-col rounded-2xl border border-[var(--graph-card-line)] bg-default"
                 :class="[!open && 'graph-raise', !open && layers === 0 && 'graph-stack-floor']">
                <div class="flex h-[76px] shrink-0 items-center gap-3.5 px-5">
                    <div class="flex size-9 shrink-0 items-center justify-center rounded-[10px] bg-elevated">
                        <UIcon :name="icon"
                               class="size-5" />
                    </div>
                    <div class="min-w-0 flex-1">
                        <div class="flex items-center gap-[7px]">
                            <span class="truncate text-sm font-semibold tracking-tight text-highlighted">{{ label }}</span>
                            <span class="size-[7px] shrink-0 rounded-full"
                                  :class="statusDotClass(status?.state ?? 'idle')" />
                        </div>
                        <div class="mt-0.5 truncate text-[11.5px] text-dimmed">{{ meta }}</div>
                    </div>
                    <GraphCountBadge :count="assetCount"
                                     emphasis />
                </div>
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
