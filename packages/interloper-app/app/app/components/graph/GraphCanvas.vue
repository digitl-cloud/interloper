<script setup lang="ts">
import type { ContextMenuItem } from '@nuxt/ui'
import { VueFlow, useVueFlow, Panel } from '@vue-flow/core'
import type { Node, Edge, Connection } from '@vue-flow/core'
import { Background } from '@vue-flow/background'
import { Controls } from '@vue-flow/controls'
import type { ComponentRecord } from '~/types/component'

/**
 * Presentational graph renderer. Consumes a normalised {@link GraphModel}
 * and knows nothing about stores — every surface (collection/job/run) feeds it
 * the same shape. Interactions are emitted; the host wires them.
 */
const props = withDefaults(defineProps<{
    model: GraphModel
    /** Allow dependency editing (drag-connect, edge delete) + source CRUD affordances. */
    editable?: boolean
    expandMode?: ExpandMode
    viewMode?: ViewMode
    loading?: boolean
    /** Connection validator, supplied by an editable host. */
    isValidConnection?: (connection: Connection) => boolean
    /** Asset ids currently materialising (spinner overlay). */
    materializingAssetIds?: Set<string>
    /** Show the in-canvas "New Source" button (off once a toolbar owns it). */
    showNewSourceButton?: boolean
    /** Asset id whose detail panel is open — the only node that gets the selection highlight. */
    selectedId?: string | null
    /** Top-level layout flow: 'TB' (collection default) or 'LR' (run pipeline). */
    direction?: 'TB' | 'LR'
    /** Zoom out to fit the whole graph instead of snapping to 100% after layout. */
    fitToContent?: boolean
    /** Render assets as small status nodes (run graph) instead of full cards. */
    compact?: boolean
}>(), {
    editable: false,
    expandMode: 'nodes',
    viewMode: 'topology',
    loading: false,
    isValidConnection: undefined,
    materializingAssetIds: undefined,
    showNewSourceButton: true,
    selectedId: null,
    direction: 'TB',
    fitToContent: false,
    compact: false,
})

const emit = defineEmits<{
    'add-source': []
    'edit-source': [sourceId: string]
    'asset-click': [asset: ComponentRecord, assetDefn: AssetDefinition | undefined, source: ComponentRecord | null]
    'pane-click': []
    'delete-source': [sourceId: string]
    'connect': [connection: Connection]
    'delete-dependency': [payload: { upstreamAssetId: string; downstreamAssetId: string }]
}>()

const flowId = `asset-graph-${useId()}`
const vueFlow = useVueFlow(flowId)
const { layoutDag } = useGraphLayout()

const sourceEntries = computed(() => props.model.sources)
const assetEntries = computed(() => props.model.assets)
const dependencies = computed(() => props.model.dependencies)

// Track which sources are expanded
const expandedSources = ref(new Set<string>())

function toggleSource(sourceId: string) {
    const next = new Set(expandedSources.value)
    if (next.has(sourceId)) next.delete(sourceId)
    else next.add(sourceId)
    expandedSources.value = next
}

// Layout constants (assets render smaller in compact / run mode)
const ASSET_W = props.compact ? 200 : 220
const ASSET_H = props.compact ? 44 : 160
const SRC_PADDING = 26
const SRC_HEADER_H = 50
const COLLAPSED_W = 244
const COLLAPSED_H = 112

// In-card expand modes (list / graph)
const MINI_W = 108
const MINI_H = 30
const MINI_GAP_X = 22
const MINI_GAP_Y = 22
const MINI_PAD = 16
const LIST_W = 256
const LIST_ROW_H = 34
const LIST_PAD_Y = 8

// Actual measured heights from VueFlow's ResizeObserver
const measuredHeights = ref(new Map<string, number>())

// ── Lookups derived from the model ──
const assetEntryById = computed(() => {
    const map = new Map<string, GraphAssetEntry>()
    for (const entry of assetEntries.value) map.set(entry.asset.id, entry)
    return map
})

const assetToSource = computed(() => {
    const map = new Map<string, string | null>()
    for (const entry of assetEntries.value) map.set(entry.asset.id, entry.source?.id ?? null)
    return map
})

function childEntries(sourceId: string): GraphAssetEntry[] {
    return assetEntries.value.filter(e => e.source?.id === sourceId)
}

const standaloneEntries = computed(() => assetEntries.value.filter(e => e.source === null))

// Clear measured heights for a source's assets when it collapses
watch(expandedSources, (next, prev) => {
    for (const sourceId of prev) {
        if (!next.has(sourceId)) {
            const updated = new Map(measuredHeights.value)
            for (const entry of childEntries(sourceId)) updated.delete(entry.asset.id)
            measuredHeights.value = updated
        }
    }
})

/** Run inner DAG layout for a source's assets. */
function getInnerLayout(sourceId: string) {
    const children = childEntries(sourceId)
    const assetIds = new Set(children.map(c => c.asset.id))
    const intraEdges = dependencies.value
        .filter(d => assetIds.has(d.downstreamAssetId) && assetIds.has(d.upstreamAssetId))
        .map(d => ({ source: d.upstreamAssetId, target: d.downstreamAssetId }))

    const layoutNodes = children.map(c => ({
        id: c.asset.id,
        width: ASSET_W,
        height: measuredHeights.value.get(c.asset.id) ?? ASSET_H,
    }))

    return layoutDag(layoutNodes, intraEdges)
}

/** Whether a source is rendered as expanded child nodes on the canvas. */
function isNodesExpanded(sourceId: string): boolean {
    return props.expandMode === 'nodes' && expandedSources.value.has(sourceId)
}

/** Mini DAG layout for the in-card "graph" expand mode. */
function getMiniGraph(sourceId: string) {
    const children = childEntries(sourceId)
    const ids = new Set(children.map(c => c.asset.id))
    const intra = dependencies.value.filter(d => ids.has(d.upstreamAssetId) && ids.has(d.downstreamAssetId))
    const layout = layoutDag(
        children.map(c => ({ id: c.asset.id, width: MINI_W, height: MINI_H })),
        intra.map(d => ({ source: d.upstreamAssetId, target: d.downstreamAssetId })),
        { gapX: MINI_GAP_X, gapY: MINI_GAP_Y },
    )
    return {
        width: layout.width,
        height: layout.height,
        nodes: children.map(c => ({ entry: c, pos: layout.positions.get(c.asset.id) ?? { x: 0, y: 0 } })),
        edges: intra.map(d => ({ from: d.upstreamAssetId, to: d.downstreamAssetId })),
    }
}

/** Compute each source node's dimensions for the active expand mode. */
function getSourceDimensions(sourceId: string): { width: number; height: number } {
    const open = expandedSources.value.has(sourceId)
    const children = childEntries(sourceId)
    if (!open || children.length === 0) {
        return { width: COLLAPSED_W, height: COLLAPSED_H }
    }

    if (props.expandMode === 'nodes') {
        const dag = getInnerLayout(sourceId)
        return {
            width: dag.width + SRC_PADDING * 2,
            height: dag.height + SRC_HEADER_H + SRC_PADDING * 2,
        }
    }

    if (props.expandMode === 'list') {
        return {
            width: LIST_W,
            height: SRC_HEADER_H + children.length * LIST_ROW_H + LIST_PAD_Y * 2,
        }
    }

    // graph
    const mini = getMiniGraph(sourceId)
    return {
        width: Math.max(COLLAPSED_W, mini.width + MINI_PAD * 2),
        height: SRC_HEADER_H + mini.height + MINI_PAD * 2,
    }
}

/** Cross-group edges at the top level (between sources and standalone assets). */
const topLevelEdges = computed(() => {
    const seen = new Set<string>()
    const result: Array<{ source: string; target: string }> = []

    for (const dep of dependencies.value) {
        const downstreamSourceId = assetToSource.value.get(dep.downstreamAssetId)
        const upstreamSourceId = assetToSource.value.get(dep.upstreamAssetId)
        if (downstreamSourceId === undefined || upstreamSourceId === undefined) continue

        const upstreamNode = upstreamSourceId ?? dep.upstreamAssetId
        const downstreamNode = downstreamSourceId ?? dep.downstreamAssetId
        if (upstreamNode === downstreamNode) continue

        const key = `${upstreamNode}->${downstreamNode}`
        if (seen.has(key)) continue
        seen.add(key)
        result.push({ source: upstreamNode, target: downstreamNode })
    }

    return result
})

/** Layout top-level nodes (sources + standalone assets) using DAG layout. */
const sourceLayout = computed(() => {
    const layoutNodes = [
        ...sourceEntries.value.map((entry) => {
            const dims = getSourceDimensions(entry.source.id)
            return { id: entry.source.id, width: dims.width, height: dims.height }
        }),
        ...standaloneEntries.value.map(entry => ({
            id: entry.asset.id,
            width: ASSET_W,
            height: measuredHeights.value.get(entry.asset.id) ?? ASSET_H,
        })),
    ]

    return layoutDag(layoutNodes, topLevelEdges.value, {
        // gapX = within-layer, gapY = between-layer. For the LR run graph that
        // means a tight vertical stack within a rank, wider spacing between ranks.
        gapX: props.compact ? 22 : 60,
        gapY: props.compact ? 80 : 60,
        direction: props.direction,
    })
})

/** Structural edges (id/source/target/handles); styling + focus applied in `edges`. */
const baseEdges = computed(() => {
    const result: Array<{ id: string; source: string; target: string; sourceHandle?: string; targetHandle?: string }> = []
    const seen = new Set<string>()

    for (const dep of dependencies.value) {
        const upstreamSourceId = assetToSource.value.get(dep.upstreamAssetId)
        const downstreamSourceId = assetToSource.value.get(dep.downstreamAssetId)
        if (upstreamSourceId === undefined || downstreamSourceId === undefined) continue

        const upstreamExpanded = upstreamSourceId === null || isNodesExpanded(upstreamSourceId)
        const downstreamExpanded = downstreamSourceId === null || isNodesExpanded(downstreamSourceId)
        const isSameSource = upstreamSourceId !== null
            && downstreamSourceId !== null
            && upstreamSourceId === downstreamSourceId

        if (isSameSource) {
            if (upstreamExpanded) {
                result.push({
                    id: `asset:${dep.upstreamAssetId}->${dep.downstreamAssetId}`,
                    source: dep.upstreamAssetId,
                    target: dep.downstreamAssetId,
                })
            }
        }
        else {
            const edgeSource = upstreamExpanded ? dep.upstreamAssetId : upstreamSourceId!
            const edgeTarget = downstreamExpanded ? dep.downstreamAssetId : downstreamSourceId!
            const key = `${edgeSource}->${edgeTarget}`
            if (seen.has(key)) continue
            seen.add(key)
            result.push({
                id: key,
                source: edgeSource,
                target: edgeTarget,
                sourceHandle: upstreamExpanded ? undefined : 'source-source',
                targetHandle: downstreamExpanded ? undefined : 'source-target',
            })
        }
    }
    return result
})

// ── Selection focus ──
// Driven solely by the open panel (selectedId), never by VueFlow node
// selection — expanding a source is not a selection. When an asset's panel is
// open its incident edges turn blue and nodes outside its neighbourhood fade;
// default (nothing open) leaves every edge faint and nothing dimmed.
const focus = computed(() => {
    const id = props.selectedId
    if (!id) return null
    const edgeKeys = new Set<string>()
    const nodeIds = new Set<string>([id])
    for (const e of baseEdges.value) {
        if (e.source === id || e.target === id) {
            edgeKeys.add(e.id)
            nodeIds.add(e.source)
            nodeIds.add(e.target)
        }
    }
    // Keep the selected asset's container un-faded — CSS opacity cascades to
    // children, which would otherwise dim the asset itself.
    const parent = assetEntryById.value.get(id)?.source
    if (parent) nodeIds.add(parent.id)
    return { edgeKeys, nodeIds }
})

/** Flat node array: source nodes + asset child nodes + standalone asset nodes. */
const nodes = computed<Node[]>(() => {
    const result: Node[] = []

    for (const entry of sourceEntries.value) {
        const source = entry.source
        const pos = sourceLayout.value.positions.get(source.id) ?? { x: 0, y: 0 }
        const open = expandedSources.value.has(source.id)
        const container = isNodesExpanded(source.id)
        const inCard = open && props.expandMode !== 'nodes'
        const dims = getSourceDimensions(source.id)

        result.push({
            id: source.id,
            type: 'source',
            position: { x: pos.x, y: pos.y },
            width: dims.width,
            height: dims.height,
            connectable: !container,
            data: {
                source,
                sourceDefn: entry.sourceDefn,
                status: entry.status,
                open,
                mode: props.expandMode,
                children: inCard ? childEntries(source.id) : [],
                miniGraph: (open && props.expandMode === 'graph') ? getMiniGraph(source.id) : undefined,
            },
        })

        if (container) {
            const children = childEntries(source.id)
            if (children.length > 0) {
                const innerLayout = getInnerLayout(source.id)
                for (const child of children) {
                    const assetPos = innerLayout.positions.get(child.asset.id) ?? { x: 0, y: 0 }
                    result.push({
                        id: child.asset.id,
                        type: 'asset',
                        parentNode: source.id,
                        extent: 'parent',
                        position: {
                            x: assetPos.x + SRC_PADDING,
                            y: assetPos.y + SRC_HEADER_H + SRC_PADDING,
                        },
                        data: { asset: child.asset, assetDefn: child.assetDefn, source, status: child.status },
                        draggable: true,
                        expandParent: true,
                        connectable: true,
                        zIndex: 1,
                    })
                }
            }
        }
    }

    for (const entry of standaloneEntries.value) {
        const pos = sourceLayout.value.positions.get(entry.asset.id) ?? { x: 0, y: 0 }
        result.push({
            id: entry.asset.id,
            type: 'asset',
            position: { x: pos.x, y: pos.y },
            data: { asset: entry.asset, assetDefn: entry.assetDefn, source: null, status: entry.status },
            draggable: true,
            connectable: true,
        })
    }

    // Selection focus. Set opacity explicitly on EVERY node each pass — only
    // setting it on faded nodes leaves a stale 0.28 that VueFlow never clears,
    // so nodes would stay dimmed after the selection moves.
    const f = focus.value
    for (const node of result) {
        const faded = f ? !f.nodeIds.has(node.id) : false
        node.style = { opacity: faded ? '0.28' : '1', transition: 'opacity 0.2s ease' }
    }

    return result
})

/** Styled edges: faint gray by default; the selection's incident edges go blue, the rest dim. */
const edges = computed<Edge[]>(() => {
    const f = focus.value
    return baseEdges.value.map((e) => {
        const active = f?.edgeKeys.has(e.id) ?? false
        const style = !f
            ? { stroke: 'var(--ui-border-accented)', strokeWidth: 1.5 }
            : active
                ? { stroke: 'var(--ui-primary)', strokeWidth: 2.5 }
                : { stroke: 'var(--ui-border-accented)', strokeWidth: 1.5, opacity: '0.15' }
        return {
            ...e,
            type: 'dependency',
            zIndex: active ? 1003 : 1001,
            style,
        }
    })
})

function onNodeClick({ node }: { node: Node }) {
    if (node.type === 'source') {
        toggleSource(node.id)
    }
    else if (node.type === 'asset') {
        emit('asset-click', node.data.asset, node.data.assetDefn, node.data.source)
    }
}

function onDeleteSource(sourceId: string) {
    if (!props.editable) return
    emit('delete-source', sourceId)
    const next = new Set(expandedSources.value)
    next.delete(sourceId)
    expandedSources.value = next
}

vueFlow.onNodesInitialized(() => {
    const heights = new Map(measuredHeights.value)
    let changed = false
    for (const graphNode of vueFlow.getNodes.value) {
        if (graphNode.type === 'asset' && graphNode.dimensions.height > 0) {
            const prev = heights.get(graphNode.id)
            if (prev !== graphNode.dimensions.height) {
                heights.set(graphNode.id, graphNode.dimensions.height)
                changed = true
            }
        }
    }
    if (changed) {
        measuredHeights.value = heights
    }

    vueFlow.fitView({ padding: 0.25 })
    if (!props.fitToContent) vueFlow.zoomTo(1)
})

// ── Connection plumbing provided to child node components ──
const validateConnection = (connection: Connection) => props.isValidConnection?.(connection) ?? false
provide('isValidConnection', validateConnection)
provide('graphReadonly', toRef(() => !props.editable))

const materializing = computed(() => props.materializingAssetIds ?? new Set<string>())
provide('materializingAssetIds', materializing)

function onConnect(connection: Connection) {
    if (!props.editable) return
    emit('connect', connection)
}

// ── Edge context menu ──
const edgeMenuOpen = ref(false)
const edgeMenuVirtual = ref({ getBoundingClientRect: () => new DOMRect() })
const edgeMenuEdge = ref<{ source: string; target: string } | null>(null)

const { confirm } = useConfirm()

const edgeMenuItems = computed<ContextMenuItem[][]>(() => {
    const edge = edgeMenuEdge.value
    if (!edge) return []
    return [
        [
            {
                label: 'Delete dependency',
                icon: 'i-lucide-trash',
                color: 'error' as const,
                onSelect: async () => {
                    const confirmed = await confirm({
                        title: 'Delete dependency',
                        description: 'This will remove the dependency between these assets. This action cannot be undone.',
                    })
                    if (confirmed) {
                        emit('delete-dependency', {
                            downstreamAssetId: edge.target,
                            upstreamAssetId: edge.source,
                        })
                    }
                },
            },
        ],
    ]
})

// ── Pane context menu ──
const paneMenuOpen = ref(false)
const paneMenuVirtual = ref({ getBoundingClientRect: () => new DOMRect() })

const paneMenuItems = computed<ContextMenuItem[][]>(() => [
    [
        {
            label: 'Expand all',
            icon: 'i-lucide-maximize-2',
            onSelect: () => {
                expandedSources.value = new Set(sourceEntries.value.map(e => e.source.id))
            },
        },
        {
            label: 'Collapse all',
            icon: 'i-lucide-minimize-2',
            onSelect: () => {
                expandedSources.value = new Set()
            },
        },
        {
            label: 'Fit view',
            icon: 'i-lucide-scan',
            onSelect: () => {
                vueFlow.fitView({ padding: 0.25 })
            },
        },
    ],
])

function onPaneContextMenu(event: MouseEvent) {
    event.preventDefault()
    const { clientX: x, clientY: y } = event
    paneMenuVirtual.value = {
        getBoundingClientRect: () => new DOMRect(x, y, 0, 0),
    }
    paneMenuOpen.value = true
}

function onEdgeContextMenu({ edge, event }: { edge: Edge; event: MouseEvent | TouchEvent }) {
    event.preventDefault()
    event.stopPropagation()

    if (!props.editable) return
    if (!('clientX' in event)) return

    // Only allow delete on asset-to-asset edges (not source-level collapsed edges)
    const isSourceAsset = assetToSource.value.has(edge.source)
    const isTargetAsset = assetToSource.value.has(edge.target)
    if (!isSourceAsset || !isTargetAsset) return

    edgeMenuEdge.value = { source: edge.source, target: edge.target }

    const { clientX: x, clientY: y } = event
    edgeMenuVirtual.value = {
        getBoundingClientRect: () => new DOMRect(x, y, 0, 0),
    }
    edgeMenuOpen.value = true
}
</script>

<template>
    <div class="relative flex-1 min-h-0 w-full">
        <VueFlow :id="flowId"
                 :nodes="nodes"
                 :edges="edges"
                 fit-view
                 class="!absolute inset-0"
                 :max-zoom="1"
                 :min-zoom="0.6"
                 snap-to-grid
                 :nodes-draggable="true"
                 :select-nodes-on-drag="false"
                 :connection-radius="80"
                 :auto-connect="false"
                 @node-click="onNodeClick"
                 @connect="onConnect"
                 @edge-context-menu="onEdgeContextMenu"
                 @pane-click="emit('pane-click')"
                 @pane-context-menu="onPaneContextMenu">
            <template #node-source="{ data }">
                <GraphSourceNode :source="data.source"
                                 :source-defn="data.sourceDefn"
                                 :status="data.status"
                                 :view-mode="viewMode"
                                 :open="data.open"
                                 :mode="data.mode"
                                 :children="data.children"
                                 :mini-graph="data.miniGraph"
                                 :selected="false"
                                 @edit="emit('edit-source', $event)"
                                 @delete="onDeleteSource"
                                 @asset-click="(a, d, s) => emit('asset-click', a, d, s)" />
            </template>
            <template #node-asset="{ data }">
                <GraphRunNode v-if="compact"
                              :asset="data.asset"
                              :asset-defn="data.assetDefn"
                              :status="data.status"
                              :view-mode="viewMode"
                              :selected="data.asset.id === selectedId"
                              @view="emit('asset-click', data.asset, data.assetDefn, data.source)" />
                <GraphAssetNode v-else
                                :asset="data.asset"
                                :asset-defn="data.assetDefn"
                                :status="data.status"
                                :view-mode="viewMode"
                                :selected="data.asset.id === selectedId"
                                @view="emit('asset-click', data.asset, data.assetDefn, data.source)" />
            </template>
            <template #edge-dependency="edgeProps">
                <GraphDependencyEdge v-bind="edgeProps" />
            </template>
            <Background :size=".8" />
            <Controls position="bottom-left"
                      :show-interactive="false" />

            <Panel v-if="editable && sourceEntries.length === 0"
                   position="top-left"
                   class="!inset-0 !m-0 flex items-center justify-center pointer-events-none">
                <div class="pointer-events-auto w-[430px] max-w-[92%] bg-default border border-default rounded-[20px] shadow-2xl px-8 py-9 text-center">
                    <div class="size-14 mx-auto rounded-[16px] bg-primary/10 text-primary flex items-center justify-center">
                        <UIcon name="i-lucide-workflow"
                               class="size-7" />
                    </div>
                    <div class="eyebrow text-primary mt-4">
                        Asset graph
                    </div>
                    <h2 class="text-[22px] font-bold tracking-[-0.02em] text-highlighted mt-2">
                        Your data, wired together
                    </h2>
                    <p class="text-[15px] text-muted leading-relaxed mt-2.5">
                        The graph is a live map of every source, the assets they produce and
                        the dependencies between them — Interloper wires it automatically as
                        you build. Add your first source to watch it take shape.
                    </p>
                    <UButton icon="i-lucide-plus"
                             label="Add your first source"
                             class="mt-6"
                             @click="emit('add-source')" />
                </div>
            </Panel>
            <Panel v-else-if="editable && showNewSourceButton"
                   position="top-center"
                   class="!m-4">
                <UButton icon="i-lucide-plus"
                         label="New Source"
                         @click="emit('add-source')" />
            </Panel>
        </VueFlow>

        <div v-if="loading"
             class="absolute inset-0 z-50 flex items-center justify-center bg-default/50">
            <UIcon name="i-lucide-loader-circle"
                   class="size-8 text-muted animate-spin" />
        </div>

        <UDropdownMenu v-model:open="edgeMenuOpen"
                       :items="edgeMenuItems"
                       :modal="false"
                       :content="{ reference: edgeMenuVirtual, side: 'bottom', align: 'start', sideOffset: 4 }">
            <div class="hidden" />
        </UDropdownMenu>

        <UDropdownMenu v-model:open="paneMenuOpen"
                       :items="paneMenuItems"
                       :modal="false"
                       :content="{ reference: paneMenuVirtual, side: 'bottom', align: 'start', sideOffset: 4 }">
            <div class="hidden" />
        </UDropdownMenu>
    </div>
</template>
