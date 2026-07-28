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
    /** Group sources by catalog type — only types with ≥2 instances form a group. */
    groupBy?: GroupBy
    loading?: boolean
    /** Connection validator, supplied by an editable host. */
    isValidConnection?: (connection: Connection) => boolean
    /** Asset ids currently materialising (spinner overlay). */
    materializingAssetIds?: Set<string>
    /** Show the in-canvas "New Source" button (off once a toolbar owns it). */
    showNewSourceButton?: boolean
    /** Asset id whose detail panel is open — the only node that gets the selection highlight. */
    selectedId?: string | null
    /** Top-level layout flow (both the collection and run graphs read left-to-right). */
    direction?: 'TB' | 'LR'
    /** Zoom out to fit the whole graph instead of snapping to 100% after layout. */
    fitToContent?: boolean
    /** Render assets as small status nodes (run graph) instead of full cards. */
    compact?: boolean
}>(), {
    editable: false,
    groupBy: 'none',
    loading: false,
    isValidConnection: undefined,
    materializingAssetIds: undefined,
    showNewSourceButton: true,
    selectedId: null,
    direction: 'LR',
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

// Track which sources / type groups are expanded
const expandedSources = ref(new Set<string>())
const expandedGroups = ref(new Set<string>())

function toggleSource(sourceId: string) {
    const next = new Set(expandedSources.value)
    if (next.has(sourceId)) next.delete(sourceId)
    else next.add(sourceId)
    expandedSources.value = next
}

function toggleGroup(groupId: string) {
    const next = new Set(expandedGroups.value)
    if (next.has(groupId)) next.delete(groupId)
    else next.add(groupId)
    expandedGroups.value = next
}

// Layout constants (assets render smaller in compact / run mode)
const ASSET_W = props.compact ? 200 : 240
const ASSET_H = props.compact ? 44 : 64
const STACK_GAP = 14
/** Horizontal gap between dependency ranks inside an expanded container. */
const INNER_GAP_RANK = 48
const SRC_PADDING = 16
const GRP_PADDING = 24
const SRC_HEADER_H = 68
const SRC_BODY_TOP = 12
/** Container width: one asset column plus side padding; collapsed cards match. */
const SRC_W = ASSET_W + SRC_PADDING * 2
const COLLAPSED_W = SRC_W
const COLLAPSED_H = 76

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

// ── Source-type groups ──
// A group only forms for catalog keys with ≥2 instances; singleton types keep
// rendering their source at top level, so grouping is a no-op until duplicates exist.
const groups = computed(() => {
    const map = new Map<string, GraphSourceEntry[]>()
    if (props.groupBy !== 'type') return map
    const byKey = new Map<string, GraphSourceEntry[]>()
    for (const entry of sourceEntries.value) {
        const list = byKey.get(entry.source.key)
        if (list) list.push(entry)
        else byKey.set(entry.source.key, [entry])
    }
    for (const [key, entries] of byKey) {
        if (entries.length > 1) map.set(`type:${key}`, entries)
    }
    return map
})

const groupOfSource = computed(() => {
    const map = new Map<string, string>()
    for (const [groupId, entries] of groups.value) {
        for (const entry of entries) map.set(entry.source.id, groupId)
    }
    return map
})

function isGroupExpanded(groupId: string): boolean {
    return expandedGroups.value.has(groupId)
}

/** Aggregate member statuses for the group card: attention > paused > idle. */
function groupStatus(members: GraphSourceEntry[]): NodeStatus {
    if (members.some(m => m.status?.state === 'attention')) return { state: 'attention' }
    if (members.some(m => m.status?.state === 'paused')) return { state: 'paused' }
    return { state: 'idle' }
}

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

// Same when a group collapses: its member sources' assets leave the canvas
// (members keep their expandedSources entries and re-measure on re-expand).
watch(expandedGroups, (next, prev) => {
    const updated = new Map(measuredHeights.value)
    let changed = false
    for (const groupId of prev) {
        if (next.has(groupId)) continue
        for (const member of groups.value.get(groupId) ?? []) {
            for (const entry of childEntries(member.source.id)) {
                if (updated.delete(entry.asset.id)) changed = true
            }
        }
    }
    if (changed) measuredHeights.value = updated
})

/**
 * Inner DAG layout for a source's assets, flowing left-to-right like the top
 * level. Independent assets share one rank and stack vertically (the mockup
 * case); dependent assets spread into columns with their edges drawn.
 */
function getInnerLayout(sourceId: string) {
    // Alphabetical input order = alphabetical tie-break within a rank.
    const children = [...childEntries(sourceId)].sort((a, b) => a.asset.key.localeCompare(b.asset.key))
    const assetIds = new Set(children.map(c => c.asset.id))
    const intraEdges = dependencies.value
        .filter(d => assetIds.has(d.downstreamAssetId) && assetIds.has(d.upstreamAssetId))
        .map(d => ({ source: d.upstreamAssetId, target: d.downstreamAssetId }))

    const layoutNodes = children.map(c => ({
        id: c.asset.id,
        width: ASSET_W,
        height: measuredHeights.value.get(c.asset.id) ?? ASSET_H,
    }))

    return layoutDag(layoutNodes, intraEdges, { direction: 'LR', gapX: STACK_GAP, gapY: INNER_GAP_RANK })
}

/** Whether a source is rendered as expanded child nodes on the canvas. */
function isNodesExpanded(sourceId: string): boolean {
    return expandedSources.value.has(sourceId)
}

/** Compute each source node's dimensions (collapsed card, or container around its asset DAG). */
function getSourceDimensions(sourceId: string): { width: number; height: number } {
    const open = expandedSources.value.has(sourceId)
    const children = childEntries(sourceId)
    if (!open || children.length === 0) {
        return { width: COLLAPSED_W, height: COLLAPSED_H }
    }

    const dag = getInnerLayout(sourceId)
    return {
        width: Math.max(SRC_W, dag.width + SRC_PADDING * 2),
        height: SRC_HEADER_H + SRC_BODY_TOP + dag.height + SRC_PADDING,
    }
}

/** Inner LR DAG layout for a group's member sources (edges projected to source level). */
function getGroupInnerLayout(groupId: string) {
    const members = [...(groups.value.get(groupId) ?? [])]
        .sort((a, b) => (a.source.name ?? a.source.key).localeCompare(b.source.name ?? b.source.key))
    const memberIds = new Set(members.map(m => m.source.id))
    const seen = new Set<string>()
    const intraEdges: Array<{ source: string; target: string }> = []
    for (const dep of dependencies.value) {
        const upstream = assetToSource.value.get(dep.upstreamAssetId)
        const downstream = assetToSource.value.get(dep.downstreamAssetId)
        if (!upstream || !downstream || upstream === downstream) continue
        if (!memberIds.has(upstream) || !memberIds.has(downstream)) continue
        const key = `${upstream}->${downstream}`
        if (seen.has(key)) continue
        seen.add(key)
        intraEdges.push({ source: upstream, target: downstream })
    }

    const layoutNodes = members.map((m) => {
        const dims = getSourceDimensions(m.source.id)
        return { id: m.source.id, width: dims.width, height: dims.height }
    })
    return layoutDag(layoutNodes, intraEdges, { direction: 'LR', gapX: STACK_GAP, gapY: INNER_GAP_RANK })
}

/** Group node dimensions — member dims already reflect their own expand state. */
function getGroupDimensions(groupId: string): { width: number; height: number } {
    if (!isGroupExpanded(groupId)) return { width: COLLAPSED_W, height: COLLAPSED_H }
    const dag = getGroupInnerLayout(groupId)
    return {
        width: Math.max(dag.width, SRC_W) + GRP_PADDING * 2,
        height: SRC_HEADER_H + SRC_BODY_TOP + dag.height + GRP_PADDING,
    }
}

// ── Endpoint projection ──
// An asset's ancestor chain is asset → owning source → type group. Edges attach
// to the outermost *collapsed* ancestor; layout ranks nodes by their top-level
// container regardless of expansion (expanded containers are still top-level nodes).

/** Outermost collapsed ancestor of an asset — the node its edges attach to. Null for unknown assets. */
function representativeOf(assetId: string): string | null {
    const sourceId = assetToSource.value.get(assetId)
    if (sourceId === undefined) return null
    if (sourceId === null) return assetId
    const groupId = groupOfSource.value.get(sourceId)
    if (groupId && !isGroupExpanded(groupId)) return groupId
    if (!isNodesExpanded(sourceId)) return sourceId
    return assetId
}

/** Top-level container of an asset, for the root layout (group > source > the asset itself). */
function topLevelOf(assetId: string): string | null {
    const sourceId = assetToSource.value.get(assetId)
    if (sourceId === undefined) return null
    if (sourceId === null) return assetId
    return groupOfSource.value.get(sourceId) ?? sourceId
}

/** Named handle for a projected endpoint; assets use the default handles. */
function containerHandle(nodeId: string, type: 'source' | 'target'): string | undefined {
    if (groups.value.has(nodeId)) return `group-${type}`
    if (!assetToSource.value.has(nodeId)) return `source-${type}`
    return undefined
}

/** Cross-container edges at the top level, for the root layout. */
const topLevelEdges = computed(() => {
    const seen = new Set<string>()
    const result: Array<{ source: string; target: string }> = []

    for (const dep of dependencies.value) {
        const source = topLevelOf(dep.upstreamAssetId)
        const target = topLevelOf(dep.downstreamAssetId)
        if (!source || !target || source === target) continue

        const key = `${source}->${target}`
        if (seen.has(key)) continue
        seen.add(key)
        result.push({ source, target })
    }

    return result
})

/** Layout top-level nodes (groups + ungrouped sources + standalone assets) using DAG layout. */
const sourceLayout = computed(() => {
    const layoutNodes = [
        ...[...groups.value.keys()].map((groupId) => {
            const dims = getGroupDimensions(groupId)
            return { id: groupId, width: dims.width, height: dims.height }
        }),
        ...sourceEntries.value
            .filter(entry => !groupOfSource.value.has(entry.source.id))
            .map((entry) => {
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
        // gapX = within-layer, gapY = between-layer. In LR that means the
        // vertical gap within a rank and the horizontal gap between ranks.
        gapX: props.compact ? 22 : 32,
        gapY: props.compact ? 80 : 90,
        direction: props.direction,
    })
})

/** Structural edges (id/source/target/handles); styling + focus applied in `edges`. */
const baseEdges = computed(() => {
    const result: Array<{ id: string; source: string; target: string; sourceHandle?: string; targetHandle?: string }> = []
    const seen = new Set<string>()

    for (const dep of dependencies.value) {
        const source = representativeOf(dep.upstreamAssetId)
        const target = representativeOf(dep.downstreamAssetId)
        // Same representative = the dependency is hidden inside a collapsed container.
        if (!source || !target || source === target) continue

        const key = `${source}->${target}`
        if (seen.has(key)) continue
        seen.add(key)
        result.push({
            id: key,
            source,
            target,
            sourceHandle: containerHandle(source, 'source'),
            targetHandle: containerHandle(target, 'target'),
        })
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
    // Keep every kept node's ancestor containers un-faded — CSS opacity
    // cascades to children, so a faded ancestor would dim the node itself.
    for (const nodeId of [...nodeIds]) {
        const parentSource = assetEntryById.value.get(nodeId)?.source
        if (parentSource) nodeIds.add(parentSource.id)
        const groupId = groupOfSource.value.get(parentSource?.id ?? nodeId)
        if (groupId) nodeIds.add(groupId)
    }
    return { edgeKeys, nodeIds }
})

/** Push a source node (and, when expanded, its asset children), optionally nested in a group. */
function pushSourceNodes(result: Node[], entry: GraphSourceEntry, pos: { x: number; y: number }, parent?: string) {
    const source = entry.source
    const open = expandedSources.value.has(source.id)
    const container = isNodesExpanded(source.id)
    const dims = getSourceDimensions(source.id)

    result.push({
        id: source.id,
        type: 'source',
        position: { x: pos.x, y: pos.y },
        parentNode: parent,
        extent: parent ? 'parent' : undefined,
        width: dims.width,
        height: dims.height,
        connectable: !container,
        zIndex: parent ? 1 : undefined,
        data: {
            source,
            sourceDefn: entry.sourceDefn,
            status: entry.status,
            open,
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
                        y: assetPos.y + SRC_HEADER_H + SRC_BODY_TOP,
                    },
                    data: { asset: child.asset, assetDefn: child.assetDefn, source, status: child.status },
                    connectable: true,
                    zIndex: parent ? 2 : 1,
                })
            }
        }
    }
}

/** Flat node array: group nodes + source nodes + asset child nodes + standalone asset nodes. */
const nodes = computed<Node[]>(() => {
    const result: Node[] = []

    // Group nodes first — VueFlow requires parents before their children.
    for (const [groupId, members] of groups.value) {
        const pos = sourceLayout.value.positions.get(groupId) ?? { x: 0, y: 0 }
        const open = isGroupExpanded(groupId)
        const dims = getGroupDimensions(groupId)

        result.push({
            id: groupId,
            type: 'sourceGroup',
            position: { x: pos.x, y: pos.y },
            width: dims.width,
            height: dims.height,
            connectable: false,
            data: {
                groupKey: members[0]!.source.key,
                sourceDefn: members[0]!.sourceDefn,
                members,
                open,
                status: groupStatus(members),
            },
        })

        if (open) {
            const innerLayout = getGroupInnerLayout(groupId)
            for (const member of members) {
                const memberPos = innerLayout.positions.get(member.source.id) ?? { x: 0, y: 0 }
                pushSourceNodes(result, member, {
                    x: memberPos.x + GRP_PADDING,
                    y: memberPos.y + SRC_HEADER_H + SRC_BODY_TOP,
                }, groupId)
            }
        }
    }

    for (const entry of sourceEntries.value) {
        if (groupOfSource.value.has(entry.source.id)) continue
        const pos = sourceLayout.value.positions.get(entry.source.id) ?? { x: 0, y: 0 }
        pushSourceNodes(result, entry, pos)
    }

    for (const entry of standaloneEntries.value) {
        const pos = sourceLayout.value.positions.get(entry.asset.id) ?? { x: 0, y: 0 }
        result.push({
            id: entry.asset.id,
            type: 'asset',
            position: { x: pos.x, y: pos.y },
            data: { asset: entry.asset, assetDefn: entry.assetDefn, source: null, status: entry.status },
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
            ? { stroke: 'var(--graph-edge)', strokeWidth: 1.5 }
            : active
                ? { stroke: 'var(--ui-primary)', strokeWidth: 2.5 }
                : { stroke: 'var(--graph-edge)', strokeWidth: 1.5, opacity: '0.15' }
        return {
            ...e,
            type: 'dependency',
            zIndex: active ? 1003 : 1001,
            style,
        }
    })
})

function onNodeClick({ node }: { node: Node }) {
    if (node.type === 'sourceGroup') {
        toggleGroup(node.id)
    }
    else if (node.type === 'source') {
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
                expandedGroups.value = new Set(groups.value.keys())
                expandedSources.value = new Set(sourceEntries.value.map(e => e.source.id))
            },
        },
        {
            label: 'Collapse all',
            icon: 'i-lucide-minimize-2',
            onSelect: () => {
                expandedGroups.value = new Set()
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
                 :nodes-draggable="false"
                 :select-nodes-on-drag="false"
                 :elevate-nodes-on-select="false"
                 :connection-radius="80"
                 :auto-connect="false"
                 @node-click="onNodeClick"
                 @connect="onConnect"
                 @edge-context-menu="onEdgeContextMenu"
                 @pane-click="emit('pane-click')"
                 @pane-context-menu="onPaneContextMenu">
            <template #node-sourceGroup="{ data }">
                <GraphSourceTypeNode :group-key="data.groupKey"
                                     :source-defn="data.sourceDefn"
                                     :members="data.members"
                                     :open="data.open"
                                     :status="data.status" />
            </template>
            <template #node-source="{ data }">
                <GraphSourceNode :source="data.source"
                                 :source-defn="data.sourceDefn"
                                 :status="data.status"
                                 :open="data.open"
                                 :selected="false"
                                 @edit="emit('edit-source', $event)"
                                 @delete="onDeleteSource" />
            </template>
            <template #node-asset="{ data }">
                <GraphRunNode v-if="compact"
                              :asset="data.asset"
                              :asset-defn="data.assetDefn"
                              :status="data.status"
                              :selected="data.asset.id === selectedId"
                              @view="emit('asset-click', data.asset, data.assetDefn, data.source)" />
                <GraphAssetNode v-else
                                :asset="data.asset"
                                :asset-defn="data.assetDefn"
                                :status="data.status"
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
