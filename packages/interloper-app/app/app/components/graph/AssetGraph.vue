<script setup lang="ts">
import type { ContextMenuItem } from '@nuxt/ui'
import { VueFlow, useVueFlow, Panel, MarkerType } from '@vue-flow/core'
import type { Node, Edge, Connection } from '@vue-flow/core'
import { Background } from '@vue-flow/background'
import { Controls } from '@vue-flow/controls'

const props = withDefaults(defineProps<{
    sourceIds?: string[]
    readonly?: boolean
}>(), {
    sourceIds: undefined,
    readonly: false,
})

const flowId = `asset-graph-${useId()}`
const vueFlow = useVueFlow(flowId)
const sourcesStore = useSourcesStore()
const assetsStore = useAssetsStore()
const catalogStore = useCatalogStore()
const { sources: allSources, loading } = storeToRefs(sourcesStore)
const { dependencies: assetDependencies } = storeToRefs(assetsStore)
const { layoutDag } = useGraphLayout()

// Filter sources when sourceIds prop is provided
const sources = computed(() => {
    if (!props.sourceIds) return allSources.value
    const ids = new Set(props.sourceIds)
    return allSources.value.filter(s => ids.has(s.id))
})

const emit = defineEmits<{
    'add-source': []
    'edit-source': [sourceId: string]
    'asset-click': [asset: SourceAsset | Asset, assetDefn: AssetDefinition | undefined, source: Source | null]
    'pane-click': []
    'delete-source': [sourceId: string]
    'create-dependencies': [pairs: Array<{ upstreamAssetId: string; downstreamAssetId: string }>]
    'delete-dependency': [payload: { upstreamAssetId: string; downstreamAssetId: string }]
}>()

// Track which sources are expanded
const expandedSources = ref(new Set<string>())

function toggleSource(sourceId: string) {
    const next = new Set(expandedSources.value)
    if (next.has(sourceId)) next.delete(sourceId)
    else next.add(sourceId)
    expandedSources.value = next
}

// Layout constants
const ASSET_W = 220
const ASSET_H = 160
const SRC_PADDING = 26
const SRC_HEADER_H = 50
const COLLAPSED_W = 232
const COLLAPSED_H = 76

// Actual measured heights from VueFlow's ResizeObserver
const measuredHeights = ref(new Map<string, number>())

// Clear measured heights for a source's assets when it collapses
watch(expandedSources, (next, prev) => {
    for (const sourceId of prev) {
        if (!next.has(sourceId)) {
            const source = sources.value.find(s => s.id === sourceId)
            if (source) {
                const updated = new Map(measuredHeights.value)
                for (const asset of source.assets) {
                    updated.delete(asset.id)
                }
                measuredHeights.value = updated
            }
        }
    }
})

// Standalone assets from the assets store
const standaloneAssets = computed(() => assetsStore.standalone)

// Asset ID -> Source ID lookup (null for standalone assets)
const assetToSource = computed(() => {
    const map = new Map<string, string | null>()
    for (const source of sources.value) {
        for (const asset of source.assets) {
            map.set(asset.id, source.id)
        }
    }
    for (const asset of standaloneAssets.value) {
        map.set(asset.id, null)
    }
    return map
})

// Asset ID -> qualified key ("source_key.asset_key" or bare "key") lookup
const qualifiedKeyById = computed(() => {
    const map = new Map<string, string>()
    for (const source of sources.value) {
        for (const asset of source.assets) {
            map.set(asset.id, `${source.key}.${asset.key}`)
        }
    }
    for (const asset of standaloneAssets.value) {
        map.set(asset.id, asset.key)
    }
    return map
})

/** Look up an AssetDefinition by qualified key or bare key. */
function getAssetDefinition(qk: string): AssetDefinition | undefined {
    return catalogStore.getAssetDefinition(qk)
}

/** Run inner DAG layout for a source's assets. */
function getInnerLayout(source: Source) {
    const assetIds = new Set(source.assets.map(a => a.id))
    const intraEdges = assetDependencies.value
        .filter(d => assetIds.has(d.asset_id) && assetIds.has(d.upstream_asset_id))
        .map(d => ({ source: d.upstream_asset_id, target: d.asset_id }))

    const layoutNodes = source.assets.map(a => ({
        id: a.id,
        width: ASSET_W,
        height: measuredHeights.value.get(a.id) ?? ASSET_H,
    }))

    return layoutDag(layoutNodes, intraEdges)
}

/** Compute each source node's dimensions. */
function getSourceDimensions(source: Source): { width: number; height: number } {
    if (!expandedSources.value.has(source.id)) {
        return { width: COLLAPSED_W, height: COLLAPSED_H }
    }

    if (source.assets.length === 0) {
        return { width: COLLAPSED_W, height: COLLAPSED_H }
    }

    const dag = getInnerLayout(source)

    return {
        width: dag.width + SRC_PADDING * 2,
        height: dag.height + SRC_HEADER_H + SRC_PADDING * 2,
    }
}

/** Cross-group edges at the top level (between sources and standalone assets). */
const topLevelEdges = computed(() => {
    const seen = new Set<string>()
    const result: Array<{ source: string; target: string }> = []

    for (const dep of assetDependencies.value) {
        const downstreamSourceId = assetToSource.value.get(dep.asset_id)
        const upstreamSourceId = assetToSource.value.get(dep.upstream_asset_id)
        if (downstreamSourceId === undefined || upstreamSourceId === undefined) continue

        // For top-level layout: use source ID if source-owned, asset ID if standalone
        const upstreamNode = upstreamSourceId ?? dep.upstream_asset_id
        const downstreamNode = downstreamSourceId ?? dep.asset_id
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
        ...sources.value.map((source) => {
            const dims = getSourceDimensions(source)
            return { id: source.id, width: dims.width, height: dims.height }
        }),
        ...standaloneAssets.value.map(asset => ({
            id: asset.id,
            width: ASSET_W,
            height: measuredHeights.value.get(asset.id) ?? ASSET_H,
        })),
    ]

    return layoutDag(layoutNodes, topLevelEdges.value, { gapX: 60, gapY: 60 })
})

/** Flat node array: source nodes + asset child nodes + standalone asset nodes. */
const nodes = computed<Node[]>(() => {
    const result: Node[] = []

    // Source nodes with their child assets
    for (const source of sources.value) {
        const sourceDefn = catalogStore.getSourceDefinition(source.key)
        const pos = sourceLayout.value.positions.get(source.id) ?? { x: 0, y: 0 }
        const isExpanded = expandedSources.value.has(source.id)
        const dims = getSourceDimensions(source)

        result.push({
            id: source.id,
            type: 'source',
            position: { x: pos.x, y: pos.y },
            width: dims.width,
            height: dims.height,
            connectable: !isExpanded,
            data: {
                source,
                sourceDefn,
                expanded: isExpanded,
            },
        })

        if (isExpanded && source.assets.length > 0) {
            const innerLayout = getInnerLayout(source)
            for (const asset of source.assets) {
                const assetDefn = getAssetDefinition(`${source.key}.${asset.key}`)
                const assetPos = innerLayout.positions.get(asset.id) ?? { x: 0, y: 0 }
                result.push({
                    id: asset.id,
                    type: 'asset',
                    parentNode: source.id,
                    extent: 'parent',
                    position: {
                        x: assetPos.x + SRC_PADDING,
                        y: assetPos.y + SRC_HEADER_H + SRC_PADDING,
                    },
                    data: { asset, assetDefn, source },
                    draggable: true,
                    expandParent: true,
                    connectable: true,
                    zIndex: 1,
                })
            }
        }
    }

    // Standalone asset nodes (top-level, no parent)
    for (const asset of standaloneAssets.value) {
        const assetDefn = getAssetDefinition(asset.key)
        const pos = sourceLayout.value.positions.get(asset.id) ?? { x: 0, y: 0 }
        result.push({
            id: asset.id,
            type: 'asset',
            position: { x: pos.x, y: pos.y },
            data: { asset, assetDefn, source: null },
            draggable: true,
            connectable: true,
        })
    }

    return result
})

/** Unified edge list from all asset dependencies. */
const edges = computed<Edge[]>(() => {
    const result: Edge[] = []
    const seen = new Set<string>()

    for (const dep of assetDependencies.value) {
        const upstreamSourceId = assetToSource.value.get(dep.upstream_asset_id)
        const downstreamSourceId = assetToSource.value.get(dep.asset_id)
        // Skip if either asset is unknown
        if (upstreamSourceId === undefined || downstreamSourceId === undefined) continue

        // Standalone assets (null source) are always "expanded" — they're top-level nodes
        const upstreamIsStandalone = upstreamSourceId === null
        const downstreamIsStandalone = downstreamSourceId === null
        const upstreamExpanded = upstreamIsStandalone || expandedSources.value.has(upstreamSourceId!)
        const downstreamExpanded = downstreamIsStandalone || expandedSources.value.has(downstreamSourceId!)
        const isSameSource = upstreamSourceId !== null
            && downstreamSourceId !== null
            && upstreamSourceId === downstreamSourceId

        if (isSameSource) {
            if (upstreamExpanded) {
                result.push({
                    id: `asset:${dep.upstream_asset_id}->${dep.asset_id}`,
                    type: 'dependency',
                    source: dep.upstream_asset_id,
                    target: dep.asset_id,
                    animated: true,
                    markerEnd: MarkerType.ArrowClosed,
                    zIndex: 1002,
                })
            }
        }
        else {
            const edgeSource = upstreamExpanded ? dep.upstream_asset_id : upstreamSourceId!
            const edgeTarget = downstreamExpanded ? dep.asset_id : downstreamSourceId!
            const sourceHandle = upstreamExpanded ? undefined : 'source-source'
            const targetHandle = downstreamExpanded ? undefined : 'source-target'

            const key = `${edgeSource}->${edgeTarget}`
            if (seen.has(key)) continue
            seen.add(key)

            result.push({
                id: key,
                type: 'dependency',
                source: edgeSource,
                target: edgeTarget,
                sourceHandle,
                targetHandle,
                animated: true,
                markerEnd: MarkerType.ArrowClosed,
                zIndex: 1002,
            })
        }
    }

    return result
})

function onNodeClick({ node }: { node: Node }) {
    if (node.type === 'source') {
        toggleSource(node.id)
    }
    else if (node.type === 'asset') {
        emit('asset-click', node.data.asset, node.data.assetDefn, node.data.source)
    }
}

async function onDeleteSource(sourceId: string) {
    if (props.readonly) return
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
    vueFlow.zoomTo(1)
})

// ── Connection validation & creation ──

const { resolveConnectionPairs, isValidConnection } = useGraphConnectionRules({
    sources,
    assetDependencies,
    assetToSource,
    qualifiedKeyById,
    getAssetDefinition,
})

function onConnect(connection: Connection) {
    if (props.readonly) return
    const pairs = resolveConnectionPairs(connection)
    emit('create-dependencies', pairs)
}

provide('isValidConnection', isValidConnection)
provide('graphReadonly', toRef(() => props.readonly))

// Provide empty materializing set for now (can be wired up later)
const materializingAssetIds = computed(() => new Set<string>())
provide('materializingAssetIds', materializingAssetIds)

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
                expandedSources.value = new Set(sources.value.map(s => s.id))
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

    if (props.readonly) return
    if (!('clientX' in event)) return

    // Only allow delete on asset-to-asset edges (not source-level collapsed edges)
    const isSourceAsset = assetToSource.value.has(edge.source) || standaloneAssets.value.some(a => a.id === edge.source)
    const isTargetAsset = assetToSource.value.has(edge.target) || standaloneAssets.value.some(a => a.id === edge.target)
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
                                 :expanded="data.expanded"
                                 @edit="emit('edit-source', $event)"
                                 @delete="onDeleteSource" />
            </template>
            <template #node-asset="{ data }">
                <GraphAssetNode :asset="data.asset"
                                :asset-defn="data.assetDefn"
                                @view="emit('asset-click', data.asset, data.assetDefn, data.source)" />
            </template>
            <template #edge-dependency="edgeProps">
                <GraphDependencyEdge v-bind="edgeProps" />
            </template>
            <Background :size=".8" />
            <Controls position="bottom-left"
                      :show-interactive="false" />

            <Panel v-if="!props.readonly && sources.length === 0"
                   position="top-left"
                   class="!inset-0 !m-0 flex items-center justify-center pointer-events-none">
                <UButton icon="i-lucide-plus"
                         label="Add your first source"
                         size="lg"
                         class="pointer-events-auto"
                         @click="emit('add-source')" />
            </Panel>
            <Panel v-else-if="!props.readonly"
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
