import type { Connection } from '@vue-flow/core'
import { qualifiedKey } from '~/types/catalog'

// ─── Types ───────────────────────────────────────────────────────────

interface LayoutNode {
    id: string
    width: number
    height: number
}

interface LayoutEdge {
    source: string
    target: string
}

interface LayoutOptions {
    direction?: 'TB' | 'LR'
    gapX?: number
    gapY?: number
}

interface LayoutResult {
    positions: Map<string, { x: number; y: number }>
    width: number
    height: number
}

export interface DependencyPair {
    upstreamAssetId: string
    downstreamAssetId: string
    paramName: string
}

interface UseGraphConnectionRulesOptions {
    sources: Ref<Source[]>
    assetDependencies: Ref<AssetDependency[]>
    assetToSource: Ref<Map<string, string | null>>
    /** asset id → qualified key ("source_key.asset_key") */
    qualifiedKeyById: Ref<Map<string, string>>
    getAssetDefinition: (qualifiedKey: string) => AssetDefinition | undefined
}

// ─── Layout ──────────────────────────────────────────────────────────

export function useGraphLayout() {
    /**
     * Compute a layered DAG layout using longest-path ranking.
     *
     * 1. Build adjacency lists + in-degree map
     * 2. Assign each node a rank = longest path from any root
     * 3. Group nodes by rank into layers
     * 4. Center each layer horizontally
     * 5. Return top-left positions and bounding box
     */
    function layoutDag(
        nodes: LayoutNode[],
        edges: LayoutEdge[],
        options?: LayoutOptions,
    ): LayoutResult {
        const direction = options?.direction ?? 'TB'
        const gapX = options?.gapX ?? 60
        const gapY = options?.gapY ?? 80

        const positions = new Map<string, { x: number; y: number }>()

        if (nodes.length === 0) {
            return { positions, width: 0, height: 0 }
        }

        const nodeMap = new Map(nodes.map(n => [n.id, n]))

        // Build adjacency (source → targets) and reverse adjacency (target → sources)
        const children = new Map<string, string[]>()
        const parents = new Map<string, string[]>()
        for (const n of nodes) {
            children.set(n.id, [])
            parents.set(n.id, [])
        }
        for (const e of edges) {
            children.get(e.source)?.push(e.target)
            parents.get(e.target)?.push(e.source)
        }

        // Assign ranks via longest-path from roots (BFS with in-degree tracking)
        const rank = new Map<string, number>()
        const inDegree = new Map<string, number>()
        for (const n of nodes) {
            inDegree.set(n.id, parents.get(n.id)!.length)
        }

        // Kahn's algorithm variant — track longest path to each node
        const queue: string[] = []
        for (const n of nodes) {
            rank.set(n.id, 0)
            if (inDegree.get(n.id) === 0) {
                queue.push(n.id)
            }
        }

        while (queue.length > 0) {
            const id = queue.shift()!
            const currentRank = rank.get(id)!
            for (const child of children.get(id)!) {
                const newRank = currentRank + 1
                if (newRank > rank.get(child)!) {
                    rank.set(child, newRank)
                }
                inDegree.set(child, inDegree.get(child)! - 1)
                if (inDegree.get(child) === 0) {
                    queue.push(child)
                }
            }
        }

        // Group nodes into layers by rank
        const layers = new Map<number, string[]>()
        for (const n of nodes) {
            const r = rank.get(n.id)!
            if (!layers.has(r)) layers.set(r, [])
            layers.get(r)!.push(n.id)
        }

        const sortedRanks = [...layers.keys()].sort((a, b) => a - b)

        const isVertical = direction === 'TB'
        // Extent along the within-layer axis (X for TB, Y for LR) and the
        // layer-advance axis (Y for TB, X for LR). Swapping these by direction
        // is what makes 'LR' actually flow left-to-right.
        const crossExtent = (n: LayoutNode) => (isVertical ? n.width : n.height)
        const mainExtent = (n: LayoutNode) => (isVertical ? n.height : n.width)

        // Cross-axis span of each layer, for centering.
        const layerSpans = sortedRanks.map((r) => {
            const ids = layers.get(r)!
            return ids.reduce((sum, id) => sum + crossExtent(nodeMap.get(id)!), 0)
                + (ids.length - 1) * gapX
        })
        const maxLayerSpan = Math.max(...layerSpans)

        // Position nodes
        let mainCursor = 0

        for (let i = 0; i < sortedRanks.length; i++) {
            const r = sortedRanks[i]!
            const ids = layers.get(r)!
            const layerSpan = layerSpans[i]!

            // Center this layer within the widest layer.
            let cross = (maxLayerSpan - layerSpan) / 2
            const maxMain = Math.max(...ids.map(id => mainExtent(nodeMap.get(id)!)))

            for (const id of ids) {
                const node = nodeMap.get(id)!
                if (isVertical) positions.set(id, { x: cross, y: mainCursor })
                else positions.set(id, { x: mainCursor, y: cross })
                cross += crossExtent(node) + gapX
            }

            mainCursor += maxMain + gapY
        }

        // Remove trailing gap
        mainCursor -= gapY

        return {
            positions,
            width: isVertical ? maxLayerSpan : mainCursor,
            height: isVertical ? mainCursor : maxLayerSpan,
        }
    }

    return { layoutDag }
}

// ─── Connection Rules ────────────────────────────────────────────────

export function useGraphConnectionRules(options: UseGraphConnectionRulesOptions) {
    /** Check if a dependency already exists. */
    function depExists(downstreamId: string, upstreamId: string): boolean {
        return options.assetDependencies.value.some(
            d => d.asset_id === downstreamId && d.upstream_asset_id === upstreamId,
        )
    }

    function resolveConnectionPairs(connection: Connection): DependencyPair[] {
        const { source: srcNode, target: tgtNode } = connection
        const srcIsAsset = options.assetToSource.value.has(srcNode)
        const tgtIsAsset = options.assetToSource.value.has(tgtNode)

        // Collect upstream assets (from source node or single asset)
        const upstreamAssets: Array<{ id: string; qk: string }> = []
        if (srcIsAsset) {
            const qk = options.qualifiedKeyById.value.get(srcNode)
            if (qk) upstreamAssets.push({ id: srcNode, qk })
        }
        else {
            const source = options.sources.value.find(s => s.id === srcNode)
            if (source) {
                for (const a of source.assets) {
                    upstreamAssets.push({ id: a.id, qk: qualifiedKey(source.key, a.key) })
                }
            }
        }

        // Collect downstream assets (from target node or single asset)
        const downstreamAssets: Array<{ id: string; qk: string }> = []
        if (tgtIsAsset) {
            const qk = options.qualifiedKeyById.value.get(tgtNode)
            if (qk) downstreamAssets.push({ id: tgtNode, qk })
        }
        else {
            const source = options.sources.value.find(s => s.id === tgtNode)
            if (source) {
                for (const a of source.assets) {
                    downstreamAssets.push({ id: a.id, qk: qualifiedKey(source.key, a.key) })
                }
            }
        }

        // Match: for each downstream, check if any upstream satisfies its requires
        const pairs: DependencyPair[] = []
        for (const downstream of downstreamAssets) {
            const spec = options.getAssetDefinition(downstream.qk)
            if (!spec) continue
            const allReqs = { ...spec.requires, ...spec.optional_requires }
            for (const upstream of upstreamAssets) {
                if (upstream.id === downstream.id) continue
                // Find the param name that this upstream satisfies
                const paramName = Object.entries(allReqs).find(([, reqQk]) => reqQk === upstream.qk)?.[0]
                if (!paramName) continue
                if (depExists(downstream.id, upstream.id)) continue
                pairs.push({ upstreamAssetId: upstream.id, downstreamAssetId: downstream.id, paramName })
            }
        }
        return pairs
    }

    function isValidConnection(connection: Connection) {
        if (connection.source === connection.target) return false
        return resolveConnectionPairs(connection).length > 0
    }

    return { resolveConnectionPairs, isValidConnection }
}
