import type { Connection } from '@vue-flow/core'
import { ANY_SOURCE, keysOf, parseQualifiedKey, qualifiedKey, upstreamRelations } from '~/types/catalog'
import type { ComponentRecord, Relation } from '~/types/component'

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
    many: boolean
}

interface UseGraphConnectionRulesOptions {
    sources: Ref<ComponentRecord[]>
    /** Persisted `upstream` relations (src = downstream asset, dst = upstream). */
    assetDependencies: Ref<Relation[]>
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
     * 4. Order each layer to reduce edge crossings (barycenter sweeps)
     * 5. Center each layer horizontally
     * 6. Return top-left positions and bounding box
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

        const layerSpan = (ids: string[]) =>
            ids.reduce((sum, id) => sum + crossExtent(nodeMap.get(id)!), 0) + (ids.length - 1) * gapX

        // Barycenter ordering: sweeping down (parents), up (children), then down
        // again, each node moves to the mean position of its neighbours.
        // Positions are measured from each layer's center because layers are
        // centered when placed; a node with no neighbour on the swept side
        // keeps its own, and ties keep the input order.
        const ordered = sortedRanks.map(r => layers.get(r)!)
        const centers = () => {
            const result = new Map<string, number>()
            for (const ids of ordered) {
                let cursor = -layerSpan(ids) / 2
                for (const id of ids) {
                    const extent = crossExtent(nodeMap.get(id)!)
                    result.set(id, cursor + extent / 2)
                    cursor += extent + gapX
                }
            }
            return result
        }
        for (const down of [true, false, true]) {
            const indices = ordered.map((_, i) => i)
            for (const i of down ? indices.slice(1) : indices.slice(0, -1).reverse()) {
                const center = centers()
                const neighbours = down ? parents : children
                const barycenter = (id: string) => {
                    const linked = neighbours.get(id)!
                    if (linked.length === 0) return center.get(id)!
                    return linked.reduce((sum, other) => sum + center.get(other)!, 0) / linked.length
                }
                const weights = new Map(ordered[i]!.map(id => [id, barycenter(id)]))
                ordered[i] = [...ordered[i]!].sort((a, b) => weights.get(a)! - weights.get(b)!)
            }
        }

        // Cross-axis span of each layer, for centering.
        const layerSpans = ordered.map(layerSpan)
        const maxLayerSpan = Math.max(...layerSpans)

        // Position nodes
        let mainCursor = 0

        for (let i = 0; i < ordered.length; i++) {
            const ids = ordered[i]!

            // Center this layer within the widest layer.
            let cross = (maxLayerSpan - layerSpans[i]!) / 2
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
    /**
     * Whether this exact binding is already persisted, the one pair a drag
     * cannot change. Any other pair stays connectable: a `many` relation
     * fans in one more upstream, and a single-valued one is repointed by the
     * API (it drops the binding it holds and inserts the new one).
     */
    function depExists(downstreamId: string, upstreamId: string, name: string): boolean {
        return options.assetDependencies.value.some(
            d => d.src_id === downstreamId && d.name === name && d.dst_id === upstreamId,
        )
    }

    /** The relation name a candidate upstream's qualified key satisfies, if any. */
    function matchingRelationName(spec: AssetDefinition, upstreamQk: string): string | undefined {
        for (const [name, relation] of Object.entries(upstreamRelations(spec))) {
            for (const declared of keysOf(relation)) {
                const { sourceKey, assetKey } = parseQualifiedKey(declared)
                // A bare key is a same-source sibling the framework binds on its
                // own; it never pairs assets across sources on the graph.
                if (!sourceKey) continue
                if (sourceKey === ANY_SOURCE ? parseQualifiedKey(upstreamQk).assetKey === assetKey : declared === upstreamQk) {
                    return name
                }
            }
        }
        return undefined
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
                for (const a of source.children) {
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
                for (const a of source.children) {
                    downstreamAssets.push({ id: a.id, qk: qualifiedKey(source.key, a.key) })
                }
            }
        }

        // Match: for each downstream, check if any upstream satisfies one of its relations
        const pairs: DependencyPair[] = []
        for (const downstream of downstreamAssets) {
            const spec = options.getAssetDefinition(downstream.qk)
            if (!spec) continue
            const relations = upstreamRelations(spec)
            for (const upstream of upstreamAssets) {
                if (upstream.id === downstream.id) continue
                const paramName = matchingRelationName(spec, upstream.qk)
                if (!paramName) continue
                if (depExists(downstream.id, upstream.id, paramName)) continue
                pairs.push({
                    upstreamAssetId: upstream.id,
                    downstreamAssetId: downstream.id,
                    paramName,
                    many: relations[paramName]!.many,
                })
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
