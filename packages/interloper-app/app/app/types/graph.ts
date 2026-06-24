import type { Asset, AssetDependency } from './asset'
import type { Source, SourceAsset } from './source'
import type { AssetDefinition, SourceDefinition } from './catalog'
import type { ExecutionStatus } from './asset_execution'

/**
 * Canonical node status states shared across every graph surface
 * (catalog graph page, job page, run page). Decoration (colour, dot)
 * is derived from this single union so all surfaces agree.
 *
 * `idle | attention | paused` are catalog-level states; the remainder
 * mirror {@link ExecutionStatus} for the live run-page graph.
 */
export type GraphNodeState =
    | 'idle'
    | 'attention'
    | 'paused'
    | 'queued'
    | 'pending'
    | 'running'
    | 'success'
    | 'failed'
    | 'skipped'
    | 'canceled'

export interface NodeStatus {
    state: GraphNodeState
    /** Optional human label, e.g. "Last run 38m ago". */
    label?: string
}

/** Map a backend execution status onto a graph node state. */
export function stateFromExecution(status: ExecutionStatus): GraphNodeState {
    return status
}

/** How a source reveals its assets on the canvas. */
export type ExpandMode = 'list' | 'graph' | 'nodes'

/** Which dimension the canvas decorates nodes by. */
export type ViewMode = 'topology' | 'status'

/** Source health filter for the catalog graph (derived states only). */
export type StatusFilter = 'all' | 'healthy' | 'attention' | 'paused'

/** A directed asset→asset dependency, normalised away from the store shape. */
export interface GraphDependency {
    upstreamAssetId: string
    downstreamAssetId: string
}

export interface GraphSourceEntry {
    source: Source
    sourceDefn: SourceDefinition | undefined
    status?: NodeStatus
}

export interface GraphAssetEntry {
    asset: SourceAsset | Asset
    assetDefn: AssetDefinition | undefined
    /** Owning source, or null for standalone assets. */
    source: Source | null
    status?: NodeStatus
}

/**
 * Normalised, store-independent graph model consumed by <GraphCanvas>.
 * Each surface (catalog / job / run) produces this same shape from its
 * own data, which is what lets one renderer serve all three.
 */
export interface GraphModel {
    sources: GraphSourceEntry[]
    /** All assets — source-owned and standalone — for child + node lookup by id. */
    assets: GraphAssetEntry[]
    dependencies: GraphDependency[]
}

/** Convert a raw store dependency into the normalised graph shape. */
export function toGraphDependency(dep: AssetDependency): GraphDependency {
    return { upstreamAssetId: dep.upstream_asset_id, downstreamAssetId: dep.asset_id }
}
