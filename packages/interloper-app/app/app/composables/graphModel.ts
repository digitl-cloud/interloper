import type { MaybeRefOrGetter } from 'vue'
import { qualifiedKey } from '~/types/catalog'
import { stateFromExecution, toGraphDependency } from '~/types/graph'
import type { GraphModel, GraphSourceEntry, GraphAssetEntry, NodeStatus } from '~/types/graph'
import type { ComponentRecord, Relation } from '~/types/component'
import { jobTargetIds } from '~/types/component'

/**
 * Graph model builders — one per surface. Each returns the same normalised
 * {@link GraphModel} so a single <GraphCanvas> can render the collection page,
 * a job's subgraph, or a run's live subgraph.
 *
 * Only {@link useCollectionGraph} is mounted today; {@link useJobGraph} and
 * {@link useRunGraph} are the seams for the (future) job and run pages.
 */

/** Assemble a model, keeping only dependencies whose endpoints are both present. */
function assemble(
    sources: GraphSourceEntry[],
    assets: GraphAssetEntry[],
    deps: Relation[],
): GraphModel {
    const present = new Set(assets.map(e => e.asset.id))
    const dependencies = deps
        .filter(d => present.has(d.src_id) && present.has(d.dst_id))
        .map(toGraphDependency)
    return { sources, assets, dependencies }
}

/** Standalone assets: persisted assets without an owning source. */
function standaloneAssets(componentsStore: ReturnType<typeof useComponentsStore>): ComponentRecord[] {
    return componentsStore.byKind('asset').filter(a => a.parent_id === null)
}

interface CollectionGraphOptions {
    /** Restrict to these source ids (undefined = every source). */
    sourceIds?: MaybeRefOrGetter<string[] | undefined>
}

/** Collection-wide graph: every source, its assets, standalone assets, all deps. */
export function useCollectionGraph(options: CollectionGraphOptions = {}) {
    const componentsStore = useComponentsStore()
    const catalogStore = useCatalogStore()
    const { assetStatus, sourceStatus } = useNodeStatus()

    const sources = computed(() => {
        const ids = toValue(options.sourceIds)
        if (!ids) return componentsStore.byKind('source')
        const set = new Set(ids)
        return componentsStore.byKind('source').filter(s => set.has(s.id))
    })

    const model = computed<GraphModel>(() => {
        const sourceEntries: GraphSourceEntry[] = sources.value.map(source => ({
            source,
            sourceDefn: catalogStore.getSourceDefinition(source.key),
            status: sourceStatus(source),
        }))

        const assetEntries: GraphAssetEntry[] = []
        for (const source of sources.value) {
            for (const asset of source.children) {
                assetEntries.push({
                    asset,
                    assetDefn: catalogStore.getAssetDefinition(qualifiedKey(source.key, asset.key)),
                    source,
                    status: assetStatus(asset.id, asset.key),
                })
            }
        }
        for (const asset of standaloneAssets(componentsStore)) {
            assetEntries.push({
                asset,
                assetDefn: catalogStore.getAssetDefinition(asset.key),
                source: null,
                status: assetStatus(asset.id, asset.key),
            })
        }

        return assemble(sourceEntries, assetEntries, componentsStore.dependencies)
    })

    return { model }
}

/** A single job's subgraph: the sources/assets the job materialises. (Seam — page not yet wired.) */
export function useJobGraph(jobId: MaybeRefOrGetter<string>) {
    const componentsStore = useComponentsStore()
    const catalogStore = useCatalogStore()
    const { assetStatus, sourceStatus } = useNodeStatus()

    const model = computed<GraphModel>(() => {
        const job = componentsStore.byId(toValue(jobId))
        if (!job) return { sources: [], assets: [], dependencies: [] }

        const sourceIds = new Set(jobTargetIds(job, 'source'))
        const assetIds = new Set(jobTargetIds(job, 'asset'))
        const sources = componentsStore.byKind('source').filter(s => sourceIds.has(s.id))

        const sourceEntries: GraphSourceEntry[] = sources.map(source => ({
            source,
            sourceDefn: catalogStore.getSourceDefinition(source.key),
            status: sourceStatus(source),
        }))

        const assetEntries: GraphAssetEntry[] = []
        for (const source of sources) {
            for (const asset of source.children) {
                if (!assetIds.has(asset.id)) continue
                assetEntries.push({
                    asset,
                    assetDefn: catalogStore.getAssetDefinition(qualifiedKey(source.key, asset.key)),
                    source,
                    status: assetStatus(asset.id, asset.key),
                })
            }
        }
        for (const asset of standaloneAssets(componentsStore)) {
            if (!assetIds.has(asset.id)) continue
            assetEntries.push({
                asset,
                assetDefn: catalogStore.getAssetDefinition(asset.key),
                source: null,
                status: assetStatus(asset.id, asset.key),
            })
        }

        return assemble(sourceEntries, assetEntries, componentsStore.dependencies)
    })

    return { model }
}

/**
 * A run's subgraph with *live* per-asset status from asset executions.
 * The caller is responsible for `useAssetExecutionsStore().fetchForRun(runId)`;
 * this model tracks that store (which is realtime-backed). (Seam — page not yet wired.)
 */
export function useRunGraph(runId: MaybeRefOrGetter<string>) {
    const assetExecutionsStore = useAssetExecutionsStore()
    const componentsStore = useComponentsStore()
    const catalogStore = useCatalogStore()

    const statusByAssetId = computed(() => {
        const map = new Map<string, NodeStatus>()
        for (const exec of assetExecutionsStore.assetExecutions) {
            if (exec.asset_id) map.set(exec.asset_id, { state: stateFromExecution(exec.status) })
        }
        return map
    })

    const model = computed<GraphModel>(() => {
        // runId identifies which run the page loaded into the store; the model
        // tracks the store contents reactively.
        void toValue(runId)
        const present = statusByAssetId.value
        const sources = componentsStore.byKind('source').filter(s => s.children.some(a => present.has(a.id)))

        const sourceEntries: GraphSourceEntry[] = sources.map(source => ({
            source,
            sourceDefn: catalogStore.getSourceDefinition(source.key),
            status: { state: 'running' as const },
        }))

        const assetEntries: GraphAssetEntry[] = []
        for (const source of sources) {
            for (const asset of source.children) {
                if (!present.has(asset.id)) continue
                assetEntries.push({
                    asset,
                    assetDefn: catalogStore.getAssetDefinition(qualifiedKey(source.key, asset.key)),
                    source,
                    status: present.get(asset.id),
                })
            }
        }
        for (const asset of standaloneAssets(componentsStore)) {
            if (!present.has(asset.id)) continue
            assetEntries.push({
                asset,
                assetDefn: catalogStore.getAssetDefinition(asset.key),
                source: null,
                status: present.get(asset.id),
            })
        }

        return assemble(sourceEntries, assetEntries, componentsStore.dependencies)
    })

    return { model }
}
