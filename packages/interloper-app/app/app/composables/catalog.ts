import type { ComponentRecord, ComponentStatus, Relation } from '~/types/component'
import { jobTargetIds, relationIds } from '~/types/component'
import type { Run } from '~/types/run'
import type { AssetDefinition, SourceDefinition } from '~/types/catalog'
import { resourceSlots } from '~/types/catalog'
import type { AssetWarning } from '~/composables/warnings'
import type { SourceDriftStatus } from '~/composables/drift'

// ─── Types ───────────────────────────────────────────────────────────

export interface CatalogRow {
    id: string
    sourceId: string
    assetId: string
    name: string
    icon: string
    sourceKey: string
    assetStatus: ComponentStatus
    tags: string[]
    dependencies: Array<{ name: string; icon: string }>
    warnings: AssetWarning[]
    jobs: Array<{ name: string }>
    lastRunStatus: string | null
    lastRunAt: string | null
    connectionName: string | null
    connectionIcon: string | null
    destinations: Array<{ name: string; icon: string }>
    createdAt: string | null
    destinationName: string | null
    destinationIcon: string | null
    destinationCreatedAt: string | null
}

// ─── Catalog Rows ────────────────────────────────────────────────────

interface UseCatalogRowsOptions {
    sources: Ref<ComponentRecord[]>
    dependencies: Ref<Relation[]>
    destinations: Ref<ComponentRecord[]>
    jobs: Ref<ComponentRecord[]>
    runs: Ref<Run[]>
    getWarnings: (assetId: string, assetKey: string) => AssetWarning[]
}

export function useCatalogRows(options: UseCatalogRowsOptions) {
    const catalogStore = useCatalogStore()
    const { sourceDrift } = useDrift()

    /** Look up an asset definition by qualified key ("source_key.asset_key"). */
    function getAssetDefinition(qk: string): AssetDefinition | undefined {
        const dot = qk.indexOf('.')
        if (dot !== -1) {
            const sourceKey = qk.substring(0, dot)
            const assetKey = qk.substring(dot + 1)
            const src = catalogStore.sourceDefinitions.find(s => s.key === sourceKey)
            return src?.assets?.find(a => a.key === assetKey)
        }
        for (const src of catalogStore.sourceDefinitions) {
            const asset = src.assets?.find(a => a.key === qk)
            if (asset) return asset
        }
        return undefined
    }

    const dependenciesByAssetId = computed(() => {
        // Build asset id → qualified key map from all sources
        const qkById = new Map<string, string>()
        for (const source of options.sources.value) {
            for (const asset of source.children) {
                qkById.set(asset.id, `${source.key}.${asset.key}`)
            }
        }

        const map = new Map<string, Array<{ name: string; icon: string }>>()
        for (const dep of options.dependencies.value) {
            if (!map.has(dep.src_id)) map.set(dep.src_id, [])
            const upstreamQk = qkById.get(dep.dst_id)
            let name = upstreamQk ?? dep.dst_id
            let icon = 'i-lucide-box'
            if (upstreamQk) {
                const defn = getAssetDefinition(upstreamQk)
                if (defn) {
                    name = defn.name
                    icon = componentIcon(defn.key)
                }
            }
            map.get(dep.src_id)!.push({ name, icon })
        }
        return map
    })

    /** Jobs that reference each source. */
    const jobsBySourceId = computed(() => {
        const map = new Map<string, ComponentRecord[]>()
        for (const job of options.jobs.value) {
            for (const sourceId of jobTargetIds(job, 'source')) {
                if (!map.has(sourceId)) map.set(sourceId, [])
                map.get(sourceId)!.push(job)
            }
        }
        return map
    })

    /** Latest run per target component (job, source, or asset). */
    const lastRunByTargetId = computed(() => {
        const map = new Map<string, Run>()
        for (const run of options.runs.value) {
            if (!run.component_id) continue
            const existing = map.get(run.component_id)
            if (!existing || (run.started_at && (!existing.started_at || run.started_at > existing.started_at))) {
                map.set(run.component_id, run)
            }
        }
        return map
    })

    function getLastRunForSource(sourceId: string): { status: string | null; at: string | null } {
        // Runs targeting the source itself or its assets count, alongside its jobs' runs.
        const source = options.sources.value.find(s => s.id === sourceId)
        const targetIds = [
            sourceId,
            ...(source?.children.map(a => a.id) ?? []),
            ...(jobsBySourceId.value.get(sourceId) ?? []).map(j => j.id),
        ]
        let latest: Run | undefined
        for (const id of targetIds) {
            const run = lastRunByTargetId.value.get(id)
            if (!run) continue
            if (!latest || (run.started_at && (!latest.started_at || run.started_at > latest.started_at))) {
                latest = run
            }
        }
        return { status: latest?.status ?? null, at: latest?.started_at ?? null }
    }

    /** Connection resource for each source (first resource of kind "connection"). */
    const connectionBySourceId = computed(() => {
        const map = new Map<string, { name: string; icon: string }>()
        for (const source of options.sources.value) {
            const sourceDefn = catalogStore.getSourceDefinition(source.key)
            if (!sourceDefn) continue
            // Find the connection resource slot in the source definition
            for (const [_slotName, resourceKey] of Object.entries(resourceSlots(sourceDefn))) {
                if (resourceKey.endsWith('_connection') || resourceKey === 'connection') {
                    map.set(source.id, {
                        name: catalogStore.catalog[resourceKey]?.name ?? resourceKey,
                        icon: componentIcon(resourceKey, 'i-lucide-plug'),
                    })
                    break
                }
            }
        }
        return map
    })

    const data = computed<CatalogRow[]>(() => {
        const rows: CatalogRow[] = []
        for (const source of options.sources.value) {
            const destInfos = relationIds(source, 'destination').map((destId) => {
                const dest = options.destinations.value.find(d => d.id === destId)
                const defn = dest ? catalogStore.catalog[dest.key] : undefined
                return {
                    name: dest?.name ?? defn?.name ?? dest?.key ?? destId,
                    icon: componentIcon(dest?.key ?? '', 'i-lucide-hard-drive'),
                }
            })

            const sourceJobs = (jobsBySourceId.value.get(source.id) ?? []).map(j => ({ name: j.name ?? j.key }))
            const lastRun = getLastRunForSource(source.id)
            const conn = connectionBySourceId.value.get(source.id)

            for (const asset of source.children) {
                const assetDefn = getAssetDefinition(`${source.key}.${asset.key}`)

                const baseRow = {
                    sourceId: source.id,
                    assetId: asset.id,
                    name: assetDefn?.name ?? asset.key,
                    icon: componentIcon(asset.key),
                    sourceKey: source.key,
                    assetStatus: asset.status,
                    tags: assetDefn?.tags ?? [],
                    dependencies: dependenciesByAssetId.value.get(asset.id) ?? [],
                    warnings: options.getWarnings(asset.id, asset.key),
                    jobs: sourceJobs,
                    lastRunStatus: lastRun.status,
                    lastRunAt: lastRun.at,
                    connectionName: conn?.name ?? null,
                    connectionIcon: conn?.icon ?? null,
                    destinations: destInfos,
                    createdAt: asset.created_at,
                }

                if (destInfos.length > 0) {
                    for (let di = 0; di < destInfos.length; di++) {
                        const destInfo = destInfos[di]!
                        rows.push({
                            ...baseRow,
                            id: `${asset.id}_dest_${di}`,
                            destinationName: destInfo.name,
                            destinationIcon: destInfo.icon,
                            destinationCreatedAt: null,
                        })
                    }
                }
                else {
                    rows.push({
                        ...baseRow,
                        id: asset.id,
                        destinationName: null,
                        destinationIcon: null,
                        destinationCreatedAt: null,
                    })
                }
            }

            // Source with no assets: placeholder row
            if (source.children.length === 0) {
                rows.push({
                    id: source.id,
                    sourceId: source.id,
                    assetId: source.id,
                    name: '(no assets)',
                    icon: 'i-lucide-box',
                    sourceKey: source.key,
                    assetStatus: 'ok',
                    tags: [],
                    dependencies: [],
                    warnings: [],
                    jobs: sourceJobs,
                    lastRunStatus: lastRun.status,
                    lastRunAt: lastRun.at,
                    connectionName: conn?.name ?? null,
                    connectionIcon: conn?.icon ?? null,
                    destinations: destInfos,
                    createdAt: null,
                    destinationName: null,
                    destinationIcon: null,
                    destinationCreatedAt: null,
                })
            }
        }
        return rows
    })

    const sourceInfoById = computed(() => {
        const map = new Map<string, {
            name: string
            icon: string
            assetCount: number
            warnings: AssetWarning[]
            drift: SourceDriftStatus
        }>()
        for (const source of options.sources.value) {
            const sourceDefn: SourceDefinition | undefined = catalogStore.getSourceDefinition(source.key)

            // Aggregate & deduplicate warnings across all assets in the source
            const all = source.children.flatMap(a => options.getWarnings(a.id, a.key))
            const seen = new Set<string>()
            const warnings = all.filter((w) => {
                if (seen.has(w.message)) return false
                seen.add(w.message)
                return true
            })

            map.set(source.id, {
                name: source.name ?? sourceDefn?.name ?? source.key,
                icon: componentIcon(source.key, 'i-lucide-database'),
                assetCount: source.children.length,
                warnings,
                drift: sourceDrift(source),
            })
        }
        return map
    })

    const assetCount = computed(() =>
        options.sources.value.reduce((sum, s) => sum + s.children.length, 0),
    )

    return { data, sourceInfoById, assetCount }
}
