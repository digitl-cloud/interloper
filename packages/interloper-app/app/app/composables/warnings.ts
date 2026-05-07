import type { Source } from '~/types/source'
import type { AssetDefinition } from '~/types/catalog'
import { parseQualifiedKey, qualifiedKey } from '~/types/catalog'

// ─── Types ───────────────────────────────────────────────────────────

export interface AssetWarning {
    category: 'dependency' | 'destination' | 'job'
    message: string
}

// ─── Composable ──────────────────────────────────────────────────────

/**
 * Computes categorised warnings for assets.
 *
 * Warning categories:
 *   - dependency  — a spec-declared upstream dependency is missing or unlinked
 *   - destination — the asset's source has no destinations configured
 *   - job         — the asset's source is not associated with any job
 */
export function useAssetWarnings() {
    const sourcesStore = useSourcesStore()
    const assetsStore = useAssetsStore()
    const catalogStore = useCatalogStore()
    const jobsStore = useJobsStore()

    /** Recorded upstream asset IDs per asset. */
    const upstreamsByAssetId = computed(() => {
        const map = new Map<string, Set<string>>()
        for (const dep of assetsStore.dependencies) {
            if (!map.has(dep.asset_id)) map.set(dep.asset_id, new Set())
            map.get(dep.asset_id)!.add(dep.upstream_asset_id)
        }
        return map
    })

    /**
     * Qualified key → asset id.
     * Built from all sources so cross-source lookups work.
     */
    const assetIdByQualifiedKey = computed(() => {
        const map = new Map<string, string>()
        for (const source of sourcesStore.sources) {
            for (const asset of source.assets) {
                map.set(qualifiedKey(source.key, asset.key), asset.id)
            }
        }
        return map
    })

    /** asset id → source (for checking destinations at the source level). */
    const sourceByAssetId = computed(() => {
        const map = new Map<string, Source>()
        for (const source of sourcesStore.sources) {
            for (const asset of source.assets) {
                map.set(asset.id, source)
            }
        }
        return map
    })

    /** source id → set of job ids that reference it. */
    const jobsBySourceId = computed(() => {
        const map = new Map<string, string[]>()
        for (const job of jobsStore.jobs) {
            for (const sourceId of job.source_ids) {
                if (!map.has(sourceId)) map.set(sourceId, [])
                map.get(sourceId)!.push(job.id)
            }
        }
        return map
    })

    /** Look up an asset definition by qualified key. */
    function getAssetDefinition(qk: string): AssetDefinition | undefined {
        const { sourceKey, assetKey } = parseQualifiedKey(qk)
        if (sourceKey) {
            const src = catalogStore.sourceDefinitions.find(s => s.key === sourceKey)
            return src?.assets?.find(a => a.key === assetKey)
        }
        // Bare key fallback: scan all sources
        for (const src of catalogStore.sourceDefinitions) {
            const asset = src.assets?.find(a => a.key === qk)
            if (asset) return asset
        }
        return undefined
    }

    /**
     * Get all warnings for a single asset.
     */
    function getWarnings(assetId: string, assetKey: string): AssetWarning[] {
        const warnings: AssetWarning[] = []

        // Find the source this asset belongs to, for qualified key construction
        const source = sourceByAssetId.value.get(assetId)
        const qk = source ? qualifiedKey(source.key, assetKey) : assetKey
        const defn = getAssetDefinition(qk)

        // --- Dependency warnings ---
        if (defn?.requires) {
            const recorded = upstreamsByAssetId.value.get(assetId) ?? new Set()
            for (const [_param, depQk] of Object.entries(defn.requires)) {
                const upstreamId = assetIdByQualifiedKey.value.get(depQk)
                if (!upstreamId || !recorded.has(upstreamId)) {
                    const depDefn = getAssetDefinition(depQk)
                    const depName = depDefn?.name ?? depQk
                    warnings.push({ category: 'dependency', message: `Missing dependency: ${depName}` })
                }
            }
        }

        // --- Destination warnings ---
        if (source && source.destinations.length === 0) {
            warnings.push({ category: 'destination', message: 'No destination configured' })
        }

        // --- Job warnings ---
        if (source && !(jobsBySourceId.value.get(source.id)?.length)) {
            warnings.push({ category: 'job', message: 'Not associated with any job' })
        }

        return warnings
    }

    /**
     * Check whether any asset in a source has warnings.
     */
    function sourceHasWarnings(source: Source): boolean {
        return source.assets.some(a => getWarnings(a.id, a.key).length > 0)
    }

    /**
     * Filter warnings by category.
     */
    function filterByCategory(warnings: AssetWarning[], category: AssetWarning['category']): AssetWarning[] {
        return warnings.filter(w => w.category === category)
    }

    return { getWarnings, sourceHasWarnings, filterByCategory }
}
