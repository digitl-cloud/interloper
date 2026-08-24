import type { ComponentRecord } from '~/types/component'
import type { SourceDefinition } from '~/types/catalog'
import { jobTargetIds } from '~/types/component'

/** The granularities an asset may declare (the set BigQuery offers). */
export type PartitionGranularity = 'hour' | 'day' | 'month' | 'year'

/** The shape of each granularity's partition key, mirroring core's id formats. */
export const KEY_PATTERNS: Record<PartitionGranularity, RegExp> = {
    hour: /^\d{4}-\d{2}-\d{2}T\d{2}$/,
    day: /^\d{4}-\d{2}-\d{2}$/,
    month: /^\d{4}-\d{2}$/,
    year: /^\d{4}$/,
}

export const KEY_PLACEHOLDERS: Record<PartitionGranularity, string> = {
    hour: '2026-08-21T13',
    day: '2026-08-21',
    month: '2026-08',
    year: '2026',
}

/** The most recent *complete* period's key: the natural default to run. */
export function previousPeriodKey(granularity: PartitionGranularity): string {
    const now = new Date()
    switch (granularity) {
        case 'hour': {
            now.setUTCHours(now.getUTCHours() - 1)
            return `${now.toISOString().slice(0, 13)}`
        }
        case 'day': {
            now.setUTCDate(now.getUTCDate() - 1)
            return now.toISOString().slice(0, 10)
        }
        case 'month': {
            now.setUTCMonth(now.getUTCMonth() - 1)
            return now.toISOString().slice(0, 7)
        }
        case 'year':
            return String(now.getUTCFullYear() - 1)
    }
}

function granularitiesOf(defn: SourceDefinition | undefined): Set<string> {
    const found = new Set<string>()
    for (const asset of defn?.assets ?? []) {
        const granularity = asset.partitioning?.granularity
        if (asset.partitioning != null) found.add(typeof granularity === 'string' ? granularity : 'day')
    }
    return found
}

/**
 * Resolve the partition granularity of a runnable component's targets.
 *
 * Granularity lives on the target assets' catalog definitions — never on the
 * job's config, where a denormalized copy could drift — mirroring how the
 * scheduler resolves it. Falls back to `'day'` when nothing resolves;
 * disagreeing targets also fall back (the scheduler refuses those jobs, so
 * the picker's shape is moot).
 */
export function usePartitionGranularity(target: () => ComponentRecord): ComputedRef<PartitionGranularity> {
    const componentsStore = useComponentsStore()
    const catalogStore = useCatalogStore()

    return computed(() => {
        const record = target()
        const found = new Set<string>()
        if (record.kind === 'source') {
            granularitiesOf(catalogStore.getSourceDefinition(record.key)).forEach(g => found.add(g))
        }
        else if (record.kind === 'job') {
            for (const sourceId of jobTargetIds(record, 'source')) {
                const source = componentsStore.byId(sourceId)
                if (!source) continue
                granularitiesOf(catalogStore.getSourceDefinition(source.key)).forEach(g => found.add(g))
            }
        }
        const [only] = found
        return found.size === 1 && only !== undefined && only in KEY_PATTERNS
            ? only as PartitionGranularity
            : 'day'
    })
}
