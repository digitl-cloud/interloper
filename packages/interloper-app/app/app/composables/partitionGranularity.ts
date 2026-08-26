import { today } from '@internationalized/date'
import type { ComponentRecord } from '~/types/component'
import type { SourceDefinition } from '~/types/catalog'
import { relationIds } from '~/types/component'

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

/**
 * The key of the period `periodsBack` whole periods before the current one,
 * on the given zone's calendar. Hour keys are always UTC-derived — hour
 * partition ids are UTC labels, mirroring the core granularity contract.
 */
export function periodKey(granularity: PartitionGranularity, periodsBack: number, timeZone = 'UTC'): string {
    if (granularity === 'hour') {
        const now = new Date()
        now.setUTCHours(now.getUTCHours() - periodsBack)
        return now.toISOString().slice(0, 13)
    }
    const t = today(timeZone)
    switch (granularity) {
        case 'day':
            return t.subtract({ days: periodsBack }).toString()
        case 'month':
            return t.subtract({ months: periodsBack }).toString().slice(0, 7)
        case 'year':
            return String(t.year - periodsBack)
    }
}

/** The most recent *complete* period's key: the natural default to run. */
export function previousPeriodKey(granularity: PartitionGranularity, timeZone = 'UTC'): string {
    return periodKey(granularity, 1, timeZone)
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
 * Granularities declared by target components' catalog definitions: every
 * partitioned asset under a source target, or the asset itself for an asset
 * target. Empty means no partitioned asset in scope.
 */
export function targetGranularities(ids: string[]): Set<string> {
    const componentsStore = useComponentsStore()
    const catalogStore = useCatalogStore()
    const found = new Set<string>()
    for (const id of ids) {
        const target = componentsStore.byId(id)
        if (!target) continue
        if (target.kind === 'source') {
            granularitiesOf(catalogStore.getSourceDefinition(target.key)).forEach(g => found.add(g))
        }
        else if (target.kind === 'asset') {
            const partitioning = catalogStore.getAssetDefinition(target.key)?.partitioning
            if (partitioning == null) continue
            const granularity = partitioning.granularity
            found.add(typeof granularity === 'string' ? granularity : 'day')
        }
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
    const catalogStore = useCatalogStore()

    return computed(() => {
        const record = target()
        const found = new Set<string>()
        if (record.kind === 'source') {
            granularitiesOf(catalogStore.getSourceDefinition(record.key)).forEach(g => found.add(g))
        }
        else if (record.kind === 'job') {
            targetGranularities(relationIds(record, 'target')).forEach(g => found.add(g))
        }
        const [only] = found
        return found.size === 1 && only !== undefined && only in KEY_PATTERNS
            ? only as PartitionGranularity
            : 'day'
    })
}
