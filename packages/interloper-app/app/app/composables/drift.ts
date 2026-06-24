import type { Source } from '~/types/source'
import type { ComponentStatus } from '~/types/component'

// ─── Types ───────────────────────────────────────────────────────────

/**
 * A source's rollup drift state: its own status, or `partial` when the
 * source is live but one or more of its assets have drifted out of it.
 */
export type SourceDriftStatus = ComponentStatus | 'partial'

export interface DriftBadge {
    label: string
    color: 'error' | 'warning' | 'neutral'
    icon: string
}

// ─── Composable ──────────────────────────────────────────────────────

/**
 * Catalog-drift presentation, derived from the `status` each source/asset
 * carries from the API (the same resolver hydration uses). Centralises the
 * status→badge mapping and the source rollup so the table, graph nodes, and
 * health banner stay consistent.
 *
 * Only `missing` is true drift (removable); `disabled` is intentional
 * (the component may return when the deployment re-enables it) and is shown
 * quietly, never flagged for cleanup.
 */
export function useDrift() {
    const sourcesStore = useSourcesStore()

    /** Badge metadata for a status, or `null` when nothing should be shown. */
    function statusBadge(status: SourceDriftStatus): DriftBadge | null {
        switch (status) {
            case 'missing':
                return { label: 'Unavailable in catalog', color: 'error', icon: 'i-lucide-unplug' }
            case 'partial':
                return { label: 'Some assets unavailable', color: 'warning', icon: 'i-lucide-triangle-alert' }
            case 'disabled':
                return { label: 'Disabled', color: 'neutral', icon: 'i-lucide-circle-slash' }
            default:
                return null
        }
    }

    /** Rollup drift state for a source (see {@link SourceDriftStatus}). */
    function sourceDrift(source: Source): SourceDriftStatus {
        if (source.status !== 'ok') return source.status
        if (source.assets.some(a => a.status === 'missing')) return 'partial'
        return 'ok'
    }

    /** Sources whose own key has drifted out of the catalog. */
    const missingSources = computed(() =>
        sourcesStore.sources.filter(s => s.status === 'missing'),
    )

    /** Live sources that have at least one drifted asset. */
    const partialSources = computed(() =>
        sourcesStore.sources.filter(s => s.status === 'ok' && s.assets.some(a => a.status === 'missing')),
    )

    /** Total count of individual assets whose key has drifted. */
    const missingAssetCount = computed(() =>
        sourcesStore.sources.reduce(
            (n, s) => n + s.assets.filter(a => a.status === 'missing').length,
            0,
        ),
    )

    /** Whether any removable drift (missing source or asset) exists. */
    const hasDrift = computed(() => missingSources.value.length > 0 || missingAssetCount.value > 0)

    return {
        statusBadge,
        sourceDrift,
        missingSources,
        partialSources,
        missingAssetCount,
        hasDrift,
    }
}
