import type { Source } from '~/types/source'
import type { NodeStatus } from '~/types/graph'

/**
 * Derives {@link NodeStatus} for catalog graph nodes from data that
 * actually exists today: configuration warnings ({@link useAssetWarnings})
 * and job enablement ({@link useSchedule}).
 *
 * The catalog page has no per-asset *run* status loaded, so live states
 * (running/success/failed) are intentionally absent here — those are
 * supplied by the run-page model from asset executions instead.
 */
export function useNodeStatus() {
    const { getWarnings } = useAssetWarnings()
    const { getSourceSchedule } = useSchedule()

    function assetStatus(assetId: string, assetKey: string): NodeStatus {
        return getWarnings(assetId, assetKey).length > 0
            ? { state: 'attention' }
            : { state: 'idle' }
    }

    function sourceStatus(source: Source): NodeStatus {
        const schedule = getSourceSchedule(source)
        if (schedule?.paused) return { state: 'paused' }
        const hasWarning = source.assets.some(a => getWarnings(a.id, a.key).length > 0)
        return hasWarning ? { state: 'attention' } : { state: 'idle' }
    }

    return { assetStatus, sourceStatus }
}
