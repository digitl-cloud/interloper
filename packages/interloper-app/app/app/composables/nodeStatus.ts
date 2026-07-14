import type { ComponentRecord } from '~/types/component'
import type { NodeStatus, GraphNodeState } from '~/types/graph'

/** Tailwind class for a status indicator dot, keyed by node state. */
const STATUS_DOT: Record<GraphNodeState, string> = {
    idle: 'bg-[var(--ui-success)]',
    attention: 'bg-[var(--ui-warning)]',
    paused: 'bg-[var(--ui-text-dimmed)]',
    queued: 'bg-[var(--ui-text-dimmed)]',
    pending: 'bg-[var(--ui-text-dimmed)]',
    running: 'bg-[var(--ui-info)] animate-pulse',
    success: 'bg-[var(--ui-success)]',
    failed: 'bg-[var(--ui-error)]',
    skipped: 'bg-[var(--ui-text-dimmed)]',
    canceled: 'bg-[var(--ui-text-dimmed)]',
}

export function statusDotClass(state: GraphNodeState): string {
    return STATUS_DOT[state]
}

/** Ring/border tint applied to a node in the Status view mode. */
const STATUS_RING: Record<GraphNodeState, string> = {
    idle: 'ring-2 ring-[var(--ui-success)]/40',
    attention: 'ring-2 ring-[var(--ui-warning)]/70',
    paused: 'ring-2 ring-[var(--ui-text-dimmed)]/40',
    queued: 'ring-2 ring-[var(--ui-text-dimmed)]/40',
    pending: 'ring-2 ring-[var(--ui-text-dimmed)]/40',
    running: 'ring-2 ring-[var(--ui-info)]/70',
    success: 'ring-2 ring-[var(--ui-success)]/60',
    failed: 'ring-2 ring-[var(--ui-error)]/70',
    skipped: 'ring-2 ring-[var(--ui-text-dimmed)]/40',
    canceled: 'ring-2 ring-[var(--ui-text-dimmed)]/40',
}

export function statusRingClass(state: GraphNodeState): string {
    return STATUS_RING[state]
}

/**
 * Derives {@link NodeStatus} for collection graph nodes from data that
 * actually exists today: configuration warnings ({@link useAssetWarnings})
 * and job enablement ({@link useSchedule}).
 *
 * The collection page has no per-asset *run* status loaded, so live states
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

    function sourceStatus(source: ComponentRecord): NodeStatus {
        const schedule = getSourceSchedule(source)
        if (schedule?.paused) return { state: 'paused' }
        const hasWarning = source.children.some(a => getWarnings(a.id, a.key).length > 0)
        return hasWarning ? { state: 'attention' } : { state: 'idle' }
    }

    return { assetStatus, sourceStatus }
}
