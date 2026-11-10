import type { ComponentRecord } from '~/types/component'
import type { Execution } from '~/types/execution'
import { stateFromExecution } from '~/types/graph'
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

/**
 * Border tint per node state. Healthy (idle) and the gray states keep the
 * default card border — their dot is enough; tints are reserved for states
 * that demand attention or are live.
 */
const STATUS_BORDER: Record<GraphNodeState, string> = {
    idle: '',
    attention: 'border-warning/55',
    paused: '',
    queued: '',
    pending: '',
    running: 'border-info/55',
    success: 'border-success/50',
    failed: 'border-error/45',
    skipped: '',
    canceled: '',
}

export function statusBorderClass(state: GraphNodeState): string {
    return STATUS_BORDER[state]
}

/** Rollup precedence for a container's dot: the loudest child state wins. */
const ROLLUP: GraphNodeState[] = ['failed', 'running', 'queued', 'canceled', 'success']

/** The state a container shows for its children: the loudest one present, `pending` when none ran. */
export function rollupState(states: Iterable<GraphNodeState>): GraphNodeState {
    const present = new Set(states)
    return ROLLUP.find(s => present.has(s)) ?? 'pending'
}

/**
 * Materialization status for the collection graph's dots, from each asset's
 * latest execution ({@link useExecutionsStore}). An asset that never ran is
 * `pending`; a source rolls its assets up, loudest state first.
 */
export function useMaterializationStatus() {
    const executionsStore = useExecutionsStore()

    function label(execution: Execution): string {
        const at = execution.completed_at ?? execution.started_at ?? execution.created_at
        const name = statusLabel(execution.status)
        return at ? `${name} · ${timeSince(new Date(at))} ago` : name
    }

    function assetStatus(assetId: string): NodeStatus {
        const execution = executionsStore.latestByAssetId.get(assetId)
        if (!execution) return { state: 'pending' }
        return { state: stateFromExecution(execution.status), label: label(execution) }
    }

    function sourceStatus(source: ComponentRecord): NodeStatus {
        return { state: rollupState(source.children.map(a => assetStatus(a.id).state)) }
    }

    return { assetStatus, sourceStatus }
}

/**
 * Derives the configuration-side {@link NodeStatus} of collection nodes from
 * warnings ({@link useAssetWarnings}) and job enablement ({@link useSchedule}):
 * what the toolbar's status filter counts and narrows by.
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
