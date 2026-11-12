import type { ComponentRecord } from '~/types/component'
import type { Execution } from '~/types/execution'
import { stateFromExecution } from '~/types/graph'
import type { NodeStatus, GraphNodeState } from '~/types/graph'

/**
 * The status-table colour of a node state. The collection-level states sit on
 * the same colours: healthy (idle) reads as success, attention as warning,
 * paused as neutral; the execution states are the status table's own.
 */
export function nodeStateColor(state: GraphNodeState): StatusColor {
    switch (state) {
        case 'idle': return 'success'
        case 'attention': return 'warning'
        case 'paused': return 'neutral'
        default: return statusMeta(state).color
    }
}

/** Whether a node state is still executing (its dot pulses). */
export function nodeStateExecuting(state: GraphNodeState): boolean {
    return statusMeta(state).executing
}

/**
 * Border tint of a node: only states that demand attention or are live tint
 * it; the rest keep the card border, their dot is enough.
 */
const STATUS_BORDER: Partial<Record<GraphNodeState, string>> = {
    attention: 'border-warning/50',
    running: 'border-info/50',
    failed: 'border-error/50',
}

export function statusBorderClass(state: GraphNodeState): string {
    return STATUS_BORDER[state] ?? ''
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
        const name = statusMeta(execution.status).label
        return at ? `${name} · ${relativeTime(new Date(at))}` : name
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
