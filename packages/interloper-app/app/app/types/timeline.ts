import type { ExecutionStatus } from '~/types/asset_execution'

/** One execution drawn as a bar on a timeline row. */
export interface TimelineBar {
    /** Identity of the execution itself (e.g. a run id); carried back on click. */
    id: string
    status: ExecutionStatus
    /** Absolute epoch ms. */
    start: number
    /** Absolute epoch ms, or null while still running — the bar then grows with the clock. */
    end: number | null
    /** Extra tooltip context, e.g. a partition date. */
    detail?: string
}

/**
 * One lane of a timeline: an entity and the executions it had in view. Rows
 * render in the given order — the caller owns sorting and grouping.
 */
export interface TimelineRow {
    /** Identity of the entity (asset, job, …); null when it no longer exists. */
    id: string | null
    name: string
    icon: string
    /** Rendered by the placeholder of a row with nothing to draw. */
    status?: ExecutionStatus
    bars: TimelineBar[]
}
