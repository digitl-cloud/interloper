import type { Ref } from 'vue'
import type { Run } from '~/types/run'
import type { Execution, ExecutionStatus } from '~/types/execution'

interface StatusMeta {
    key: string
    label: string
    /** Execution or run statuses the bucket gathers (a run is `dispatched` before it runs). */
    statuses: string[]
    /** Tailwind background class for the dot. */
    colorClass: string
    /** The same color as a CSS value, for components colored by prop (`UProgressGroup`). */
    color: string
}

/** Bucket order also drives the proportion-bar segment order and the legend. */
const STATUS_META: StatusMeta[] = [
    { key: 'success', label: 'Success', statuses: ['success'], colorClass: 'bg-green-500', color: 'var(--color-green-500)' },
    { key: 'running', label: 'Running', statuses: ['running', 'dispatched'], colorClass: 'bg-blue-500', color: 'var(--color-blue-500)' },
    { key: 'failed', label: 'Failed', statuses: ['failed'], colorClass: 'bg-red-500', color: 'var(--color-red-500)' },
    { key: 'canceled', label: 'Canceled', statuses: ['canceled'], colorClass: 'bg-amber-500', color: 'var(--color-amber-500)' },
    { key: 'skipped', label: 'Skipped', statuses: ['skipped'], colorClass: 'bg-gray-400', color: 'var(--color-gray-400)' },
    { key: 'pending', label: 'Pending', statuses: ['pending', 'queued'], colorClass: 'bg-gray-600', color: 'var(--color-gray-600)' },
]

/** Always shown in the legend, even at zero. Pending only appears when present. */
const CORE_KEYS = new Set(['success', 'running', 'failed', 'canceled', 'skipped'])

/** Statuses grouped under a bucket key (e.g. pending → pending+queued). */
export function statusesForKey(key: string): string[] {
    return STATUS_META.find(m => m.key === key)?.statuses ?? []
}

export interface RunStatusBucket {
    key: string
    label: string
    count: number
    colorClass: string
    color: string
    /** Share of the total run, 0–100. */
    percent: number
    /** Part of the always-on legend (shown even at zero). */
    core: boolean
}

export interface RunStats {
    total: number
    succeeded: number
    failed: number
    running: number
    /** Assets that produced nothing: canceled + skipped + pending/queued. */
    notRun: number
    buckets: RunStatusBucket[]
    /** Formatted run duration (elapsed so far when still running), or null. */
    duration: string | null
}

/** How many of a run's asset executions are in each status. */
export function executionCounts(executions: Execution[]): Record<string, number> {
    const counts: Record<string, number> = {}
    for (const ex of executions) counts[ex.status] = (counts[ex.status] ?? 0) + 1
    return counts
}

/** Bucketed, run-level stats from a run's execution count per status. */
export function runStats(run: Run | null, counts: Record<string, number>): RunStats {
    const total = Object.values(counts).reduce((sum, count) => sum + count, 0)
    const buckets: RunStatusBucket[] = STATUS_META.map((m) => {
        const count = m.statuses.reduce((sum, st) => sum + (counts[st] ?? 0), 0)
        return {
            key: m.key,
            label: m.label,
            count,
            colorClass: m.colorClass,
            color: m.color,
            percent: total ? (count / total) * 100 : 0,
            core: CORE_KEYS.has(m.key),
        }
    })
    const get = (k: string) => buckets.find(b => b.key === k)?.count ?? 0

    return {
        total,
        succeeded: get('success'),
        failed: get('failed'),
        running: get('running'),
        notRun: get('canceled') + get('skipped') + get('pending'),
        buckets,
        duration: run?.started_at ? formatElapsed(run.started_at, run.completed_at) : null,
    }
}

/** What happened to a run's assets, e.g. "3 succeeded · 2 failed · 5 not run". */
export function outcomeSummary(stats: RunStats): string[] {
    const { succeeded, running, failed, notRun } = stats
    return [
        succeeded ? `${succeeded} succeeded` : '',
        running ? `${running} running` : '',
        failed ? `${failed} failed` : '',
        notRun ? `${notRun} not run` : '',
    ].filter(Boolean)
}

/** A stats' non-empty buckets as `UProgressGroup` segments, in bucket order. */
export function progressSegments(stats: RunStats) {
    return stats.buckets.filter(b => b.count > 0).map(b => ({ value: b.count, color: b.color }))
}

/** Reactive {@link runStats} over a run and its asset executions. */
export function useRunStats(run: Ref<Run | null>, executions: Ref<Execution[]>) {
    return computed<RunStats>(() => runStats(run.value, executionCounts(executions.value)))
}

/** Dot/segment color class for an execution or run status. */
export function runStatusDotClass(status: string): string {
    return STATUS_META.find(m => (m.statuses as string[]).includes(status))?.colorClass ?? 'bg-gray-400'
}

/** The status bucket an execution status falls in (e.g. queued → pending). */
export function bucketKeyForStatus(status: ExecutionStatus): string {
    return STATUS_META.find(m => m.statuses.includes(status))?.key ?? status
}
