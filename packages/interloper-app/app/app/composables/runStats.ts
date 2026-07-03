import type { Ref } from 'vue'
import type { Run } from '~/types/run'
import type { AssetExecution, ExecutionStatus } from '~/types/asset_execution'

interface StatusMeta {
    key: string
    label: string
    statuses: ExecutionStatus[]
    /** Tailwind background class for the dot and the proportion-bar segment. */
    colorClass: string
}

/** Bucket order also drives the proportion-bar segment order and the legend. */
const STATUS_META: StatusMeta[] = [
    { key: 'success', label: 'Success', statuses: ['success'], colorClass: 'bg-green-500' },
    { key: 'running', label: 'Running', statuses: ['running'], colorClass: 'bg-blue-500' },
    { key: 'failed', label: 'Failed', statuses: ['failed'], colorClass: 'bg-red-500' },
    { key: 'canceled', label: 'Canceled', statuses: ['canceled'], colorClass: 'bg-amber-500' },
    { key: 'skipped', label: 'Skipped', statuses: ['skipped'], colorClass: 'bg-gray-500' },
    { key: 'pending', label: 'Pending', statuses: ['pending', 'queued'], colorClass: 'bg-gray-800' },
]

/** Always shown in the legend, even at zero. Pending only appears when present. */
const CORE_KEYS = new Set(['success', 'running', 'failed', 'canceled', 'skipped'])

/** Execution statuses grouped under a bucket key (e.g. pending → pending+queued). */
export function statusesForKey(key: string): ExecutionStatus[] {
    return STATUS_META.find(m => m.key === key)?.statuses ?? []
}

export interface RunStatusBucket {
    key: string
    label: string
    count: number
    colorClass: string
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

/** Derive per-status counts and run-level stats from the asset executions. */
export function useRunStats(run: Ref<Run | null>, executions: Ref<AssetExecution[]>) {
    return computed<RunStats>(() => {
        const counts: Record<string, number> = {}
        for (const ex of executions.value) counts[ex.status] = (counts[ex.status] ?? 0) + 1

        const total = executions.value.length
        const buckets: RunStatusBucket[] = STATUS_META.map((m) => {
            const count = m.statuses.reduce((sum, st) => sum + (counts[st] ?? 0), 0)
            return {
                key: m.key,
                label: m.label,
                count,
                colorClass: m.colorClass,
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
            duration: run.value?.started_at ? formatElapsed(run.value.started_at, run.value.completed_at) : null,
        }
    })
}
