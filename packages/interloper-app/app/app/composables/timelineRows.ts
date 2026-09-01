import type { ComputedRef, MaybeRefOrGetter } from 'vue'
import type { Execution, ExecutionStatus } from '~/types/execution'
import type { Run } from '~/types/run'
import type { TimelineBar, TimelineRow } from '~/types/timeline'

/**
 * Row builders for `ChartExecutionTimeline`. The component renders lanes in the
 * order it is given, so each view decides here what a lane is: an asset of one
 * run, or a job across a window of wall-clock time.
 */

const DEFAULT_ICON = 'i-lucide-box'
const JOB_ICON = 'i-lucide-calendar-clock'
const DELETED_ICON = 'i-lucide-circle-slash'
const DELETED_LABEL = 'Deleted target'

/** Sort weight: assets that ran sort first, non-started assets sink to the bottom. */
const STATUS_WEIGHT: Record<string, number> = {
    success: 0,
    running: 1,
    failed: 2,
    canceled: 3,
    skipped: 4,
    queued: 5,
    pending: 5,
}

/** Statuses that will never advance — the bar they carry is closed. */
const TERMINAL_STATUSES = new Set(['success', 'failed', 'canceled', 'skipped'])

function byName(a: TimelineRow, b: TimelineRow): number {
    return a.name.localeCompare(b.name)
}

/** Absolute bounds of an execution, or null when it never started. */
function bounds(
    status: string,
    startedAt: string | null,
    completedAt: string | null,
): { start: number, end: number | null } | null {
    if (!startedAt) return null
    const start = new Date(startedAt).getTime()
    const completed = completedAt ? new Date(completedAt).getTime() : null
    // Still in flight → open-ended, so the bar grows with the clock.
    return { start, end: completed ?? (TERMINAL_STATUSES.has(status) ? start : null) }
}

/** One row per asset execution of a single run, ordered as its timeline reads best. */
export function useExecutionRows(
    executions: MaybeRefOrGetter<Execution[]>,
): ComputedRef<TimelineRow[]> {
    const assetDisplayName = useAssetDisplayName()
    const assetIcon = useAssetIcon()

    return computed(() => toValue(executions)
        .map((execution) => {
            const interval = bounds(execution.status, execution.started_at, execution.completed_at)
            const names = execution.component_id ? assetDisplayName.value.get(execution.component_id) : undefined
            return {
                id: execution.component_id,
                name: names?.label ?? execution.component_key,
                icon: (execution.component_id ? assetIcon.value.get(execution.component_id) : undefined) ?? DEFAULT_ICON,
                status: execution.status,
                bars: interval
                    ? [{ id: `${execution.run_id}:${execution.component_key}`, status: execution.status, ...interval }]
                    : [],
            }
        })
        .sort((a, b) => {
            const wa = STATUS_WEIGHT[a.status] ?? 9
            const wb = STATUS_WEIGHT[b.status] ?? 9
            if (wa !== wb) return wa - wb
            return (a.bars[0]?.start ?? Infinity) - (b.bars[0]?.start ?? Infinity)
        }))
}

/**
 * One row per job, carrying every run of that job in the given set — the
 * scheduling view of execution history. Jobs without a run in the window keep
 * their (empty) row so the schedule still reads; runs launched straight at a
 * source or asset get their own rows after the jobs rather than being dropped.
 */
export function useRunTimelineRows(runs: MaybeRefOrGetter<Run[]>): ComputedRef<TimelineRow[]> {
    const componentsStore = useComponentsStore()
    const assetDisplayName = useAssetDisplayName()
    const assetIcon = useAssetIcon()

    return computed(() => {
        const barsByTarget = new Map<string, TimelineBar[]>()
        const orphaned: TimelineBar[] = []

        for (const run of toValue(runs)) {
            const interval = bounds(run.status, run.started_at, run.completed_at)
            if (!interval) continue
            const bar: TimelineBar = {
                id: run.id,
                status: run.status as ExecutionStatus,
                ...interval,
                detail: run.partition_key ?? undefined,
            }
            const target = run.component_id && componentsStore.byId(run.component_id) ? run.component_id : null
            if (!target) {
                orphaned.push(bar)
                continue
            }
            const existing = barsByTarget.get(target)
            if (existing) existing.push(bar)
            else barsByTarget.set(target, [bar])
        }

        const jobs = componentsStore.byKind('job')
        const jobRows = jobs
            .map(job => ({
                id: job.id,
                name: job.name ?? job.key,
                icon: JOB_ICON,
                bars: barsByTarget.get(job.id) ?? [],
            }))
            .sort(byName)

        const jobIds = new Set(jobs.map(job => job.id))
        const adHocRows: TimelineRow[] = []
        for (const [id, bars] of barsByTarget) {
            if (jobIds.has(id)) continue
            const component = componentsStore.byId(id)!
            const names = component.kind === 'asset' ? assetDisplayName.value.get(id) : undefined
            adHocRows.push({
                id,
                name: names?.label ?? component.name ?? component.key,
                icon: component.kind === 'asset'
                    ? assetIcon.value.get(id) ?? DEFAULT_ICON
                    : componentIcon(component.key),
                bars,
            })
        }

        return [
            ...jobRows,
            ...adHocRows.sort(byName),
            ...(orphaned.length ? [{ id: null, name: DELETED_LABEL, icon: DELETED_ICON, bars: orphaned }] : []),
        ]
    })
}
