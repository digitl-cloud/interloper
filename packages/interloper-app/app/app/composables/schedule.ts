import cronstrue from 'cronstrue'
import type { ComponentRecord } from '~/types/component'
import { jobCron, jobEnabled, jobNextRunAt, jobTargetIds } from '~/types/component'

/** Human-readable cron, e.g. "Daily at 06:00". Falls back to the raw expression. */
export function cronLabel(cron: string): string {
    try {
        return cronstrue.toString(cron, { use24HourTimeFormat: true })
    }
    catch {
        return cron
    }
}

const DOW = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']

/**
 * Compact cadence label for common crons, e.g. "Daily · 06:00", "Hourly",
 * "Weekly · Mon 06:00". Falls back to {@link cronLabel} for anything exotic.
 */
export function scheduleSummary(cron: string): string {
    const parts = cron.trim().split(/\s+/)
    if (parts.length !== 5) return cronLabel(cron)

    const [min, hour, dom, mon, dow] = parts as [string, string, string, string, string]
    const num = (s: string) => (/^\d+$/.test(s) ? Number(s) : null)
    const hh = num(hour)
    const mm = num(min)
    const time = hh !== null && mm !== null
        ? `${String(hh).padStart(2, '0')}:${String(mm).padStart(2, '0')}`
        : null

    if (dom === '*' && mon === '*') {
        if (dow === '*') {
            if (hour === '*' && mm === 0) return 'Hourly'
            if (time) return `Daily · ${time}`
        }
        else if (num(dow) !== null && time) {
            return `Weekly · ${DOW[num(dow)!] ?? dow} ${time}`
        }
    }
    if (num(dom) !== null && mon === '*' && dow === '*' && time) {
        return `Monthly · ${dom} ${time}`
    }
    return cronLabel(cron)
}

export interface SourceSchedule {
    /** Display label: a single cron description, or "Multiple schedules". */
    label: string
    /** All referencing jobs are disabled. */
    paused: boolean
    /** Number of jobs that schedule this source. */
    jobCount: number
    nextRunAt: string | null
}

/**
 * Schedule helpers derived from jobs. A source carries no schedule of its
 * own — schedules live on jobs, which may reference many sources — so we
 * summarise the job(s) that reference a given source.
 */
export function useSchedule() {
    const componentsStore = useComponentsStore()

    function jobsForSource(source: ComponentRecord): ComponentRecord[] {
        return componentsStore.byKind('job').filter(j => jobTargetIds(j, 'source').includes(source.id))
    }

    /** Schedule summary for a source, or null when no job references it. */
    function getSourceSchedule(source: ComponentRecord): SourceSchedule | null {
        const jobs = jobsForSource(source)
        if (jobs.length === 0) return null

        const paused = jobs.every(j => !jobEnabled(j))
        const nextRunAt = jobs
            .map(j => jobNextRunAt(j))
            .filter((d): d is string => !!d)
            .sort()[0] ?? null

        if (jobs.length === 1) {
            return { label: scheduleSummary(jobCron(jobs[0]!)), paused, jobCount: 1, nextRunAt }
        }
        return { label: 'Multiple schedules', paused, jobCount: jobs.length, nextRunAt }
    }

    return { cronLabel, jobsForSource, getSourceSchedule }
}
