import type { Run } from '~/types/run'

export interface HourBucket {
    hour: string
    succeeded: number
    failed: number
}

export interface AttentionItem {
    kind: 'error_group' | 'connection' | 'run_stack' | 'drift' | 'overdue'
    severity: 'error' | 'warning'
    title: string
    target: string | null
    since: string | null
    component_id: string | null
    component_kind: string | null
    run_id: string | null
}

export interface UpcomingRun {
    job_id: string
    job_name: string
    next_run_at: string
    start_key: string | null
    end_key: string | null
}

export interface KindInventory {
    kind: string
    total: number
    healthy: number
    failing: number
    attention: number
    disabled: number
}

export interface Overview {
    generated_at: string
    runs: { total: number, succeeded: number, failed: number, hourly: HourBucket[] }
    activity: { running: number, queued: number, longest_running_seconds: number | null }
    backfills: { active: number, partitions_done: number, partitions_total: number }
    jobs: { enabled: number, failing: number }
    attention: AttentionItem[]
    upcoming: UpcomingRun[]
    recent: Run[]
    components: KindInventory[]
}

/**
 * Days as parallel arrays: index `i` of each array is the day `start + i`, from the first to the last day in the
 * window with partitions. A day inside that run without any is a zero. A day's partitions are its `successful`,
 * `failed` and `missing` ones; `missed` counts the snapshot periods that ended without a success, which are not among
 * them and no backfill can recover.
 */
export interface CoverageDays {
    start: string
    successful: number[]
    failed: number[]
    missing: number[]
    missed: number[]
}

/** A calendar group, a source with its assets or a standalone asset, and its days. */
export interface CoverageSource extends CoverageDays {
    id: string
    key: string
    name: string
    kind: 'source' | 'asset'
    failed_run_ids: Record<string, string>
}

/**
 * A type on the calendar, the sources of one definition or its standalone assets, with their days summed;
 * `troubled` counts the sources with a missing or failed partition in the window.
 */
export interface CoverageType extends CoverageDays {
    key: string
    sources: number
    troubled: number
}

export interface Coverage {
    since: string
    until: string
    types: CoverageType[]
}

export interface CoverageSources {
    since: string
    until: string
    sources: CoverageSource[]
}

/** One type's sources as the store holds them while the type is expanded. */
export interface TypeSources {
    /** `null` until the first answer arrives. */
    sources: CoverageSource[] | null
    loading: boolean
    error: Error | null
}

/** One source's partitions on one day, unrolled from its arrays. */
export interface CoverageDay {
    date: string
    source_id: string
    successful: number
    failed: number
    missing: number
    missed: number
    failed_run_id: string | null
}

export type CoverageMonths = 3 | 6 | 12
