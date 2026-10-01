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

export interface CoverageDay {
    date: string
    job_id: string
    expected: number
    covered: number
    failed: number
    failed_run_id: string | null
}

export interface Coverage {
    since: string
    until: string
    jobs: { id: string, name: string }[]
    days: CoverageDay[]
}

export type CoverageMonths = 3 | 6 | 12
