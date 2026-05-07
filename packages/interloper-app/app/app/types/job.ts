export interface Job {
    id: string
    org_id: string
    name: string
    cron: string
    tags: string[]
    enabled: boolean
    partitioned: boolean
    backfill_days: number | null
    source_ids: string[]
    asset_ids: string[]
    last_run_at: string | null
    next_run_at: string | null
    created_at: string | null
}

export interface JobInput {
    name: string
    cron: string
    source_ids?: string[]
    asset_ids?: string[]
    tags?: string[]
    enabled?: boolean
    partitioned?: boolean
    backfill_days?: number | null
}
