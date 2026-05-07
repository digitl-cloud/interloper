export interface Run {
    id: string
    org_id: string
    job_id: string | null
    backfill_id: string | null
    partition_date: string | null
    status: string
    started_at: string | null
    completed_at: string | null
    created_at: string | null
    job: { id: string; name: string } | null
}
