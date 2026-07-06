export interface Run {
    id: string
    org_id: string
    /** Target component (job, source, or asset); null if the target was deleted. */
    component_id: string | null
    backfill_id: string | null
    partition_date: string | null
    status: string
    retry_of: string | null
    attempt: number
    retry_scope: string | null
    started_at: string | null
    completed_at: string | null
    created_at: string | null
}
