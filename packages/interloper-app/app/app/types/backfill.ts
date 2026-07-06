export interface Backfill {
    id: string
    org_id: string
    /** Target component (backfills are job-only); null if the job was deleted. */
    component_id: string | null
    status: string
    start_date: string
    end_date: string
    concurrency: number
    fail_fast: boolean
    partitions: number
    started_at: string | null
    completed_at: string | null
    created_at: string | null
}
