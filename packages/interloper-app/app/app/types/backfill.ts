export interface Backfill {
    id: string
    org_id: string
    /** Target component (backfills are job-only); null if the job was deleted. */
    component_id: string | null
    /** Target identity, resolved server-side on API and realtime records alike; null when deleted. */
    component_kind: string | null
    component_key: string | null
    component_name: string | null
    status: string
    start_key: string
    end_key: string
    concurrency: number
    fail_fast: boolean
    partitions: number
    started_at: string | null
    completed_at: string | null
    created_at: string | null
}
