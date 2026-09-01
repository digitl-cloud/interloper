export interface Run {
    id: string
    org_id: string
    /** Target component (any workload kind); null if the target was deleted. */
    component_id: string | null
    /** Target identity, resolved server-side; null when deleted, absent on realtime partials. */
    component_kind?: string | null
    component_key?: string | null
    component_name?: string | null
    backfill_id: string | null
    partition_key: string | null
    status: string
    retry_of: string | null
    attempt: number
    retry_scope: string | null
    started_at: string | null
    completed_at: string | null
    created_at: string | null
}
