export interface Run {
    id: string
    org_id: string
    component_id: string | null
    component_kind: string | null
    component_key: string | null
    component_name: string | null
    backfill_id: string | null
    partition_key: string | null
    status: string
    retry_of: string | null
    root_run_id: string | null
    attempt: number
    retry_scope: string | null
    scheduled_for: string | null
    started_at: string | null
    completed_at: string | null
    created_at: string | null
}
