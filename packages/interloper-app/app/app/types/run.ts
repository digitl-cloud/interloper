export interface Run {
    id: string
    org_id: string
    /** Target component (any workload kind); null if the target was deleted. */
    component_id: string | null
    /** Target identity, resolved server-side on API and realtime records alike; null when deleted. */
    component_kind: string | null
    component_key: string | null
    component_name: string | null
    backfill_id: string | null
    partition_key: string | null
    status: string
    retry_of: string | null
    /** The stack this attempt belongs to; its own id for a first attempt. */
    root_run_id: string | null
    /**
     * This attempt's number. A listing carries each stack's latest attempt, so
     * there it is also how many attempts the stack took.
     */
    attempt: number
    retry_scope: string | null
    /** Earliest instant the queue may claim this run, set while a retry backs off. */
    scheduled_for: string | null
    started_at: string | null
    completed_at: string | null
    created_at: string | null
}
