export type ExecutionStatus = 'pending' | 'success' | 'failed' | 'canceled' | 'running' | 'queued' | 'skipped'

export interface Execution {
    run_id: string
    org_id: string
    component_id: string | null
    component_key: string
    status: ExecutionStatus
    started_at: string | null
    completed_at: string | null
    created_at: string | null
}
