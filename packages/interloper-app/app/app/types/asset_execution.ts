export type ExecutionStatus = 'pending' | 'success' | 'failed' | 'canceled' | 'running' | 'queued' | 'skipped'

export interface AssetExecution {
    run_id: string
    org_id: string
    asset_id: string | null
    asset_key: string
    status: ExecutionStatus
    started_at: string | null
    completed_at: string | null
    created_at: string | null
}
