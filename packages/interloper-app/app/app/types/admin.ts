export interface AdminOrganisation {
    id: string
    name: string
    member_count: number
    created_at: string | null
    /** Soft-deleted orgs stay listed for billing history but are read-only. */
    deleted_at: string | null
}

export interface AdminUserOrganisation {
    id: string
    name: string
}

export interface AdminConfig {
    deployment: {
        version: string | null
        launcher: {
            type: string
            config: Record<string, unknown>
            defaults: Record<string, unknown>
        }
        runner: {
            type: string
            config: Record<string, unknown>
            defaults: Record<string, unknown>
        }
        features: Record<string, boolean>
        agent_model: string | null
    }
    auth: {
        allowed_domains: string[]
        super_admin_emails: string[]
        google_oauth_configured: boolean
        google_redirect_uri: string
        session_expiry_days: number
        cookie_secure: boolean
    }
    services: {
        cron: { enabled: boolean, reconcile_interval: number, batch_size: number, max_execution_delay: number | null }
        worker: { enabled: boolean, poll_interval: number }
        reaper: { enabled: boolean, timeout: number, poll_interval: number }
        smtp: { enabled: boolean, host: string, from_addr: string }
        mcp_external_url: string
    }
    data: {
        encryption_configured: boolean
        catalog: Record<string, string[]>
    }
    quotas: AdminQuotaLimits
}

export interface AdminQuotaLimits {
    max_sources: number | null
    max_assets_per_source: number | null
    max_successful_runs_per_month: number | null
    max_backfill_days: number | null
}

export interface AdminOrgQuotaStatus {
    id: string
    name: string
    /** Soft-deleted orgs keep their ledger visible but are read-only. */
    deleted_at: string | null
    limits: AdminQuotaLimits
    effective: AdminQuotaLimits
    sources: number
    max_assets_per_source: number
    successful_runs: number
    reserved_runs: number
    /** Recomputed from the runs table; differing from successful_runs signals ledger drift. */
    recomputed_successful_runs: number
}

/** One quota's display descriptor: key, registry label, instance default. */
export interface AdminQuotaField {
    key: keyof AdminQuotaLimits
    label: string
    default: number | null
}

export interface AdminQuotas {
    period_start: string
    defaults: AdminQuotaLimits
    fields: AdminQuotaField[]
    organisations: AdminOrgQuotaStatus[]
}

export interface AdminActivityEntry {
    kind: string
    when: string
    title: string
    detail: string | null
}

export interface AdminUser {
    id: string
    email: string
    name: string | null
    avatar_url: string | null
    is_super_admin: boolean
    organisations: AdminUserOrganisation[]
    created_at: string | null
}
