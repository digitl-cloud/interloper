import type { AdminOrgQuotaStatus } from '~/types/admin'

export interface QuotaPeak {
    label: string
    used: number
    limit: number
    pct: number
}

/** The organisation's quota closest to its ceiling; null when none of its quotas is limited. */
export function peakQuota(usage: AdminOrgQuotaStatus): QuotaPeak | null {
    const candidates = [
        { label: 'Sources', used: usage.sources, limit: usage.effective.max_sources },
        { label: 'Assets / source', used: usage.max_assets_per_source, limit: usage.effective.max_assets_per_source },
        { label: 'Runs', used: usage.successful_runs, limit: usage.effective.max_successful_runs_per_month },
    ].filter((entry): entry is { label: string, used: number, limit: number } =>
        entry.limit != null && entry.limit > 0)
    if (!candidates.length) return null
    const peak = candidates.reduce((a, b) => (b.used / b.limit > a.used / a.limit ? b : a))
    return { ...peak, pct: Math.round((peak.used / peak.limit) * 100) }
}
