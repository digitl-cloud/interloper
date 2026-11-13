export interface AdminOrganisation {
    id: string
    name: string
    key: string
    member_count: number
    created_at: string | null
    /** Soft-deleted orgs stay listed for billing history but are read-only. */
    deleted_at: string | null
}

export interface AdminUserOrganisation {
    id: string
    name: string
}

/**
 * One field of a settings section, or one public key of a mapping field
 * (`config.image`). `value` is set only when the section lists the field as
 * public; `configured` says whether the deployment changed it from its default.
 */
export interface AdminSettingsField {
    name: string
    description: string | null
    public: boolean
    configured: boolean
    value: unknown
}

/** One settings section a package declares, as the running instance loaded it. */
export interface AdminSettingsSection {
    name: string
    /** Key of the extension declaring the section (`core`, `db`, `scheduler`, ...). */
    extension: string
    description: string | null
    fields: AdminSettingsField[]
}

/** Something an installed package declares that was skipped: its manifest, a section or an entry. */
export interface AdminExtensionFailure {
    package: string
    /** `null` when the whole manifest was skipped. */
    section: string | null
    /** `null` when a whole section or manifest was skipped. */
    entry: string | null
    error: string
}

/** Where an extension's UI is loaded from, as the API serves it. */
export interface ExtensionUI {
    /** URL of the entry module. */
    entry: string
    /** URLs of the stylesheets the entry needs. */
    css: string[]
}

/** One installed package that ships an extension manifest. */
export interface AdminExtension {
    package: string
    /** `null` when its manifest could not be read. */
    key: string | null
    name: string
    description: string
    icon: string
    version: string | null
    /** `failed` when anything it declares was skipped. */
    status: 'ok' | 'failed'
    /** `null` when it ships no UI, or its UI failed to load. */
    ui: ExtensionUI | null
    failures: AdminExtensionFailure[]
}

export interface AdminConfig {
    version: string | null
    features: Record<string, boolean>
    catalog: Record<string, string[]>
    sections: AdminSettingsSection[]
    extensions: AdminExtension[]
}

/**
 * Quota limits keyed by quota key; null means unlimited (or unset, for
 * overrides). Keyed rather than declared field-by-field because the API
 * derives the payload from its quota registry: a new quota arrives here
 * without a matching type edit, and the drawer already indexes by key.
 */
export type AdminQuotaLimits = Record<string, number | null>

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
    key: string
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

/** An OAuth client an operator registered for the instance; `id` is its client id. Never carries the secret. */
export interface AdminOAuthClient {
    id: string
    name: string
    redirect_uris: string[]
    created_at: string | null
}

/** Creation response: the only place the client secret ever appears. */
export interface CreatedOAuthClient extends AdminOAuthClient {
    client_secret: string
}
