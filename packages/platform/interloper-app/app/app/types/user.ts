import type { Membership } from './organisation'

export interface User {
    id: string
    email: string
    name: string | null
    avatar_url: string | null
    timezone: string | null
    is_super_admin: boolean
    login_providers: LoginProvider[]
    memberships: Membership[]
    last_organisation_id: string | null
    features: Record<string, boolean>
}

/** A sign-in provider the deployment offers. */
export interface LoginProvider {
    name: string
    label: string
}

/** A way the caller signs in; `provider` is null when the deployment no longer offers it. */
export interface Identity {
    id: string
    provider: LoginProvider | null
    created_at: string | null
}
