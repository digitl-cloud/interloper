import type { Membership } from './organisation'

export interface User {
    id: string
    email: string
    name: string | null
    avatar_url: string | null
    timezone: string | null
    is_super_admin: boolean
    memberships: Membership[]
    last_organisation_id: string | null
    features: Record<string, boolean>
}
