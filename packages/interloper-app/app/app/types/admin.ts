export interface AdminOrganisation {
    id: string
    name: string
    member_count: number
    created_at: string | null
}

export interface AdminUser {
    id: string
    email: string
    name: string | null
    avatar_url: string | null
    is_super_admin: boolean
    organisation_count: number
    created_at: string | null
}
