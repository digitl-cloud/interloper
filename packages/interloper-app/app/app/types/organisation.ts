export interface Organisation {
    id: string
    name: string
    created_at: string | null
}

export interface OrgMember {
    id: string
    email: string
    name: string | null
    avatar_url: string | null
    role: string
    status: 'active' | 'invited'
}
