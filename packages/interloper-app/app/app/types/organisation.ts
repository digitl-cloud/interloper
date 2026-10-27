export interface Organisation {
    id: string
    name: string
    slug: string
    created_at: string | null
}

/** An organisation the user acts in, with the role they act with; a super-admin acts as `admin` in every one. */
export interface Membership {
    id: string
    name: string
    slug: string
    role: string
}

export interface Member {
    id: string
    email: string
    name: string | null
    avatar_url: string | null
    role: string
}

export interface Invitation {
    id: string
    email: string
    role: string
    created_at: string | null
    expires_at: string
}

/** A members-table row: an active member or a pending invitation. */
export interface OrgMember {
    id: string
    email: string
    name: string | null
    avatar_url: string | null
    role: string
    status: 'active' | 'invited'
}
