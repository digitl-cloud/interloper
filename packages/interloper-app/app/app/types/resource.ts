export interface Resource {
    id: string
    org_id: string
    kind: string
    key: string
    name: string
    encrypted: boolean
    created_at: string | null
    updated_at: string | null
}

export interface ResourceDetail extends Resource {
    data: Record<string, any>
}

export interface ResourceInput {
    kind: string
    key: string
    name: string
    data: Record<string, any>
    encrypted?: boolean
}
