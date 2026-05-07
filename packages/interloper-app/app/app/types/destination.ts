export interface Destination {
    id: string
    key: string
    name: string
    config: Record<string, any> | null
    resources: Record<string, string>
    created_at: string | null
}

export interface DestinationInput {
    key: string
    name?: string
    config?: Record<string, any>
    resources?: Record<string, string>
}
