import type { Destination } from './destination'

export interface Asset {
    id: string
    source_id: string | null
    org_id: string
    key: string
    materializable: boolean
    config: Record<string, any> | null
    resources: Record<string, string>
    destinations: Destination[]
    created_at: string | null
}

export interface AssetDependency {
    asset_id: string
    upstream_asset_id: string
}

export interface AssetCreateInput {
    key: string
    config?: Record<string, any>
    resources?: Record<string, string>
    destination_ids?: string[]
}

export interface AssetUpdateInput {
    materializable?: boolean
    config?: Record<string, any>
    resources?: Record<string, string>
    destination_ids?: string[]
}
