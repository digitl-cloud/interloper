import type { Destination } from './destination'

export interface SourceAsset {
    id: string
    key: string
    materializable: boolean
}

export interface Source {
    id: string
    org_id: string
    key: string
    name: string
    config: Record<string, any> | null
    resources: Record<string, string>
    destinations: Destination[]
    assets: SourceAsset[]
    created_at: string | null
}

export interface SourceInput {
    key: string
    name: string
    config?: Record<string, any>
    resources?: Record<string, string>
    asset_keys?: string[]
    destination_ids?: string[]
    /** Cross-source deps: { asset_key: { param_name: upstream_asset_id } } */
    cross_deps?: Record<string, Record<string, string>>
}
