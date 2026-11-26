/** A page of an asset's rows, as the rows API reads it. */
export interface AssetRows {
    /** The schema's columns when it declares one, else the data's. */
    columns: string[]
    rows: Record<string, unknown>[]
    /** The rows matching within what was read; exact unless `truncated`. */
    total: number
    /** More rows exist than were read: the read hit its cap, or, unsearched, rows follow the page. */
    truncated: boolean
    /** An editable asset's version, the hash of every row its rewrite must name; null for other assets. */
    version: string | null
}
