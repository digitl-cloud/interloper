import { h } from 'vue'
import { USkeleton } from '#components'
import type { TableColumn } from '@nuxt/ui'

/** A row a table draws as skeleton cells while its real rows load. */
export interface SkeletonRow {
    skeleton: true
}

/** A table row in the theme: a 20px line between 12px paddings, and the 1px rule every row draws. */
const TABLE_ROW_HEIGHT = 44
const TABLE_RULE = 1
/** The padding Nuxt UI's table puts above and below the cell that holds its empty state. */
const EMPTY_CELL_PADDING = 48

/**
 * The least height of an empty state in a table that drew `count` skeleton rows: the rows' room
 * less the empty row's own padding and rule, so the table keeps its height when it lands empty.
 */
export function skeletonEmptyHeight(count: number): number {
    return count * (TABLE_ROW_HEIGHT + TABLE_RULE) - EMPTY_CELL_PADDING - TABLE_RULE
}

export function skeletonRows(count: number): SkeletonRow[] {
    return Array.from({ length: count }, () => ({ skeleton: true }))
}

export function isSkeletonRow(row: unknown): row is SkeletonRow {
    return typeof row === 'object' && row !== null && (row as Partial<SkeletonRow>).skeleton === true
}

/**
 * A table's row id that tells a skeleton row from a loaded one, so the loaded rows replace the
 * skeleton's rather than patching them in place, and fade in as a table's new rows do.
 */
export function skeletonRowId(row: unknown, index: number): string {
    return isSkeletonRow(row) ? `skeleton-${index}` : String(index)
}

/**
 * The same columns, drawing a skeleton bar in each cell of a placeholder row,
 * aligned like the column's own cells and as tall as a line of their text, so
 * the table keeps its shape while it loads and its rows' height once loaded.
 */
export function withSkeletons<T>(columns: TableColumn<T>[]): TableColumn<T | SkeletonRow>[] {
    return columns.map((column) => {
        const cell = column.cell
        const right = String((column.meta as { class?: { td?: string } } | undefined)?.class?.td ?? '').includes('text-right')
        return {
            ...column,
            cell: (context: any) => {
                if (isSkeletonRow(context.row.original)) {
                    return h(USkeleton, { class: ['h-5 w-full max-w-28', right ? 'ml-auto' : ''] })
                }
                return typeof cell === 'function' ? cell(context) : context.getValue()
            },
        }
    }) as TableColumn<T | SkeletonRow>[]
}
