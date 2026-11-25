import { h } from 'vue'
import { USkeleton } from '#components'
import type { TableColumn } from '@nuxt/ui'

/** A row a table draws as skeleton cells while its real rows load. */
export interface SkeletonRow {
    skeleton: true
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
 * aligned like the column's own cells, so the table keeps its shape while it
 * loads.
 */
export function withSkeletons<T>(columns: TableColumn<T>[]): TableColumn<T | SkeletonRow>[] {
    return columns.map((column) => {
        const cell = column.cell
        const right = String((column.meta as { class?: { td?: string } } | undefined)?.class?.td ?? '').includes('text-right')
        return {
            ...column,
            cell: (context: any) => {
                if (isSkeletonRow(context.row.original)) {
                    return h(USkeleton, { class: ['h-4 w-full max-w-28', right ? 'ml-auto' : ''] })
                }
                return typeof cell === 'function' ? cell(context) : context.getValue()
            },
        }
    }) as TableColumn<T | SkeletonRow>[]
}
