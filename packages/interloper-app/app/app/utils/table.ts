import { h } from 'vue'
import { UButton } from '#components'
import type { Column } from '@tanstack/vue-table'
import type { TableColumn } from '@nuxt/ui'

/**
 * Builds a clickable column header that toggles sorting (none → asc → desc)
 * and reflects the current state with an arrow icon. Use as a column's
 * `header` to make it sortable.
 */
export function sortableHeader<T>(label: string) {
    return ({ column }: { column: Column<T> }) => {
        const sorted = column.getIsSorted()
        const icon = sorted === 'asc'
            ? 'i-lucide-arrow-up-narrow-wide'
            : sorted === 'desc'
                ? 'i-lucide-arrow-down-wide-narrow'
                : 'i-lucide-arrow-up-down'
        return h(UButton, {
            color: 'neutral',
            variant: 'ghost',
            size: 'sm',
            label,
            icon,
            trailing: true,
            class: '-mx-2.5 data-[state=open]:bg-elevated',
            'aria-label': `Sort by ${label}`,
            onClick: () => column.toggleSorting(column.getIsSorted() === 'asc'),
        })
    }
}

/**
 * Returns a copy of `columns` with sortable headers applied to every column
 * that (a) has a real accessor to sort on, (b) has a plain string header, and
 * (c) hasn't opted out via `enableSorting: false`. Display-only columns
 * (selection checkbox, avatar, actions, custom-rendered headers) pass through
 * untouched.
 */
export function withSortableHeaders<T>(columns: TableColumn<T>[]): TableColumn<T>[] {
    return columns.map((column) => {
        const hasAccessor = 'accessorKey' in column || 'accessorFn' in column
        if (!hasAccessor) return column
        if (typeof column.header !== 'string' || column.header === '') return column
        if (column.enableSorting === false) return column
        // Only the header is overridden; cast back since the spread widens the
        // discriminated `TableColumn` union (its `id` becomes optional).
        return { ...column, header: sortableHeader<T>(column.header) } as TableColumn<T>
    })
}
