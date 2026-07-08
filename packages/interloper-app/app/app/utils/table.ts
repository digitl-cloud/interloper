import { h } from 'vue'
import { UButton } from '#components'
import type { Column } from '@tanstack/vue-table'
import type { TableColumn } from '@nuxt/ui'
import { stateColumns, type ComponentDefinition } from '~/types/catalog'
import type { ComponentRecord } from '~/types/component'
import { formatDate } from '~/utils/time'

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
 * Table columns for a component row's `state`, derived from its definition's
 * `state_schema`. Datetime values render via `formatDate`; `_id` values as a
 * short monospace id.
 */
export function stateSchemaColumns(defn: ComponentDefinition | undefined): TableColumn<ComponentRecord>[] {
    if (!defn) return []
    return stateColumns(defn).map((col) => {
        if (col.format === 'datetime') {
            return {
                accessorKey: col.key,
                header: col.label,
                accessorFn: (row: ComponentRecord) => row.state?.[col.key] ? formatDate(row.state[col.key]) : '—',
            } as TableColumn<ComponentRecord>
        }
        return {
            accessorKey: col.key,
            header: col.label,
            accessorFn: (row: ComponentRecord) => row.state?.[col.key] ?? '',
            cell: ({ row }: { row: { original: ComponentRecord } }) => {
                const value = row.original.state?.[col.key]
                if (value == null || value === '') return '—'
                const text = String(value)
                if (col.key.endsWith('_id')) return h('span', { class: 'font-mono text-xs' }, text.substring(0, 8))
                return text
            },
        } as TableColumn<ComponentRecord>
    })
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
