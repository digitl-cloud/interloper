import { h } from 'vue'
import { RelativeTime, StatusBadge, UBadge, UButton, UIcon, UTooltip } from '#components'
import type { Column } from '@tanstack/vue-table'
import type { TableColumn } from '@nuxt/ui'
import { stateColumns, type ComponentDefinition } from '~/types/catalog'
import type { ComponentRecord } from '~/types/component'
import type { DriftBadge } from '~/composables/drift'
import { componentIcon } from '~/utils/icons'
import { shortId } from '~/utils/ids'

/**
 * Builds a clickable column header that toggles sorting (none → asc → desc).
 * Only the sorted column shows its arrow; the others reveal theirs on hover
 * or focus, so a header row reads as plain labels. Use as a column's `header`
 * to make it sortable. The button sits flush with the column (its padding is
 * pulled out) and keeps the header row's type, so sortable and plain headers
 * read alike.
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
            size: 'md',
            label,
            icon,
            trailing: true,
            class: ['group -mx-2 px-2 font-normal hover:text-highlighted', sorted ? 'text-highlighted' : 'text-muted'],
            ui: sorted ? undefined : { trailingIcon: 'opacity-0 transition-opacity group-hover:opacity-100 group-focus-visible:opacity-100' },
            'aria-label': `Sort by ${label}`,
            onClick: () => column.toggleSorting(column.getIsSorted() === 'asc'),
        })
    }
}

/** The dash an empty cell shows. */
export function emptyCell() {
    return h('span', { class: 'text-dimmed' }, '—')
}

/** A UUID as its short form, the full id in a tooltip. */
export function idCell(id: string) {
    return h(UTooltip, { text: id }, () => h('span', { class: 'tabular-nums slashed-zero' }, shortId(id)))
}

/**
 * A sortable timestamp column. It orders on the raw ISO value and reads as a
 * relative time ("2d ago"), the exact one on hover.
 */
export function dateColumn<T>(key: keyof T & string, header: string): TableColumn<T> {
    return {
        accessorKey: key,
        header,
        cell: ({ row }: { row: { getValue: <V>(key: string) => V } }) => {
            const value = row.getValue<string | null>(key)
            return value ? h(RelativeTime, { value, class: 'text-muted' }) : emptyCell()
        },
    } as TableColumn<T>
}

/**
 * The first column of a component table: the name, its discriminator when the
 * two differ, and an optional status, on one line, stacking under the name
 * once the viewport is too narrow for it. Search matches the name and the
 * discriminator alike.
 */
export function nameColumn(
    header: string,
    badge?: (row: ComponentRecord) => DriftBadge | null,
): TableColumn<ComponentRecord> {
    const nameOf = (row: ComponentRecord) => row.name ?? useCatalogStore().typeName(row.key)
    return {
        id: 'name',
        header,
        accessorFn: (row: ComponentRecord) => [nameOf(row), row.discriminator].filter(Boolean).join(' '),
        cell: ({ row }: { row: { original: ComponentRecord } }) => {
            const name = nameOf(row.original)
            const { discriminator } = row.original
            const meta = badge?.(row.original)
            const children = [h('span', { class: 'truncate' }, name)]
            if (discriminator && discriminator !== name) {
                children.push(h(UBadge, { color: 'neutral', size: 'sm', label: discriminator, class: 'max-w-full slashed-zero sm:shrink-0' }))
            }
            if (meta) children.push(h(StatusBadge, { color: meta.color, label: meta.label, size: 'sm', class: 'shrink-0' }))
            return h('span', { class: 'flex min-w-0 flex-col items-start gap-1 sm:flex-row sm:items-center sm:gap-2' }, children)
        },
    } as TableColumn<ComponentRecord>
}

/** The `Type` column of a component table: the definition's icon and name. */
export function typeColumn(icon: (key: string) => string = componentIcon): TableColumn<ComponentRecord> {
    const typeName = (key: string) => useCatalogStore().typeName(key)
    return {
        id: 'type',
        header: 'Type',
        accessorFn: (row: ComponentRecord) => typeName(row.key),
        cell: ({ row }: { row: { original: ComponentRecord } }) => h('span', { class: 'flex items-center gap-2 text-muted' }, [
            h(UIcon, { name: icon(row.original.key), class: 'size-4 shrink-0' }),
            typeName(row.original.key),
        ]),
    } as TableColumn<ComponentRecord>
}

/**
 * Table columns for a component row's `state`, derived from its definition's
 * `state_schema`. Datetime values read as relative times; `_id` values as a
 * short id with the full one in a tooltip.
 */
export function stateSchemaColumns(defn: ComponentDefinition | undefined): TableColumn<ComponentRecord>[] {
    if (!defn) return []
    return stateColumns(defn).map((col) => {
        if (col.format === 'datetime') {
            return {
                id: col.key,
                header: col.label,
                accessorFn: (row: ComponentRecord) => row.state?.[col.key] ?? '',
                cell: ({ row }: { row: { original: ComponentRecord } }) => {
                    const value = row.original.state?.[col.key]
                    return value ? h(RelativeTime, { value }) : emptyCell()
                },
            } as TableColumn<ComponentRecord>
        }
        return {
            id: col.key,
            header: col.label,
            accessorFn: (row: ComponentRecord) => row.state?.[col.key] ?? '',
            cell: ({ row }: { row: { original: ComponentRecord } }) => {
                const value = row.original.state?.[col.key]
                if (value == null || value === '') return emptyCell()
                const text = String(value)
                if (col.key.endsWith('_id')) return idCell(text)
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
