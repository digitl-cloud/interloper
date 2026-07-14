<script setup lang="ts" generic="TData extends { id: string }, TValue">
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import { getPaginationRowModel } from '@tanstack/vue-table'

const PAGE_SIZE = 20

const props = defineProps<{
    columns: TableColumn<TData>[]
    data: TData[]
    loading?: boolean
    searchPlaceholder?: string
    /** Extra context-menu / action items per row (prepended before edit & delete). */
    rowActions?: (item: TData) => DropdownMenuItem[][]
    /** When true, suppresses the built-in actions column and row menus. */
    noActions?: boolean
}>()

const emit = defineEmits<{
    delete: [ids: string[]]
    edit: [item: TData]
}>()

const globalFilter = ref('')
const showDeleteModal = ref(false)
const showDeleteOneModal = ref(false)
const deleteOneItem = ref<TData | null>(null)
const tableRef = useTemplateRef<{ tableApi: any }>('table')

const pagination = ref({ pageIndex: 0, pageSize: PAGE_SIZE })

// Reset to first page when search filter changes
watch(globalFilter, () => {
    pagination.value = { ...pagination.value, pageIndex: 0 }
})

const selectedCount = computed<number>(() =>
    tableRef.value?.tableApi?.getFilteredSelectedRowModel().rows.length ?? 0,
)
const totalCount = computed<number>(() =>
    tableRef.value?.tableApi?.getFilteredRowModel().rows.length ?? 0,
)

function selectedIds(): string[] {
    return tableRef.value?.tableApi?.getFilteredSelectedRowModel().rows.map((row: any) => row.original.id) ?? []
}

function confirmBulkDelete() {
    emit('delete', selectedIds())
    tableRef.value?.tableApi?.toggleAllRowsSelected(false)
    showDeleteModal.value = false
}

function confirmDeleteOne() {
    if (deleteOneItem.value) {
        emit('delete', [deleteOneItem.value.id])
    }
    deleteOneItem.value = null
    showDeleteOneModal.value = false
}

// ── Row action menus ──

function buildRowActions(item: TData): DropdownMenuItem[][] {
    const extra = props.rowActions?.(item) ?? []
    if (props.noActions) return extra

    return [
        ...extra,
        [
            {
                label: 'Edit',
                icon: 'i-lucide-pencil',
                onSelect: () => emit('edit', item),
            },
            {
                label: 'Delete',
                icon: 'i-lucide-trash-2',
                color: 'error' as const,
                onSelect: () => {
                    deleteOneItem.value = item
                    showDeleteOneModal.value = true
                },
            },
        ],
    ]
}

// ── Append actions column ──

const columnsWithActions = computed<TableColumn<TData>[]>(() => {
    const sortable = withSortableHeaders(props.columns)
    if (props.noActions) return sortable
    return [
        ...sortable,
        {
            id: 'actions',
            header: '',
            cell: ({ row }: any) => row.original,
        } as TableColumn<TData>,
    ]
})

// ── Right-click context menu ──
const ctxMenuOpen = ref(false)
const ctxMenuVirtual = ref({ getBoundingClientRect: () => new DOMRect() })
const ctxMenuItems = ref<DropdownMenuItem[][]>([])

function onRowContextMenu(e: Event, row: { original: TData }) {
    const actions = buildRowActions(row.original)
    if (actions.length === 0) return
    const event = e as MouseEvent
    event.preventDefault()

    ctxMenuItems.value = actions
    const { clientX: x, clientY: y } = event
    ctxMenuVirtual.value = { getBoundingClientRect: () => new DOMRect(x, y, 0, 0) }
    ctxMenuOpen.value = true
}


// ── Empty state ──
const slots = useSlots()
/**
 * Loading has completed at least once. Before that, an empty `data` just means
 * "not fetched yet"; after, background refetches (the store's loading flag is
 * global, e.g. a wizard fetching candidate components) must not swap the empty
 * state out for the table.
 */
const hasLoaded = ref(!props.loading)
watch(() => props.loading, (loading) => {
    if (!loading) hasLoaded.value = true
})
/** With no data at all (not just filtered out), render the #empty slot instead of the table. */
const showEmpty = computed(() => hasLoaded.value && props.data.length === 0 && !!slots.empty)
</script>

<template>
    <div class="w-full flex flex-col gap-4">
        <div v-if="!showEmpty"
             class="flex items-center gap-3">
            <UInput v-model="globalFilter"
                    :placeholder="searchPlaceholder ?? 'Search...'"
                    icon="i-lucide-search"
                    class="max-w-sm"
                    @update:model-value="tableRef?.tableApi?.setGlobalFilter($event)" />

            <div class="ml-auto flex items-center gap-2">
                <slot name="toolbar" />

                <UModal v-if="selectedCount > 0"
                        v-model:open="showDeleteModal"
                        title="Confirm Deletion">
                    <UButton color="error"
                             icon="i-lucide-trash-2"
                             :label="`Delete (${selectedCount})`" />

                    <template #body>
                        <div class="space-y-4">
                            <p>
                                This will permanently delete {{ selectedCount }} {{ selectedCount === 1 ? 'item' :
                                    'items' }}. This action cannot be undone.
                            </p>
                            <div class="flex justify-end gap-2">
                                <UButton label="Cancel"
                                         @click="showDeleteModal = false" />
                                <UButton color="error"
                                         label="Delete"
                                         @click="confirmBulkDelete" />
                            </div>
                        </div>
                    </template>
                </UModal>
            </div>
        </div>

        <div v-if="showEmpty"
             class="w-full max-w-[1040px] mx-auto">
            <slot name="empty" />
        </div>

        <UTable v-if="!showEmpty"
                ref="table"
                v-model:pagination="pagination"
                :data="data"
                :columns="columnsWithActions"
                :loading="loading"
                :global-filter="globalFilter"
                :pagination-options="{ getPaginationRowModel: getPaginationRowModel() }"
                sticky
                class="max-h-[calc(100vh-16rem)]"
                @select="(_e: Event, row: any) => emit('edit', row.original)"
                @contextmenu="onRowContextMenu">
            <template #actions-cell="{ row }">
                <div class="flex justify-end">
                    <UDropdownMenu :items="buildRowActions(row.original)">
                        <UButton icon="i-lucide-ellipsis-vertical"
                                 color="neutral"
                                 variant="ghost"
                                 size="sm" />
                    </UDropdownMenu>
                </div>
            </template>
        </UTable>

        <!-- Right-click context menu -->
        <UDropdownMenu v-model:open="ctxMenuOpen"
                       :items="ctxMenuItems"
                       :modal="false"
                       :content="{ reference: ctxMenuVirtual, side: 'bottom', align: 'start', sideOffset: 4 }">
            <div class="hidden" />
        </UDropdownMenu>

        <!-- Single-item delete confirmation -->
        <UModal v-model:open="showDeleteOneModal"
                title="Confirm Deletion">
            <template #default />
            <template #body>
                <div class="space-y-4">
                    <p>
                        This will permanently delete this item. This action cannot be undone.
                    </p>
                    <div class="flex justify-end gap-2">
                        <UButton label="Cancel"
                                 @click="showDeleteOneModal = false" />
                        <UButton color="error"
                                 label="Delete"
                                 @click="confirmDeleteOne" />
                    </div>
                </div>
            </template>
        </UModal>

        <TableFooter v-if="!showEmpty"
                     :page="pagination.pageIndex + 1"
                     :total="totalCount"
                     :page-size="PAGE_SIZE"
                     @update:page="(p: number) => pagination = { ...pagination, pageIndex: p - 1 }">
            <template v-if="selectedCount > 0">
                {{ selectedCount }} of {{ totalCount }} row(s) selected.
            </template>
            <template v-else>
                {{ totalCount }} row(s) total.
            </template>
        </TableFooter>
    </div>
</template>
