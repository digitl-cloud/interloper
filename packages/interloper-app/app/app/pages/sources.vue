<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { Source } from '~/types/source'

definePageMeta({ title: 'Sources' })

const UIcon = resolveComponent('UIcon')
const UBadge = resolveComponent('UBadge')

const catalogStore = useCatalogStore()
const sourcesStore = useSourcesStore()
const destinationsStore = useDestinationsStore()

function typeIcon(key: string): string {
    return componentIcon(key)
}

const drawerOpen = ref(false)
const editingSource = ref<Source | null>(null)
const stepperRef = ref<any>(null)
const toast = useToast()

function handleEdit(source: Source) {
    editingSource.value = source
    drawerOpen.value = true
}

function handleCreate() {
    editingSource.value = null
    drawerOpen.value = true
}

sourcesStore.fetch()

const columns: TableColumn<Source>[] = [
    { accessorKey: 'select' as any, header: '' },
    {
        accessorKey: 'name',
        header: 'Source',
        cell: ({ row }) => h('span', { class: 'flex items-center gap-2' }, [
            h(UIcon, { name: typeIcon(row.original.key), class: 'size-4 shrink-0' }),
            row.original.name,
        ]),
    },
    {
        accessorKey: 'assets',
        header: 'Assets',
        cell: ({ row }) => h(UBadge, {
            color: 'neutral',
            variant: 'subtle',
        }, () => `${row.original.assets.length} asset${row.original.assets.length !== 1 ? 's' : ''}`),
    },
    {
        accessorKey: 'destinations',
        header: 'Destinations',
        cell: ({ row }) => {
            const dests = row.original.destinations
            if (dests.length === 0) return h('span', { class: 'text-muted' }, '—')
            const first = dests[0]!
            const defn = catalogStore.catalog[first.key]
            const icon = componentIcon(first.key)
            const label = first.name ?? defn?.name ?? first.key
            return h(UBadge, {
                color: 'neutral',
                variant: 'subtle',
            }, () => h('span', { class: 'flex items-center gap-1.5' }, [
                h(UIcon, { name: icon, class: 'size-4 shrink-0' }),
                label,
                dests.length > 1 ? ` +${dests.length - 1}` : '',
            ]))
        },
    },
    {
        accessorKey: 'created_at',
        header: 'Created',
        accessorFn: (row: Source) => row.created_at ? formatDate(row.created_at) : '—',
    },
]

function handleSaved() {
    sourcesStore.fetch()
    drawerOpen.value = false
}

async function handleDelete(ids: string[]) {
    try {
        await sourcesStore.remove(ids)
        toast.add({ title: `${ids.length} source${ids.length > 1 ? 's' : ''} deleted`, color: 'success' })
    }
    catch {
        toast.add({ title: 'Failed to delete source', color: 'error' })
    }
}
</script>

<template>
    <div>
        <div class="flex flex-col h-full">
            <DataTable :columns="columns"
                       :data="sourcesStore.sources"
                       :loading="sourcesStore.loading"
                       search-placeholder="Search sources..."
                       @delete="handleDelete"
                       @edit="handleEdit">
                <template #toolbar>
                    <UButton icon="i-lucide-plus"
                             label="New source"
                             @click="handleCreate" />
                </template>
            </DataTable>
        </div>

        <UDrawer v-model:open="drawerOpen"
                 direction="right"
                 :handle="false"
                 :handle-only="true"
                 :title="stepperRef?.title ?? 'New Source'"
                 :ui="{ content: 'w-[40rem]', description: 'sr-only' }">
            <template #description>
                Configure a new source
            </template>
            <template #body>
                <SourcesStepper v-if="drawerOpen"
                                :key="editingSource?.id ?? 'new'"
                                ref="stepperRef"
                                :source="editingSource"
                                @created="handleSaved"
                                @updated="handleSaved" />
            </template>
            <template #footer>
                <StepperNav v-if="stepperRef"
                            :can-proceed="stepperRef.canProceed"
                            :has-prev="stepperRef.hasPrev"
                            :submitting="stepperRef.submitting"
                            :submit-label="stepperRef.submitLabel"
                            @next="stepperRef.next()"
                            @prev="stepperRef.prev()" />
            </template>
        </UDrawer>
    </div>
</template>
