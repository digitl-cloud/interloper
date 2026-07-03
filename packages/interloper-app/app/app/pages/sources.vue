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
const { statusBadge, sourceDrift } = useDrift()

function typeName(key: string): string {
    return catalogStore.catalog[key]?.name ?? key
}

const stepperRef = ref<any>(null)
const toast = useToast()

const {
    open: drawerOpen,
    editing: editingSource,
    presetTypeKey,
    openCreate: handleCreate,
    openCreateWithType: handleCreateFromCatalog,
    openEdit: handleEdit,
} = useWizardDrawer<Source>()

sourcesStore.fetch()

const columns: TableColumn<Source>[] = [
    { accessorKey: 'select' as any, header: '' },
    {
        accessorKey: 'name',
        header: 'Source',
        cell: ({ row }) => {
            const children: any[] = [
                h(UIcon, { name: componentIcon(row.original.key), class: 'size-4 shrink-0' }),
                row.original.name,
            ]
            const badge = statusBadge(sourceDrift(row.original))
            if (badge) {
                children.push(h(UBadge, {
                    color: badge.color,
                    variant: 'subtle',
                    size: 'sm',
                    class: 'ml-1',
                }, () => h('span', { class: 'flex items-center gap-1' }, [
                    h(UIcon, { name: badge.icon, class: 'size-3 shrink-0' }),
                    badge.label,
                ])))
            }
            return h('span', { class: 'flex items-center gap-2' }, children)
        },
    },
    {
        accessorKey: 'type',
        header: 'Type',
        cell: ({ row }) => h(UBadge, {
            color: 'neutral',
            variant: 'subtle',
        }, () => h('span', { class: 'flex items-center gap-1.5' }, [
            h(UIcon, { name: componentIcon(row.original.key), class: 'size-4 shrink-0' }),
            typeName(row.original.key),
        ])),
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
        <DriftBanner />

        <div class="flex flex-col flex-1 min-h-0">
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

                <template #empty>
                    <SourcesEmptyState @create="handleCreate"
                                       @create-type="handleCreateFromCatalog" />
                </template>
            </DataTable>
        </div>

        <WizardDrawer v-model:open="drawerOpen"
                      default-title="New Source"
                      description="Configure a new source"
                      :stepper="stepperRef">
            <SourcesStepper v-if="drawerOpen"
                            :key="editingSource?.id ?? 'new'"
                            ref="stepperRef"
                            :source="editingSource"
                            :initial-type-key="presetTypeKey"
                            @created="handleSaved"
                            @updated="handleSaved" />
        </WizardDrawer>
    </div>
</template>
