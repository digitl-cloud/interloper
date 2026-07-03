<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { Destination } from '~/types/destination'

definePageMeta({ title: 'Destinations' })

const UIcon = resolveComponent('UIcon')
const UBadge = resolveComponent('UBadge')

const catalogStore = useCatalogStore()
const destinationsStore = useDestinationsStore()
const resourcesStore = useResourcesStore()

const stepperRef = ref<any>(null)
const toast = useToast()

const {
    open: drawerOpen,
    editing: editingDestination,
    presetTypeKey,
    openCreate: handleCreate,
    openCreateWithType: handleCreateFromCatalog,
    openEdit: handleEdit,
} = useWizardDrawer<Destination>()

destinationsStore.fetch()
resourcesStore.fetch()

function typeIcon(key: string): string {
    return componentIcon(key, 'i-lucide-hard-drive')
}

function typeName(key: string): string {
    const defn = catalogStore.catalog[key]
    return defn?.name ?? key
}

const columns: TableColumn<Destination>[] = [
    { accessorKey: 'select' as any, header: '' },
    {
        accessorKey: 'key',
        header: 'Destination',
        cell: ({ row }) => {
            const label = row.original.name ?? typeName(row.original.key)
            return h('span', { class: 'flex items-center gap-2' }, [
                h(UIcon, { name: typeIcon(row.original.key), class: 'size-4 shrink-0' }),
                label,
            ])
        },
    },
    {
        accessorKey: 'type',
        header: 'Type',
        cell: ({ row }) => h(UBadge, {
            color: 'neutral',
            variant: 'subtle',
        }, () => h('span', { class: 'flex items-center gap-1.5' }, [
            h(UIcon, { name: typeIcon(row.original.key), class: 'size-4 shrink-0' }),
            typeName(row.original.key),
        ])),
    },
    {
        accessorKey: 'resources',
        header: 'Connection',
        cell: ({ row }) => {
            const resources = row.original.resources
            const connId = resources.connection
            if (!connId) return h('span', { class: 'text-muted' }, '—')
            const resource = resourcesStore.findById(connId)
            if (!resource) return h('span', { class: 'text-muted' }, '—')
            const icon = componentIcon(resource.key, 'i-lucide-key-round')
            return h(UBadge, { color: 'neutral', variant: 'subtle' }, () =>
                h('span', { class: 'flex items-center gap-1.5' }, [
                    h(UIcon, { name: icon, class: 'size-3.5 shrink-0' }),
                    resource.name,
                ]),
            )
        },
    },
    {
        accessorKey: 'created_at',
        header: 'Created',
        accessorFn: (row: Destination) => row.created_at ? formatDate(row.created_at) : '—',
    },
]

async function handleDelete(ids: string[]) {
    try {
        await destinationsStore.remove(ids)
        toast.add({ title: `${ids.length} destination${ids.length > 1 ? 's' : ''} deleted`, color: 'success' })
    }
    catch {
        toast.add({ title: 'Failed to delete destination', color: 'error' })
    }
}

function handleSaved() {
    destinationsStore.fetch()
    drawerOpen.value = false
}
</script>

<template>
    <div>
        <div class="flex flex-col flex-1 min-h-0">
            <DataTable :columns="columns"
                       :data="destinationsStore.destinations"
                       :loading="destinationsStore.loading"
                       search-placeholder="Search destinations..."
                       @delete="handleDelete"
                       @edit="handleEdit">
                <template #toolbar>
                    <UButton icon="i-lucide-plus"
                             label="New destination"
                             @click="handleCreate" />
                </template>

                <template #empty>
                    <EmptyState icon="i-lucide-hard-drive"
                                title="No destinations yet"
                                description="A destination is wherever Interloper materializes your typed assets — a warehouse, a database, or plain files, all running on infrastructure you control. Pick one from the catalog below to start landing data.">
                        <UButton icon="i-lucide-plus"
                                 label="New destination"
                                 class="mt-5"
                                 @click="handleCreate" />
                    </EmptyState>

                    <div class="mt-9 mb-3.5">
                        <h2 class="text-lg font-bold tracking-[-0.015em] text-highlighted">
                            Destinations Catalog
                        </h2>
                        <p class="text-sm text-muted mt-1.5">
                            Production-ready IO backends that ship with Interloper now.
                        </p>
                    </div>
                    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                        <CatalogCard v-for="def in catalogStore.destinationDefinitions"
                                     :key="def.key"
                                     variant="rich"
                                     :icon="typeIcon(def.key)"
                                     :title="def.name"
                                     :caption="def.provider"
                                     :description="def.description"
                                     :chips="(def.tags ?? []).map(t => ({ label: t }))"
                                     @click="handleCreateFromCatalog(def.key)" />
                    </div>
                </template>
            </DataTable>
        </div>

        <WizardDrawer v-model:open="drawerOpen"
                      default-title="New Destination"
                      description="Configure destination"
                      :stepper="stepperRef">
            <DestinationsStepper v-if="drawerOpen"
                                 :key="editingDestination?.id ?? 'new'"
                                 ref="stepperRef"
                                 :destination="editingDestination"
                                 :initial-type-key="presetTypeKey"
                                 @created="handleSaved"
                                 @updated="handleSaved" />
        </WizardDrawer>
    </div>
</template>
