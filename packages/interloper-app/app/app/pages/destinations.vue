<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { ComponentRecord } from '~/types/component'
import { resourceMap } from '~/types/component'

definePageMeta({ title: 'Destinations' })

const UIcon = resolveComponent('UIcon')
const EntityBadge = resolveComponent('EntityBadge')

const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()

const destinations = computed(() => componentsStore.byKind('destination'))

const stepperRef = ref<any>(null)
const toast = useToast()

const {
    open: drawerOpen,
    editing: editingDestination,
    presetTypeKey,
    openCreate: handleCreate,
    openCreateWithType: handleCreateFromCatalog,
    openEdit: handleEdit,
} = useWizardDrawer<ComponentRecord>()

// Resource kinds too — the Connection column resolves resource names by id.
componentsStore.fetchAll()
componentsStore.fetchRelations()

function typeIcon(key: string): string {
    return componentIcon(key, 'i-lucide-hard-drive')
}

function typeName(key: string): string {
    const defn = catalogStore.catalog[key]
    return defn?.name ?? key
}

const columns: TableColumn<ComponentRecord>[] = [
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
        cell: ({ row }) => h('span', { class: 'flex items-center gap-1.5 text-muted' }, [
            h(UIcon, { name: typeIcon(row.original.key), class: 'size-4 shrink-0' }),
            typeName(row.original.key),
        ]),
    },
    {
        accessorKey: 'resources',
        header: 'Connection',
        cell: ({ row }) => {
            const connId = resourceMap(row.original).connection
            if (!connId) return h('span', { class: 'text-muted' }, '—')
            const resource = componentsStore.byId(connId)
            if (!resource) return h('span', { class: 'text-muted' }, '—')
            return h(EntityBadge, {
                icon: componentIcon(resource.key, 'i-lucide-key-round'),
                label: resource.name ?? resource.key,
            })
        },
    },
    {
        accessorKey: 'created_at',
        header: 'Created',
        accessorFn: (row: ComponentRecord) => row.created_at ? formatDate(row.created_at) : '—',
    },
]

async function handleDelete(ids: string[]) {
    try {
        await componentsStore.remove(ids)
        toast.add({ title: `${ids.length} destination${ids.length > 1 ? 's' : ''} deleted`, color: 'success' })
    }
    catch (e) {
        toast.add(inUseToast(e, 'Destination') ?? { title: 'Failed to delete destination', color: 'error' })
    }
}

function handleSaved() {
    componentsStore.fetchAll(['destination'])
    drawerOpen.value = false
}
</script>

<template>
    <div>
        <div class="flex flex-col flex-1 min-h-0">
            <DataTable :columns="columns"
                       :data="destinations"
                       :loading="componentsStore.loading"
                       :delete-impact="componentsStore.deleteImpact"
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
