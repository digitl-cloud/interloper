<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { ComponentRecord } from '~/types/component'
import { resourceMap } from '~/types/component'

definePageMeta({ title: 'Components', fullBleed: true })

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

/** The connection a destination is bound to, if any. */
function connectionOf(destination: ComponentRecord): ComponentRecord | undefined {
    const id = resourceMap(destination).connection
    return id ? componentsStore.byId(id) : undefined
}

const columns: TableColumn<ComponentRecord>[] = [
    {
        id: 'destination',
        header: 'Destination',
        accessorFn: (row: ComponentRecord) => row.name ?? typeName(row.key),
        cell: ({ row }) => {
            const label = row.original.name ?? typeName(row.original.key)
            return h('span', { class: 'flex items-center gap-2' }, [
                h(UIcon, { name: typeIcon(row.original.key), class: 'size-4 shrink-0' }),
                label,
            ])
        },
    },
    {
        id: 'type',
        header: 'Type',
        accessorFn: (row: ComponentRecord) => typeName(row.key),
        cell: ({ row }) => h('span', { class: 'flex items-center gap-1.5 text-muted' }, [
            h(UIcon, { name: typeIcon(row.original.key), class: 'size-4 shrink-0' }),
            typeName(row.original.key),
        ]),
    },
    {
        id: 'connection',
        header: 'Connection',
        accessorFn: (row: ComponentRecord) => connectionOf(row)?.name ?? connectionOf(row)?.key ?? '',
        cell: ({ row }) => {
            const resource = connectionOf(row.original)
            if (!resource) return h('span', { class: 'text-muted' }, '—')
            return h(EntityBadge, {
                icon: componentIcon(resource.key, 'i-lucide-key-round'),
                label: resource.name ?? resource.key,
            })
        },
    },
    dateColumn<ComponentRecord>('created_at', 'Created'),
]

async function handleDelete(ids: string[]) {
    try {
        await componentsStore.remove(ids)
        toast.add({ title: `${ids.length} destination${ids.length > 1 ? 's' : ''} deleted`, color: 'success' })
    }
    catch (e) {
        toast.add(inUseToast(e, 'Destination') ?? errorToast(e, 'Failed to delete destination'))
    }
}

function handleSaved() {
    componentsStore.fetchAll(['destination'])
    drawerOpen.value = false
}

const typeKey = ref<string | null>(null)
</script>

<template>
    <div>
        <NavActions>
            <UButton icon="i-lucide-plus"
                     label="New destination"
                     @click="handleCreate" />
        </NavActions>
        <NavComponentsHub>
            <DataTable :columns="columns"
                       :data="destinations"
                       :filter="row => typeKey === null || row.key === typeKey"
                       :loading="componentsStore.loading"
                       :error="componentsStore.error"
                       :delete-impact="componentsStore.deleteImpact"
                       search-placeholder="Search destinations..."
                       @delete="handleDelete"
                       @edit="handleEdit"
                       @retry="componentsStore.reload()">
                <template #filters>
                    <TypeFilter v-model="typeKey"
                                :components="destinations" />
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
        </NavComponentsHub>

        <WizardDrawer v-model:open="drawerOpen"
                      default-title="New Destination"
                      description="Configure destination"
                      :stepper="stepperRef">
            <WizardDefinitionStepper v-if="drawerOpen"
                                     :key="editingDestination?.id ?? 'new'"
                                     ref="stepperRef"
                                     kind="destination"
                                     noun="Destination"
                                     :component="editingDestination"
                                     :initial-type-key="presetTypeKey"
                                     :definitions="catalogStore.destinationDefinitions"
                                     resource-relation-steps
                                     name-optional
                                     @created="handleSaved"
                                     @updated="handleSaved" />
        </WizardDrawer>
    </div>
</template>
