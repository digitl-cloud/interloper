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

const drawerOpen = ref(false)
const editingDestination = ref<Destination | null>(null)
const stepperRef = ref<any>(null)
const toast = useToast()

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
        }, () => typeName(row.original.key)),
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

function handleEdit(dest: Destination) {
    editingDestination.value = dest
    drawerOpen.value = true
}

function handleCreate() {
    editingDestination.value = null
    drawerOpen.value = true
}

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
        <div class="flex flex-col h-full">
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
            </DataTable>
        </div>

        <UDrawer v-model:open="drawerOpen"
                 direction="right"
                 :handle="false"
                 :handle-only="true"
                 :title="stepperRef?.title ?? 'New Destination'"
                 :ui="{ content: 'w-[40rem]', description: 'sr-only' }">
            <template #description>
                Configure destination
            </template>
            <template #body>
                <DestinationsStepper v-if="drawerOpen"
                                      :key="editingDestination?.id ?? 'new'"
                                      ref="stepperRef"
                                      :destination="editingDestination"
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
