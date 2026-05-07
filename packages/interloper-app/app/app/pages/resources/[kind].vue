<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { Resource } from '~/types/resource'

definePageMeta({ title: 'Resources' })

const UIcon = resolveComponent('UIcon')
const UBadge = resolveComponent('UBadge')

const route = useRoute()
const catalogStore = useCatalogStore()
const resourcesStore = useResourcesStore()

const kind = computed(() => route.params.kind as string)
const pageTitle = computed(() => {
    const k = kind.value
    return k.charAt(0).toUpperCase() + k.slice(1) + 's'
})

/** Available type definitions for this resource kind. */
const definitions = computed(() => catalogStore.definitionsForKind(kind.value))

/** Resources filtered by current kind. */
const resources = computed(() => resourcesStore.byKind(kind.value))

function typeName(key: string): string {
    return catalogStore.catalog[key]?.name ?? key
}

function typeIcon(key: string): string {
    return componentIcon(key)
}

const drawerOpen = ref(false)
const stepperRef = ref<any>(null)
const editingResource = ref<Resource | null>(null)

// Re-fetch when kind changes
watch(kind, () => resourcesStore.fetch(kind.value), { immediate: true })

const columns: TableColumn<Resource>[] = [
    { accessorKey: 'select' as any, header: '' },
    { accessorKey: 'name', header: 'Name' },
    {
        accessorKey: 'key',
        header: 'Type',
        cell: ({ row }) => h(UBadge, {
            color: 'neutral',
            variant: 'subtle',
        }, () => h('span', { class: 'flex items-center gap-1.5' }, [
            h(UIcon, { name: typeIcon(row.original.key), class: 'size-4 shrink-0' }),
            typeName(row.original.key),
        ])),
    },
    { accessorKey: 'encrypted', header: 'Encrypted' },
    {
        accessorKey: 'created_at',
        header: 'Created',
        accessorFn: (row: Resource) => row.created_at ? formatDate(row.created_at) : '-',
    },
]

const toast = useToast()

async function handleDelete(ids: string[]) {
    try {
        await resourcesStore.remove(ids)
        toast.add({ title: `${ids.length} resource(s) deleted`, color: 'success' })
    }
    catch {
        toast.add({ title: 'Failed to delete resource', description: 'It may be in use by a source or destination.', color: 'error' })
    }
}

function handleEdit(item: Resource) {
    editingResource.value = item
    drawerOpen.value = true
}

function handleCreate() {
    editingResource.value = null
    drawerOpen.value = true
}

function handleSaved() {
    resourcesStore.fetch(kind.value)
    drawerOpen.value = false
}
</script>

<template>
    <div>
        <div class="flex flex-col h-full">
            <DataTable :columns="columns"
                       :data="resources"
                       :loading="resourcesStore.loading"
                       :search-placeholder="`Search ${pageTitle.toLowerCase()}...`"
                       @delete="handleDelete"
                       @edit="handleEdit">
                <template #toolbar>
                    <UButton icon="i-lucide-plus"
                             :label="`New ${kind}`"
                             @click="handleCreate" />
                </template>
            </DataTable>
        </div>

        <UDrawer v-model:open="drawerOpen"
                 direction="right"
                 modal
                 :handle="false"
                 :handle-only="true"
                 :title="stepperRef?.title ?? `New ${kind}`"
                 :ui="{ content: 'w-[40rem]', description: 'sr-only' }">
            <template #description>
                {{ editingResource ? 'Edit' : 'Configure a new' }} {{ kind }}
            </template>
            <template #body>
                <ResourcesStepper v-if="drawerOpen"
                                  :key="editingResource?.id ?? 'new'"
                                  ref="stepperRef"
                                  :kind="kind"
                                  :definitions="definitions"
                                  :resource="editingResource"
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
