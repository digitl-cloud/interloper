<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { ComponentRecord } from '~/types/component'
import { hookEnabled, hookEvents, relationIds } from '~/types/component'

definePageMeta({ title: 'Hooks' })

const UBadge = resolveComponent('UBadge')

const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const toast = useToast()

const hooks = computed(() => componentsStore.byKind('hook'))

const stepperRef = ref<any>(null)

const {
    open: drawerOpen,
    editing: editingHook,
    openCreate: handleCreate,
    openEdit: handleEdit,
} = useWizardDrawer<ComponentRecord>()

componentsStore.fetchAll(['hook', 'source', 'asset', 'job'])

const columns = computed<TableColumn<ComponentRecord>[]>(() => [
    { accessorKey: 'select' as any, header: '' },
    {
        accessorKey: 'name',
        header: 'Name',
        cell: ({ row }) => h('span', { class: 'font-medium' }, row.original.name ?? ''),
    },
    {
        accessorKey: 'key',
        header: 'Type',
        accessorFn: (row: ComponentRecord) => catalogStore.catalog[row.key]?.name ?? row.key,
    },
    {
        accessorKey: 'events',
        header: 'Events',
        cell: ({ row }) => h('div', { class: 'flex flex-wrap gap-1' }, hookEvents(row.original).map(event =>
            h(UBadge, {
                key: event,
                color: 'neutral',
                variant: 'subtle',
            }, () => event),
        )),
    },
    {
        accessorKey: 'watches',
        header: 'Watches',
        cell: ({ row }) => {
            const count = relationIds(row.original, 'watch').length
            return h(UBadge, {
                color: 'neutral',
                variant: 'subtle',
            }, () => `${count} watched`)
        },
    },
    {
        accessorKey: 'enabled',
        header: 'Status',
        cell: ({ row }) => h(UBadge, {
            color: hookEnabled(row.original) ? 'success' : 'neutral',
            variant: 'subtle',
        }, () => hookEnabled(row.original) ? 'Enabled' : 'Disabled'),
    },
    ...stateSchemaColumns(catalogStore.definitionsForKind('hook')[0]),
    {
        accessorKey: 'created_at',
        header: 'Created',
        accessorFn: (row: ComponentRecord) => row.created_at ? formatDate(row.created_at) : '—',
    },
])

function handleSaved() {
    componentsStore.fetchAll(['hook'])
    drawerOpen.value = false
}

async function handleDelete(ids: string[]) {
    try {
        await componentsStore.remove(ids)
        toast.add({ title: `${ids.length} hook${ids.length > 1 ? 's' : ''} deleted`, color: 'success' })
    }
    catch {
        toast.add({ title: 'Failed to delete hook', color: 'error' })
    }
}
</script>

<template>
    <div>
        <div class="flex flex-col flex-1 min-h-0">
            <DataTable :columns="columns"
                       :data="hooks"
                       :loading="componentsStore.loading"
                       search-placeholder="Search hooks..."
                       @delete="handleDelete"
                       @edit="handleEdit">
                <template #toolbar>
                    <UButton icon="i-lucide-plus"
                             label="New hook"
                             @click="handleCreate" />
                </template>

                <template #empty>
                    <EmptyState icon="i-carbon-lightning"
                                title="No hooks yet"
                                description="A hook reacts to what your pipelines do. Watch a source, asset, or job and trigger downstream runs or notify an external system when runs complete or fail." />

                    <div class="flex items-center gap-3 border border-default rounded-[14px] px-5 py-4 bg-(--ui-bg-band) mt-9">
                        <div class="flex-1 text-sm text-toned">
                            Create a hook to react to run outcomes on your components.
                        </div>
                        <UButton icon="i-lucide-plus"
                                 label="New hook"
                                 size="md"
                                 @click="handleCreate" />
                    </div>
                </template>
            </DataTable>
        </div>

        <WizardDrawer v-model:open="drawerOpen"
                      :default-title="editingHook ? 'Edit Hook' : 'New Hook'"
                      description="Configure hook"
                      :stepper="stepperRef">
            <HooksStepper v-if="drawerOpen"
                          :key="editingHook?.id ?? 'new'"
                          ref="stepperRef"
                          :hook="editingHook"
                          @created="handleSaved"
                          @updated="handleSaved" />
        </WizardDrawer>
    </div>
</template>
