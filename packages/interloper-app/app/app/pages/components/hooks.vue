<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { ComponentRecord, RelationRef } from '~/types/component'
import { hookEnabled, hookEvents, relationRefs } from '~/types/component'

definePageMeta({ title: 'Components', fullBleed: true })

const UBadge = resolveComponent('UBadge')
const EntityBadge = resolveComponent('EntityBadge')

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

componentsStore.fetchAll(['hook'])

/** The components a hook watches, in relation order. */
function watchedBy(hook: ComponentRecord): RelationRef[] {
    return relationRefs(hook, 'watches')
}

const columns = computed<TableColumn<ComponentRecord>[]>(() => [
    nameColumn('Hook'),
    typeColumn(),
    {
        id: 'events',
        header: 'Events',
        accessorFn: (row: ComponentRecord) => hookEvents(row).join(', '),
        cell: ({ row }) => h('div', { class: 'flex flex-wrap gap-1' }, hookEvents(row.original).map(event =>
            h(UBadge, {
                key: event,
                color: 'neutral',
            }, () => event),
        )),
    },
    {
        id: 'watches',
        header: 'Watches',
        accessorFn: (row: ComponentRecord) => watchedBy(row).map(w => w.dst_name ?? w.dst_key).join(', '),
        cell: ({ row }) => {
            const watched = watchedBy(row.original)
            if (watched.length === 0) return h('span', { class: 'text-muted' }, '—')
            const first = watched[0]!
            return h(EntityBadge, {
                icon: componentIcon(first.dst_key),
                label: first.dst_name ?? first.dst_key,
                extra: watched.length - 1,
            })
        },
    },
    {
        id: 'status',
        header: 'Status',
        accessorFn: (row: ComponentRecord) => String(hookEnabled(row)),
        cell: ({ row }) => h(UBadge, {
            color: hookEnabled(row.original) ? 'success' : 'neutral',
        }, () => hookEnabled(row.original) ? 'Enabled' : 'Disabled'),
    },
    ...stateSchemaColumns(catalogStore.definitionsForKind('hook')[0]),
    dateColumn<ComponentRecord>('created_at', 'Created'),
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
    catch (e) {
        toast.add(inUseToast(e, 'Hook') ?? errorToast(e, 'Failed to delete hook'))
    }
}

const typeKey = ref<string | null>(null)
const enabled = ref<boolean | null>(null)

function matchesFilters(hook: ComponentRecord): boolean {
    return (typeKey.value === null || hook.key === typeKey.value)
        && (enabled.value === null || hookEnabled(hook) === enabled.value)
}
</script>

<template>
    <div>
        <NavActions>
            <UButton icon="i-lucide-plus"
                     label="New hook"
                     @click="handleCreate" />
        </NavActions>
        <NavComponentsHub>
            <DataTable :columns="columns"
                       :data="hooks"
                       :filter="matchesFilters"
                       :loading="componentsStore.loading"
                       :error="componentsStore.error"
                       :delete-impact="componentsStore.deleteImpact"
                       search-placeholder="Search hooks..."
                       @delete="handleDelete"
                       @edit="handleEdit"
                       @retry="componentsStore.fetchAll(['hook'])">
                <template #filters>
                    <TypeFilter v-model="typeKey"
                                :components="hooks" />
                    <EnabledFilter v-model="enabled" />
                </template>

                <template #empty>
                    <EmptyState icon="i-carbon-lightning"
                                title="No hooks yet"
                                description="A hook reacts to what your pipelines do. Watch a source, asset, or job and trigger downstream runs or notify an external system when runs complete or fail.">
                        <UButton icon="i-lucide-plus"
                                 label="New hook"
                                 class="mt-5"
                                 @click="handleCreate" />
                    </EmptyState>
                </template>
            </DataTable>
        </NavComponentsHub>

        <WizardDrawer v-model:open="drawerOpen"
                      :default-title="editingHook ? 'Edit Hook' : 'New Hook'"
                      description="Configure hook"
                      :stepper="stepperRef">
            <WizardDefinitionStepper v-if="drawerOpen"
                                     :key="editingHook?.id ?? 'new'"
                                     ref="stepperRef"
                                     kind="hook"
                                     noun="Hook"
                                     :component="editingHook"
                                     :relation-steps="[
                                         {
                                             name: 'watches',
                                             description: 'Components this hook observes — it fires when one of their runs matches the selected events.',
                                         },
                                         {
                                             name: 'targets',
                                             required: false,
                                             description: 'Components a trigger-style hook runs when it fires. Optional — leave empty for hooks that only notify.',
                                         },
                                     ]"
                                     @created="handleSaved"
                                     @updated="handleSaved" />
        </WizardDrawer>
    </div>
</template>
