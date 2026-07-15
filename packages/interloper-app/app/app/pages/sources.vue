<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { ComponentRecord } from '~/types/component'
import { relationIds } from '~/types/component'

definePageMeta({ title: 'Sources' })

const UIcon = resolveComponent('UIcon')
const UBadge = resolveComponent('UBadge')
const EntityBadge = resolveComponent('EntityBadge')

const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()
const { statusBadge, sourceDrift } = useDrift()

const sources = computed(() => componentsStore.byKind('source'))

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
} = useWizardDrawer<ComponentRecord>()

componentsStore.fetchAll()
componentsStore.fetchRelations()

// ── Run now ─────────────────────────────────────────────────────

const runsStore = useRunsStore()
const runModalOpen = ref(false)
const runModalSource = ref<ComponentRecord | null>(null)

/** Whether any of the source's assets is partitioned (then a partition date is prompted). */
function sourcePartitioned(source: ComponentRecord): boolean {
    const defn = catalogStore.getSourceDefinition(source.key)
    return defn?.assets?.some(a => a.partitioning) ?? false
}

async function runNow(source: ComponentRecord) {
    if (sourcePartitioned(source)) {
        runModalSource.value = source
        runModalOpen.value = true
        return
    }
    try {
        const runId = await runsStore.createRun(source.id)
        toast.add({ title: `Run queued (${runId.slice(0, 8)})`, color: 'success' })
    }
    catch {
        toast.add({ title: 'Failed to queue run', color: 'error' })
    }
}

function rowActions(source: ComponentRecord): DropdownMenuItem[][] {
    return [[
        {
            label: 'Run now',
            icon: 'i-lucide-play',
            onSelect: () => runNow(source),
        },
    ]]
}

const columns: TableColumn<ComponentRecord>[] = [
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
        cell: ({ row }) => h('span', { class: 'flex items-center gap-1.5 text-muted' }, [
            h(UIcon, { name: componentIcon(row.original.key), class: 'size-4 shrink-0' }),
            typeName(row.original.key),
        ]),
    },
    {
        accessorKey: 'assets',
        header: 'Assets',
        cell: ({ row }) => h('span', { class: 'text-muted' },
            `${row.original.children.length} asset${row.original.children.length !== 1 ? 's' : ''}`),
    },
    {
        accessorKey: 'destinations',
        header: 'Destinations',
        cell: ({ row }) => {
            const dests = relationIds(row.original, 'destination')
                .map(id => componentsStore.byId(id))
                .filter((d): d is ComponentRecord => !!d)
            if (dests.length === 0) return h('span', { class: 'text-muted' }, '—')
            const first = dests[0]!
            return h(EntityBadge, {
                icon: componentIcon(first.key),
                label: first.name ?? catalogStore.catalog[first.key]?.name ?? first.key,
                extra: dests.length - 1,
            })
        },
    },
    {
        accessorKey: 'created_at',
        header: 'Created',
        accessorFn: (row: ComponentRecord) => row.created_at ? formatDate(row.created_at) : '—',
    },
]

function handleSaved() {
    componentsStore.fetchAll(['source'])
    drawerOpen.value = false
}

async function handleDelete(ids: string[]) {
    try {
        await componentsStore.remove(ids)
        toast.add({ title: `${ids.length} source${ids.length > 1 ? 's' : ''} deleted`, color: 'success' })
    }
    catch (e) {
        toast.add(inUseToast(e, 'Source') ?? { title: 'Failed to delete source', color: 'error' })
    }
}
</script>

<template>
    <div>
        <DriftBanner />

        <div class="flex flex-col flex-1 min-h-0">
            <DataTable :columns="columns"
                       :data="sources"
                       :loading="componentsStore.loading"
                       :row-actions="rowActions"
                       :delete-impact="componentsStore.deleteImpact"
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

        <ExecutionsRunModal v-if="runModalSource"
                  v-model:open="runModalOpen"
                  :target="runModalSource"
                  partitioned />
    </div>
</template>
