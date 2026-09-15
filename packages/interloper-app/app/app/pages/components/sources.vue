<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { ComponentRecord } from '~/types/component'
import { relationIds } from '~/types/component'

definePageMeta({ title: 'Components', fullBleed: true })

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

// Deep link (command palette): /sources?new=1 opens the create wizard.
const route = useRoute()
const router = useRouter()
watchEffect(() => {
    if (route.query.new === undefined) return
    handleCreate()
    router.replace({ query: { ...route.query, new: undefined } })
})

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
    catch (e) {
        toast.add(errorToast(e, 'Failed to queue run'))
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

/** The destination components a source is bound to, in relation order. */
function destinationsOf(source: ComponentRecord): ComponentRecord[] {
    return relationIds(source, 'destinations')
        .map(id => componentsStore.byId(id))
        .filter((d): d is ComponentRecord => !!d)
}

const columns: TableColumn<ComponentRecord>[] = [
    {
        id: 'name',
        header: 'Source',
        // The discriminator rides the accessor so the search box matches an account id as well as its name.
        accessorFn: (row: ComponentRecord) => [row.name, row.discriminator].filter(Boolean).join(' '),
        cell: ({ row }) => {
            const { name, discriminator } = row.original
            const children: any[] = [
                h(UIcon, { name: componentIcon(row.original.key), class: 'size-4 shrink-0' }),
                name,
            ]
            const badge = statusBadge(sourceDrift(row.original))
            if (badge) {
                children.push(h(UBadge, {
                    color: badge.color,
                    size: 'sm',
                    class: 'ml-1',
                }, () => h('span', { class: 'flex items-center gap-1' }, [
                    h(UIcon, { name: badge.icon, class: 'size-3 shrink-0' }),
                    badge.label,
                ])))
            }
            const title = h('span', { class: 'flex items-center gap-2' }, children)
            if (!discriminator || discriminator === name) return title
            return h('span', { class: 'flex flex-col gap-0.5' }, [
                title,
                h('span', { class: 'text-xs text-muted font-mono pl-6' }, discriminator),
            ])
        },
    },
    {
        id: 'type',
        header: 'Type',
        accessorFn: (row: ComponentRecord) => typeName(row.key),
        cell: ({ row }) => h('span', { class: 'flex items-center gap-1.5 text-muted' }, [
            h(UIcon, { name: componentIcon(row.original.key), class: 'size-4 shrink-0' }),
            typeName(row.original.key),
        ]),
    },
    {
        id: 'assets',
        header: 'Assets',
        accessorFn: (row: ComponentRecord) => row.children.length,
        cell: ({ row }) => h('span', { class: 'text-muted' },
            `${row.original.children.length} asset${row.original.children.length !== 1 ? 's' : ''}`),
    },
    {
        id: 'destinations',
        header: 'Destinations',
        accessorFn: (row: ComponentRecord) => destinationsOf(row).map(d => d.name ?? d.key).join(', '),
        cell: ({ row }) => {
            const dests = destinationsOf(row.original)
            if (dests.length === 0) return h('span', { class: 'text-muted' }, '—')
            const first = dests[0]!
            return h(EntityBadge, {
                icon: componentIcon(first.key),
                label: first.name ?? catalogStore.catalog[first.key]?.name ?? first.key,
                extra: dests.length - 1,
            })
        },
    },
    dateColumn<ComponentRecord>('created_at', 'Created'),
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
        toast.add(inUseToast(e, 'Source') ?? errorToast(e, 'Failed to delete source'))
    }
}

const typeKey = ref<string | null>(null)
</script>

<template>
    <div>
        <NavActions>
            <UButton icon="i-lucide-plus"
                     label="New source"
                     @click="handleCreate" />
        </NavActions>
        <NavComponentsHub>
            <DriftBanner />
            <DataTable :columns="columns"
                       :data="sources"
                       :filter="row => typeKey === null || row.key === typeKey"
                       :loading="componentsStore.loading"
                       :error="componentsStore.error"
                       :row-actions="rowActions"
                       :delete-impact="componentsStore.deleteImpact"
                       search-placeholder="Search sources..."
                       @delete="handleDelete"
                       @edit="handleEdit"
                       @retry="componentsStore.reload()">
                <template #filters>
                    <TypeFilter v-model="typeKey"
                                :components="sources" />
                </template>

                <template #empty>
                    <SourcesEmptyState @create="handleCreate"
                                       @create-type="handleCreateFromCatalog" />
                </template>
            </DataTable>
        </NavComponentsHub>

        <WizardDrawer v-model:open="drawerOpen"
                      default-title="New Source"
                      description="Configure a new source"
                      :stepper="stepperRef">
            <SourcesWizard v-if="drawerOpen"
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
