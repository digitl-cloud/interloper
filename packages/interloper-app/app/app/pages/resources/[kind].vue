<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { ComponentRecord } from '~/types/component'

definePageMeta({ title: 'Resources' })

const UIcon = resolveComponent('UIcon')
const UBadge = resolveComponent('UBadge')
const UTooltip = resolveComponent('UTooltip')

const route = useRoute()
const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()

const kind = computed(() => route.params.kind as string)
const kindLabel = computed(() => kind.value.charAt(0).toUpperCase() + kind.value.slice(1))
const pageTitle = computed(() => `${kindLabel.value}s`)

/** Available type definitions for this resource kind. */
const definitions = computed(() => catalogStore.definitionsForKind(kind.value))

/** Resources filtered by current kind. */
const resources = computed(() => componentsStore.byKind(kind.value))

function typeName(key: string): string {
    return catalogStore.catalog[key]?.name ?? key
}

const stepperRef = ref<any>(null)

const {
    open: drawerOpen,
    editing: editingResource,
    presetTypeKey,
    openCreate: handleCreate,
    openCreateWithType: handleCreateFromCatalog,
    openEdit: handleEdit,
} = useWizardDrawer<ComponentRecord>()

// Everything once — the delete preview needs referrer records and relations
// of every kind — then a per-kind refresh when switching resource kinds.
componentsStore.fetchAll()
componentsStore.fetchRelations()
watch(kind, () => componentsStore.fetchAll([kind.value]))

/** Whether automatic renewal is on for this connection (config, default on). */
function autoRenewColumn(): TableColumn<ComponentRecord> {
    return {
        accessorKey: 'auto_renew',
        header: 'Auto renew',
        accessorFn: (row: ComponentRecord) =>
            catalogStore.catalog[row.key]?.renewable ? String(row.config?.auto_renew !== false) : '',
        cell: ({ row }) => {
            if (!catalogStore.catalog[row.original.key]?.renewable) return h('span', { class: 'text-dimmed' }, '—')
            const enabled = row.original.config?.auto_renew !== false
            return h(UBadge, {
                color: enabled ? 'success' : 'neutral',
                icon: enabled ? 'i-lucide-refresh-cw' : 'i-lucide-pause',
            }, () => enabled ? 'On' : 'Off')
        },
    }
}

/** Connection renewal state reads as one badge, like the collection's Last run. */
function lastRenewedColumn(): TableColumn<ComponentRecord> {
    return {
        accessorKey: 'last_renewed_at',
        header: 'Last renewed',
        accessorFn: (row: ComponentRecord) => row.state?.last_renewed_at ?? '',
        cell: ({ row }) => {
            const state = row.original.state ?? {}
            if (state.last_renewal_error) {
                return h(UTooltip, { text: state.last_renewal_error }, () =>
                    h(UBadge, { color: 'error', icon: 'i-lucide-x' }, () => 'Failed'))
            }
            if (!state.last_renewed_at) return h('span', { class: 'text-dimmed' }, '—')
            return h(UBadge, { color: 'success', icon: 'i-lucide-check' }, () =>
                `${timeSince(new Date(state.last_renewed_at))} ago`)
        },
    }
}

// State columns (e.g. a connection's renewal timestamps) come from the
// kind's state schema; every definition of a kind shares its anchor's state
// model, so the first one stands in for them all. Connections fold the
// renewal state into one badge: the error rides the badge's tooltip.
const tableStateColumns = computed<TableColumn<ComponentRecord>[]>(() => {
    const stateColumns = stateSchemaColumns(definitions.value[0])
    if (kind.value !== 'connection') return stateColumns
    return [
        autoRenewColumn(),
        ...stateColumns
            .filter(column => (column as { accessorKey?: string }).accessorKey !== 'last_renewal_error')
            .map(column =>
                (column as { accessorKey?: string }).accessorKey === 'last_renewed_at' ? lastRenewedColumn() : column),
    ]
})

const columns = computed<TableColumn<ComponentRecord>[]>(() => [
    { accessorKey: 'name', header: 'Name' },
    {
        accessorKey: 'key',
        header: 'Type',
        cell: ({ row }) => h('span', { class: 'flex items-center gap-1.5 text-muted' }, [
            h(UIcon, { name: componentIcon(row.original.key), class: 'size-4 shrink-0' }),
            typeName(row.original.key),
        ]),
    },
    ...tableStateColumns.value,
    {
        accessorKey: 'created_at',
        header: 'Created',
        accessorFn: (row: ComponentRecord) => row.created_at ? formatDate(row.created_at) : '-',
    },
])

// ── Renewal ──

function rowActions(item: ComponentRecord): DropdownMenuItem[][] {
    if (kind.value !== 'connection' || !catalogStore.catalog[item.key]?.renewable) return []
    return [[{
        label: 'Renew now',
        icon: 'i-lucide-refresh-cw',
        onSelect: () => renewNow(item),
    }]]
}

async function renewNow(item: ComponentRecord) {
    try {
        await runsStore.createRun(item.id)
        toast.add({ title: `Renewal queued for ${item.name ?? typeName(item.key)}`, color: 'success' })
    }
    catch (e) {
        toast.add(errorToast(e, 'Failed to queue renewal'))
    }
}

const toast = useToast()
const runsStore = useRunsStore()

async function handleDelete(ids: string[]) {
    try {
        await componentsStore.remove(ids)
        toast.add({ title: `${ids.length} resource(s) deleted`, color: 'success' })
    }
    catch (e) {
        toast.add(inUseToast(e, pageTitle.value.slice(0, -1)) ?? errorToast(e, 'Failed to delete resource'))
    }
}

function handleSaved() {
    componentsStore.fetchAll([kind.value])
    drawerOpen.value = false
}

// ── Empty state ──

const KIND_ICONS: Record<string, string> = {
    connection: 'i-lucide-key-round',
    config: 'i-lucide-settings',
}

const EMPTY_COPY: Record<string, { hero: string, catalogTitle: string, catalogDesc: string }> = {
    connection: {
        hero: 'A connection is a securely-stored credential — an OAuth token, API key or service account — that '
            + 'Interloper uses to authenticate with a platform. Create one, and every source from that provider '
            + 'can reuse it. Credentials are encrypted at rest and never leave your cloud.',
        catalogTitle: 'Connections Catalog',
        catalogDesc: 'Pick a platform to store a reusable credential — every source from that provider can share it.',
    },
}

const emptyCopy = computed(() => EMPTY_COPY[kind.value] ?? {
    hero: `Create your first ${kind.value} to get started.`,
    catalogTitle: 'Available types',
    catalogDesc: `Pick a type to create a new ${kind.value}.`,
})
</script>

<template>
    <div>
        <NavActions>
            <UButton icon="i-lucide-plus"
                     :label="`New ${kind}`"
                     @click="handleCreate" />
        </NavActions>
        <div class="flex flex-col flex-1 min-h-0">
            <DataTable :columns="columns"
                       :data="resources"
                       :loading="componentsStore.loading"
                       :delete-impact="componentsStore.deleteImpact"
                       :row-actions="rowActions"
                       :search-placeholder="`Search ${pageTitle.toLowerCase()}...`"
                       @delete="handleDelete"
                       @edit="handleEdit">
                <template #empty>
                    <EmptyState :icon="KIND_ICONS[kind] ?? 'i-lucide-box'"
                                :title="`No ${pageTitle.toLowerCase()} yet`"
                                :description="emptyCopy.hero">
                        <UButton icon="i-lucide-plus"
                                 :label="`New ${kind}`"
                                 class="mt-5"
                                 @click="handleCreate" />
                    </EmptyState>

                    <div class="mt-9 mb-3.5">
                        <h2 class="text-lg font-bold tracking-[-0.015em] text-highlighted">
                            {{ emptyCopy.catalogTitle }}
                        </h2>
                        <p class="text-sm text-muted mt-1.5">{{ emptyCopy.catalogDesc }}</p>
                    </div>
                    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                        <CatalogCard v-for="def in definitions"
                                     :key="def.key"
                                     variant="compact"
                                     :icon="componentIcon(def.key)"
                                     :title="def.name"
                                     :caption="def.provider"
                                     @click="handleCreateFromCatalog(def.key)" />
                    </div>

                    <div v-if="kind === 'connection'"
                         class="flex items-center gap-3 border border-default rounded-lg px-5 py-4 bg-(--ui-bg-band) mt-6">
                        <UIcon name="i-lucide-info"
                               class="size-5 text-primary shrink-0" />
                        <div class="flex-1 text-[13.5px] text-toned leading-normal">
                            You can also create a connection inline while adding a source — whichever you reach first.
                            Ready to connect data?
                            <NuxtLink to="/sources"
                                      class="text-primary font-semibold">Browse sources →</NuxtLink>
                        </div>
                    </div>
                </template>
            </DataTable>
        </div>

        <WizardDrawer v-model:open="drawerOpen"
                      modal
                      :default-title="`New ${kind}`"
                      :description="`${editingResource ? 'Edit' : 'Configure a new'} ${kind}`"
                      :stepper="stepperRef">
            <WizardDefinitionStepper v-if="drawerOpen"
                                     :key="editingResource?.id ?? 'new'"
                                     ref="stepperRef"
                                     :kind="kind"
                                     :noun="kindLabel"
                                     :component="editingResource"
                                     :initial-type-key="presetTypeKey"
                                     :definitions="definitions"
                                     :config-label="kind === 'connection' ? 'Credentials' : 'Configuration'"
                                     @created="handleSaved"
                                     @updated="handleSaved" />
        </WizardDrawer>
    </div>
</template>
