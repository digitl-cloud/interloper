<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { ComponentRecord } from '~/types/component'
import { RESOURCE_KINDS } from '~/types/catalog'

definePageMeta({
    // A dynamic root route would otherwise swallow every unknown URL.
    validate: route => RESOURCE_KINDS.map(kindSlug).includes(route.params.kind as string),
})

const UBadge = resolveComponent('UBadge')
const UTooltip = resolveComponent('UTooltip')

const route = useRoute()
const catalogStore = useCatalogStore()
const organisationStore = useOrganisationStore()
const componentsStore = useComponentsStore()
const views = useComponentViews()
const appConfig = useAppConfig()
const { statusBadge } = useDrift()

const kind = computed(() => kindFromSlug(route.params.kind as string))
const kindLabel = computed(() => kind.value.charAt(0).toUpperCase() + kind.value.slice(1))
const pageTitle = computed(() => `${kindLabel.value}s`)

/** Available type definitions for this resource kind. */
const definitions = computed(() => catalogStore.definitionsForKind(kind.value))

/** Resources filtered by current kind. */
const resources = computed(() => componentsStore.byKind(kind.value))

const stepperRef = ref<any>(null)

const {
    open: drawerOpen,
    editing: editingResource,
    presetTypeKey,
    presetName,
    openCreate: handleCreate,
    openCreateWithType: handleCreateFromCatalog,
    openEdit: handleEdit,
} = useWizardDrawer<ComponentRecord>()

componentsStore.fetchAll([kind.value])
watch(kind, () => componentsStore.fetchAll([kind.value]))

// Deep link: `?new` opens the create wizard; `?new=<definition key>&name=<name>`
// opens it on that type with the name prefilled (the connection setup hand-off
// an MCP tool returns). `?edit=<id>` opens the edit wizard for that component
// (the overview's Reconnect link); an empty `?edit=` opens nothing. The query
// is consumed so a reload does not reopen it.
const router = useRouter()
watchEffect(() => {
    const key = route.query.new
    const edit = route.query.edit
    if (key === undefined && edit === undefined) return
    if (typeof edit === 'string' && edit) {
        componentsStore.fetchOne(edit)
            .then(handleEdit)
            .catch(e => toast.add(errorToast(e, "Couldn't open the connection")))
    }
    else if (edit === undefined) {
        const name = typeof route.query.name === 'string' ? route.query.name : undefined
        if (typeof key === 'string' && key) handleCreateFromCatalog(key, name)
        else handleCreate()
    }
    router.replace({ query: { ...route.query, new: undefined, name: undefined, edit: undefined } })
})

/**
 * Whether automatic renewal is on for this connection (config, default on).
 * An `unreadable` row disclosed no config at all, so its toggle is unknown
 * rather than on.
 */
function autoRenewColumn(): TableColumn<ComponentRecord> {
    const sortValue = (row: ComponentRecord) => {
        if (!catalogStore.catalog[row.key]?.renewable) return ''
        return row.status === 'unreadable' ? 'unknown' : String(row.config?.auto_renew !== false)
    }
    return {
        id: 'auto_renew',
        header: 'Auto renew',
        accessorFn: sortValue,
        cell: ({ row }) => {
            if (!catalogStore.catalog[row.original.key]?.renewable) return h('span', { class: 'text-dimmed' }, '—')
            if (row.original.status === 'unreadable') {
                return h(UTooltip, { text: 'The stored config could not be read' }, () =>
                    h(UBadge, { color: 'warning', icon: appConfig.ui.icons.warning }, () => 'Unknown'))
            }
            const enabled = row.original.config?.auto_renew !== false
            return h(UBadge, {
                color: enabled ? 'success' : 'neutral',
                icon: enabled ? appConfig.icons.refresh : undefined,
            }, () => enabled ? 'On' : 'Off')
        },
    }
}

/** Connection renewal state reads as one badge, like the collection's Last run. */
function lastRenewedColumn(): TableColumn<ComponentRecord> {
    return {
        id: 'last_renewed_at',
        header: 'Last renewed',
        accessorFn: (row: ComponentRecord) => row.state?.last_renewed_at ?? '',
        cell: ({ row }) => {
            const state = row.original.state ?? {}
            if (state.last_renewal_error) {
                return h(UTooltip, { text: state.last_renewal_error }, () =>
                    h(UBadge, { color: 'error', icon: appConfig.ui.icons.error }, () => 'Failed'))
            }
            if (!state.last_renewed_at) return h('span', { class: 'text-dimmed' }, '—')
            return h(UBadge, { color: 'success', icon: appConfig.ui.icons.success }, () =>
                h('span', { class: 'tabular-nums' }, relativeTime(new Date(state.last_renewed_at))))
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
            .filter(column => column.id !== 'last_renewal_error')
            .map(column => (column.id === 'last_renewed_at' ? lastRenewedColumn() : column)),
    ]
})

const columns = computed<TableColumn<ComponentRecord>[]>(() => [
    nameColumn(kindLabel.value, row => statusBadge(row.status)),
    typeColumn(),
    ...tableStateColumns.value,
    dateColumn<ComponentRecord>('created_at', 'Created'),
])

// ── Renewal ──

function rowActions(item: ComponentRecord): DropdownMenuItem[][] {
    if (kind.value !== 'connection' || !catalogStore.catalog[item.key]?.renewable) return []
    return [[{
        label: 'Renew now',
        icon: appConfig.icons.refresh,
        onSelect: () => renewNow(item),
    }]]
}

async function renewNow(item: ComponentRecord) {
    try {
        await runsStore.createRun(item.id)
        toast.add({ title: `Renewal queued for ${item.name ?? catalogStore.typeName(item.key)}`, color: 'success' })
    }
    catch (e) {
        toast.add(errorToast(e, "Couldn't queue the renewal"))
    }
}

const toast = useToast()
const runsStore = useRunsStore()

async function handleDelete(ids: string[]) {
    try {
        await componentsStore.remove(ids)
        toast.add({ title: `${ids.length} ${ids.length === 1 ? kind.value : kindSlug(kind.value)} deleted`, color: 'success' })
    }
    catch (e) {
        toast.add(inUseToast(e, pageTitle.value.slice(0, -1)) ?? errorToast(e, `Couldn't delete the ${kind.value}`))
    }
}

function handleSaved() {
    componentsStore.fetchAll([kind.value])
    drawerOpen.value = false
}

// ── Empty state ──

const EMPTY_COPY: Record<string, { hero: string, catalogTitle: string, catalogDesc: string }> = {
    connection: {
        hero: 'A connection is a securely-stored credential — an OAuth token, API key or service account — that '
            + 'Interloper uses to authenticate with a platform. Create one, and every source from that provider '
            + 'can reuse it. Credentials are encrypted at rest and never leave your cloud.',
        catalogTitle: 'Connection catalog',
        catalogDesc: 'Pick a platform to store a reusable credential — every source from that provider can share it.',
    },
}

const emptyCopy = computed(() => EMPTY_COPY[kind.value] ?? {
    hero: `Create your first ${kind.value} to get started.`,
    catalogTitle: 'Available types',
    catalogDesc: `Pick a type to create a new ${kind.value}.`,
})

const typeKey = useQueryParam('type', null)
</script>

<template>
    <UDashboardPanel id="kind">
        <template #header>
            <AppNavbar title="Components" />
            <UDashboardToolbar>
                <template #left>
                    <PageTabs :items="views" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <DriftBanner :kind="kind" />
            <DataTable :columns="columns"
                       fill
                       :data="resources"
                       :filter="row => typeKey === null || row.key === typeKey"
                       :loading="componentsStore.loading"
                       :error="componentsStore.error"
                       :delete-impact="componentsStore.deleteImpact"
                       :row-actions="rowActions"
                       :search-placeholder="`Search ${pageTitle.toLowerCase()}…`"
                       @delete="handleDelete"
                       @edit="handleEdit"
                       @retry="componentsStore.fetchAll([kind])">
                <template #filters>
                    <TypeFilter v-model="typeKey"
                                :components="resources" />
                </template>

                <template #empty>
                    <div class="flex flex-col gap-4 sm:gap-6">
                        <UEmpty :icon="resourceKindIcon(kind)"
                                :title="`No ${pageTitle.toLowerCase()} yet`"
                                :description="emptyCopy.hero"
                                :actions="[{ label: `New ${kind}`, icon: appConfig.ui.icons.plus, onClick: handleCreate }]" />

                        <UPageCard variant="naked"
                                   orientation="horizontal"
                                   :title="emptyCopy.catalogTitle"
                                   :description="emptyCopy.catalogDesc" />
                        <div class="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
                            <CatalogCard v-for="def in definitions"
                                         :key="def.key"
                                         variant="compact"
                                         :icon="componentIcon(def.key)"
                                         :title="def.name"
                                         :maturity="def.maturity"
                                         :caption="def.provider"
                                         @click="handleCreateFromCatalog(def.key)" />
                        </div>

                        <UAlert v-if="kind === 'connection'"
                                color="neutral"
                                :icon="appConfig.ui.icons.info"
                                description="You can also create a connection inline while adding a source, whichever you reach first."
                                orientation="horizontal"
                                :actions="[{
                                    label: 'Browse sources',
                                    color: 'neutral',
                                    variant: 'link',
                                    trailingIcon: appConfig.ui.icons.arrowRight,
                                    to: organisationStore.path(kindPath('source')),
                                }]" />
                    </div>
                </template>
                <template #actions>
                    <UButton :icon="appConfig.ui.icons.plus"
                             :label="`New ${kind}`"
                             @click="handleCreate" />
                </template>
            </DataTable>

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
                                         :initial-name="presetName"
                                         :definitions="definitions"
                                         :credentials-label="kind === 'connection' ? 'Credentials' : undefined"
                                         @created="handleSaved"
                                         @updated="handleSaved" />
            </WizardDrawer>
        </template>
    </UDashboardPanel>
</template>
