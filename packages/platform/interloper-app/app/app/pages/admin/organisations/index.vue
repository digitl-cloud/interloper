<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { AdminOrganisation, AdminOrgQuotaStatus, AdminQuotas } from '~/types/admin'

definePageMeta({
    layout: 'admin',
    middleware: 'super-admin',
})

const UBadge = resolveComponent('UBadge')
const AdminUsageMeter = resolveComponent('AdminUsageMeter')

const adminStore = useAdminStore()
const appConfig = useAppConfig()
const toast = useToast()

const rows = ref<AdminOrganisation[]>([])
const quotas = ref<AdminQuotas | null>(null)
const loading = ref(false)
const error = ref<Error | null>(null)

// Rename and delete live on the detail page's Settings tab.
const createOpen = ref(false)
const createState = reactive({ name: '' })
const creating = ref(false)

async function loadData() {
    loading.value = true
    error.value = null
    try {
        [rows.value, quotas.value] = await Promise.all([
            adminStore.listOrganisations(),
            adminStore.getQuotas(),
        ])
    }
    catch (err) {
        error.value = err as Error
    }
    finally {
        loading.value = false
    }
}

/** Usage columns come from the quotas overview; the rest from the org row. */
const usageByOrg = computed(() => new Map<string, AdminOrgQuotaStatus>(
    (quotas.value?.organisations ?? []).map(row => [row.id, row])))

function openCreate() {
    createState.name = ''
    createOpen.value = true
}

async function submitCreate() {
    const name = createState.name.trim()
    if (!name) return

    creating.value = true
    try {
        await adminStore.createOrganisation(name)
        toast.add({ title: `Organisation "${name}" created`, color: 'success' })
        createOpen.value = false
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, "Couldn't create the organisation"))
    }
    finally {
        creating.value = false
    }
}

function openOrg(org: AdminOrganisation) {
    if (org.deleted_at) return
    navigateTo(`/admin/organisations/${org.id}`)
}

const dash = () => h('span', { class: 'text-dimmed' }, '—')

const columns: TableColumn<AdminOrganisation>[] = [
    {
        accessorKey: 'name',
        header: 'Name',
        cell: ({ row }) => {
            const org = row.original
            if (!org.deleted_at) return h('span', { class: 'font-medium' }, org.name)
            return h('div', { class: 'flex items-center gap-2' }, [
                h('span', { class: 'text-dimmed line-through' }, org.name),
                h(UBadge, { label: 'Deleted', color: 'neutral', size: 'sm' }),
            ])
        },
    },
    {
        accessorKey: 'member_count',
        header: 'Members',
        cell: ({ row }) => row.original.deleted_at
            ? dash()
            : h('span', { class: 'text-muted tabular-nums' }, String(row.original.member_count)),
    },
    {
        id: 'sources',
        header: 'Sources',
        cell: ({ row }) => {
            const usage = usageByOrg.value.get(row.original.id)
            if (row.original.deleted_at || !usage) return dash()
            return h('span', { class: 'text-muted tabular-nums' }, String(usage.sources))
        },
    },
    {
        id: 'quota',
        header: 'Highest quota usage',
        cell: ({ row }) => {
            const usage = usageByOrg.value.get(row.original.id)
            if (row.original.deleted_at || !usage) return dash()
            const peak = peakQuota(usage)
            if (!peak) return dash()
            return h('div', { class: 'flex flex-col gap-0.5' }, [
                h('div', { class: 'w-32' }, h(AdminUsageMeter, { used: peak.used, limit: peak.limit })),
                h('span', { class: 'text-xs text-dimmed whitespace-nowrap tabular-nums' },
                    `${peak.label} ${peak.used.toLocaleString()} / ${peak.limit.toLocaleString()}`),
            ])
        },
    },
    {
        id: 'ledger',
        header: 'Ledger',
        cell: ({ row }) => {
            const usage = usageByOrg.value.get(row.original.id)
            if (row.original.deleted_at || !usage) return dash()
            const drift = usage.successful_runs !== usage.recomputed_successful_runs
            return drift
                ? h(UBadge, { label: 'Drift', color: 'warning', icon: appConfig.ui.icons.warning })
                : h(UBadge, { label: 'In sync', color: 'success', icon: appConfig.ui.icons.success })
        },
    },
    {
        accessorKey: 'created_at',
        header: 'Created',
        cell: ({ row }) => h('span', { class: 'text-muted tabular-nums' }, formatDay(row.original.created_at)),
    },
]

onMounted(loadData)
</script>

<template>
    <UDashboardPanel id="admin-organisations">
        <template #header>
            <AppNavbar title="Organisations" />
        </template>
        <template #body>
            <DataTable :columns="columns"
                       fill
                       :data="rows"
                       :loading="loading"
                       :error="error"
                       no-actions
                       search-placeholder="Search organisations…"
                       @edit="openOrg"
                       @retry="loadData">
                <template #actions>
                    <UButton :icon="appConfig.ui.icons.plus"
                             label="New organisation"
                             @click="openCreate" />
                </template>
            </DataTable>

            <UModal v-model:open="createOpen"
                    title="New organisation">
                <template #body>
                    <UForm id="create-organisation"
                           :state="createState"
                           @submit="submitCreate">
                        <UFormField label="Name"
                                    name="name"
                                    required>
                            <UInput v-model="createState.name"
                                    autofocus
                                    class="w-full" />
                        </UFormField>
                    </UForm>
                </template>
                <template #footer>
                    <UButton label="Cancel"
                             color="neutral"
                             variant="outline"
                             @click="createOpen = false" />
                    <UButton label="Create organisation"
                             type="submit"
                             form="create-organisation"
                             :disabled="!createState.name.trim() || creating"
                             :loading="creating" />
                </template>
            </UModal>
        </template>
    </UDashboardPanel>
</template>
