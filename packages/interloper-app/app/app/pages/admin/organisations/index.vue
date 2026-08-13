<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { AdminOrganisation, AdminOrgQuotaStatus, AdminQuotas } from '~/types/admin'

definePageMeta({
    title: 'Organisations',
    layout: 'admin',
    middleware: 'super-admin',
})

const UBadge = resolveComponent('UBadge')
const AdminUsageMeter = resolveComponent('AdminUsageMeter')

const adminStore = useAdminStore()
const toast = useToast()

const rows = ref<AdminOrganisation[]>([])
const quotas = ref<AdminQuotas | null>(null)
const loading = ref(false)

// Create modal state — rename and delete live on the detail page's Settings tab.
const createOpen = ref(false)
const createName = ref('')
const creating = ref(false)

async function loadData() {
    loading.value = true
    try {
        [rows.value, quotas.value] = await Promise.all([
            adminStore.listOrganisations(),
            adminStore.getQuotas(),
        ])
    }
    catch (err) {
        console.error('[Admin] Failed to load organisations', err)
    }
    finally {
        loading.value = false
    }
}

/** Usage columns come from the quotas overview; the rest from the org row. */
const usageByOrg = computed(() => new Map<string, AdminOrgQuotaStatus>(
    (quotas.value?.organisations ?? []).map(row => [row.id, row])))

const periodLabel = computed(() => {
    if (!quotas.value) return ''
    return new Date(quotas.value.period_start).toLocaleDateString(undefined, { month: 'long', year: 'numeric' })
})

function openCreate() {
    createName.value = ''
    createOpen.value = true
}

async function submitCreate() {
    const name = createName.value.trim()
    if (!name) return

    creating.value = true
    try {
        await adminStore.createOrganisation(name)
        toast.add({ title: `Organisation "${name}" created`, color: 'success' })
        createOpen.value = false
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to create organisation'))
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
                h(UBadge, { label: 'Deleted', color: 'neutral', variant: 'subtle', size: 'sm' }),
            ])
        },
    },
    {
        accessorKey: 'member_count',
        header: 'Members',
        cell: ({ row }) => row.original.deleted_at
            ? dash()
            : h('span', { class: 'text-muted' }, String(row.original.member_count)),
    },
    {
        id: 'sources',
        header: 'Sources',
        cell: ({ row }) => {
            const usage = usageByOrg.value.get(row.original.id)
            if (row.original.deleted_at || !usage) return dash()
            return h('span', { class: 'text-muted' }, String(usage.sources))
        },
    },
    {
        id: 'runs',
        header: 'Runs',
        cell: ({ row }) => {
            const usage = usageByOrg.value.get(row.original.id)
            if (row.original.deleted_at || !usage) return dash()
            return h('span', { class: 'text-muted tabular-nums' }, usage.successful_runs.toLocaleString())
        },
    },
    {
        id: 'quota',
        header: 'Run quota used',
        cell: ({ row }) => {
            const usage = usageByOrg.value.get(row.original.id)
            const limit = usage?.effective.max_successful_runs_per_month ?? null
            if (row.original.deleted_at || !usage || limit == null) return dash()
            return h('div', { class: 'w-32' }, h(AdminUsageMeter, { used: usage.successful_runs, limit }))
        },
    },
    {
        accessorKey: 'created_at',
        header: 'Created',
        cell: ({ row }) => h('span', { class: 'text-muted' }, formatDay(row.original.created_at)),
    },
]

onMounted(loadData)
</script>

<template>
    <div class="flex flex-col flex-1 min-h-0 gap-3">
        <div v-if="quotas"
             class="flex items-center gap-2 text-sm text-muted shrink-0">
            <UIcon name="i-lucide-calendar"
                   class="size-4" />
            <span>Runs and usage shown for the current quota period, <b class="text-default font-semibold">{{ periodLabel }}</b></span>
        </div>

        <DataTable :columns="columns"
                   :data="rows"
                   :loading="loading"
                   no-actions
                   search-placeholder="Search organisations..."
                   @edit="openOrg">
            <template #toolbar>
                <UButton icon="i-lucide-plus"
                         label="New organisation"
                         @click="openCreate" />
            </template>
        </DataTable>

        <UModal v-model:open="createOpen"
                title="New organisation"
                :ui="{ footer: 'justify-end' }">
            <template #body>
                <UInput v-model="createName"
                        placeholder="Organisation name"
                        autofocus
                        class="w-full"
                        @keydown.enter="submitCreate" />
            </template>
            <template #footer>
                <UButton label="Cancel"
                         color="neutral"
                         variant="outline"
                         @click="createOpen = false" />
                <UButton label="Create"
                         :disabled="!createName.trim() || creating"
                         :loading="creating"
                         @click="submitCreate" />
            </template>
        </UModal>
    </div>
</template>
