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

const defaultChips = computed(() => (quotas.value?.fields ?? []).map(field => ({
    key: field.key,
    value: field.default != null ? field.default.toLocaleString() : 'unlimited',
})))

/** The quota closest to its ceiling — what the usage column reports. */
function peakQuota(usage: AdminOrgQuotaStatus) {
    const candidates = [
        { label: 'Sources', used: usage.sources, limit: usage.effective.max_sources },
        { label: 'Assets / source', used: usage.max_assets_per_source, limit: usage.effective.max_assets_per_source },
        { label: 'Runs', used: usage.successful_runs, limit: usage.effective.max_successful_runs_per_month },
    ].filter((entry): entry is { label: string, used: number, limit: number } =>
        entry.limit != null && entry.limit > 0)
    if (!candidates.length) return null
    return candidates.reduce((a, b) => (b.used / b.limit > a.used / a.limit ? b : a))
}

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
        id: 'quota',
        header: 'Highest quota usage',
        cell: ({ row }) => {
            const usage = usageByOrg.value.get(row.original.id)
            if (row.original.deleted_at || !usage) return dash()
            const peak = peakQuota(usage)
            if (!peak) return dash()
            return h('div', { class: 'flex flex-col gap-0.5' }, [
                h('div', { class: 'w-32' }, h(AdminUsageMeter, { used: peak.used, limit: peak.limit })),
                h('span', { class: 'text-[11.5px] text-dimmed whitespace-nowrap' },
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
            const badge = drift
                ? h(UBadge, { label: 'Drift', color: 'warning', variant: 'subtle', icon: 'i-lucide-triangle-alert' })
                : h(UBadge, { label: 'In sync', color: 'success', variant: 'subtle', icon: 'i-lucide-check' })
            if (!drift) return badge
            return h('div', { class: 'flex flex-col gap-0.5 items-start' }, [
                badge,
                h('span', { class: 'text-[11.5px] text-dimmed whitespace-nowrap' },
                    `runs table: ${usage.recomputed_successful_runs.toLocaleString()}`),
            ])
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
             class="flex flex-wrap items-center gap-2.5 text-sm text-muted shrink-0">
            <span class="flex items-center gap-1.5">
                <UIcon name="i-lucide-calendar"
                       class="size-4" />
                Quota usage for <b class="text-default font-semibold">{{ periodLabel }}</b>
            </span>
            <span class="size-[3px] rounded-full bg-accented" />
            <span>Instance defaults</span>
            <span v-for="chip in defaultChips"
                  :key="chip.key"
                  class="rounded-md border border-default bg-default px-2 py-0.5 font-mono text-xs text-toned">
                {{ chip.key }}=<b class="font-semibold">{{ chip.value }}</b>
            </span>
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
