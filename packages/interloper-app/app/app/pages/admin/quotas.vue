<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { AdminOrgQuotaStatus, AdminQuotas } from '~/types/admin'

definePageMeta({
    title: 'Quotas',
    layout: 'admin',
    middleware: 'super-admin',
    pageHeader: {
        title: 'Quotas',
        description: 'Per-organisation limits and current-period usage. Empty limits inherit the instance defaults.',
    },
})

const UBadge = resolveComponent('UBadge')

const adminStore = useAdminStore()
const toast = useToast()

const quotas = ref<AdminQuotas | null>(null)
const loading = ref(true)

async function loadData() {
    try {
        quotas.value = await adminStore.getQuotas()
    }
    catch (err) {
        console.error('[Admin] Failed to load quotas', err)
    }
    finally {
        loading.value = false
    }
}

onMounted(loadData)

// Edit modal state — string-typed fields so an emptied input means "inherit".
const editOpen = ref(false)
const editTarget = ref<AdminOrgQuotaStatus | null>(null)
const editForm = ref({ max_sources: '', max_assets_per_source: '', max_successful_runs_per_month: '' })
const saving = ref(false)

const LIMIT_FIELDS = [
    { key: 'max_sources', label: 'Max sources' },
    { key: 'max_assets_per_source', label: 'Max assets per source' },
    { key: 'max_successful_runs_per_month', label: 'Max successful runs / month' },
] as const

function defaultPlaceholder(key: keyof typeof editForm.value): string {
    const value = quotas.value?.defaults[key]
    return value != null ? `default: ${value}` : 'default: unlimited'
}

function openEdit(org: AdminOrgQuotaStatus) {
    editTarget.value = org
    editForm.value = {
        max_sources: org.limits.max_sources?.toString() ?? '',
        max_assets_per_source: org.limits.max_assets_per_source?.toString() ?? '',
        max_successful_runs_per_month: org.limits.max_successful_runs_per_month?.toString() ?? '',
    }
    editOpen.value = true
}

async function submitEdit() {
    const target = editTarget.value
    if (!target) return

    saving.value = true
    try {
        await adminStore.updateOrgQuota(target.id, {
            max_sources: editForm.value.max_sources === '' ? null : Number(editForm.value.max_sources),
            max_assets_per_source: editForm.value.max_assets_per_source === ''
                ? null
                : Number(editForm.value.max_assets_per_source),
            max_successful_runs_per_month: editForm.value.max_successful_runs_per_month === ''
                ? null
                : Number(editForm.value.max_successful_runs_per_month),
        })
        toast.add({ title: `Quota limits updated for ${target.name}`, color: 'success' })
        editOpen.value = false
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to update quota limits'))
    }
    finally {
        saving.value = false
    }
}

function rowActions(org: AdminOrgQuotaStatus): DropdownMenuItem[][] {
    return [
        [
            {
                label: 'Edit limits',
                icon: 'i-lucide-pencil',
                onSelect: () => openEdit(org),
            },
        ],
    ]
}

const rows = computed(() => quotas.value?.organisations ?? [])

const periodLabel = computed(() => {
    if (!quotas.value) return ''
    return new Date(quotas.value.period_start).toLocaleDateString(undefined, { month: 'long', year: 'numeric' })
})

/** "used / limit" with unlimited limits shown as a plain count. */
function usageText(used: number, limit: number | null): string {
    return limit != null ? `${used} / ${limit}` : String(used)
}

function usageCell(used: number, limit: number | null) {
    const over = limit != null && used >= limit
    return h('span', { class: over ? 'font-semibold text-error' : undefined }, usageText(used, limit))
}

const columns: TableColumn<AdminOrgQuotaStatus>[] = [
    {
        accessorKey: 'name',
        header: 'Organisation',
        cell: ({ row }) => h('span', { class: 'font-semibold text-highlighted' }, row.original.name),
    },
    {
        id: 'sources',
        header: 'Sources',
        cell: ({ row }) => usageCell(row.original.sources, row.original.effective.max_sources),
    },
    {
        id: 'assets',
        header: 'Assets / source (max)',
        cell: ({ row }) => usageCell(row.original.max_assets_per_source, row.original.effective.max_assets_per_source),
    },
    {
        id: 'runs',
        header: 'Successful runs',
        cell: ({ row }) => {
            const org = row.original
            const parts = [usageCell(org.successful_runs, org.effective.max_successful_runs_per_month)]
            if (org.reserved_runs > 0)
                parts.push(h('span', { class: 'text-dimmed text-xs' }, ` +${org.reserved_runs} reserved`))
            return h('span', parts)
        },
    },
    {
        id: 'ledger',
        header: 'Ledger',
        cell: ({ row }) => {
            const org = row.original
            return org.successful_runs === org.recomputed_successful_runs
                ? h(UBadge, { label: 'In sync', color: 'success' })
                : h(UBadge, {
                        label: `Drift (runs table: ${org.recomputed_successful_runs})`,
                        color: 'warning',
                    })
        },
    },
    {
        id: 'overrides',
        header: 'Overrides',
        cell: ({ row }) => {
            const limits = row.original.limits
            const set = Object.entries(limits).filter(([, value]) => value != null)
            if (!set.length) return h('span', { class: 'text-dimmed' }, '—')
            return h('div', { class: 'flex flex-wrap gap-1' }, set.map(([key, value]) =>
                h('span', { key, class: 'rounded-[5px] bg-elevated px-1.5 py-0.5 font-mono text-xs' }, `${key}=${value}`),
            ))
        },
    },
]
</script>

<template>
    <div class="flex flex-col flex-1 min-h-0 gap-3">
        <div v-if="quotas"
             class="flex items-center gap-2 text-sm text-muted">
            <UIcon name="i-lucide-calendar"
                   class="size-4" />
            <span>Current period: {{ periodLabel }}</span>
        </div>

        <DataTable :columns="columns"
                   :data="rows"
                   :loading="loading"
                   :row-actions="rowActions"
                   no-actions
                   search-placeholder="Search organisations..."
                   @edit="openEdit" />

        <UModal v-model:open="editOpen"
                :title="`Quota limits — ${editTarget?.name}`"
                :ui="{ footer: 'justify-end' }">
            <template #body>
                <div class="flex flex-col gap-3">
                    <p class="text-sm text-muted">
                        Overrides for this organisation. Leave a field empty to inherit the instance default.
                    </p>
                    <UFormField v-for="field in LIMIT_FIELDS"
                                :key="field.key"
                                :label="field.label">
                        <UInput v-model="editForm[field.key]"
                                type="number"
                                min="0"
                                :placeholder="defaultPlaceholder(field.key)"
                                class="w-full"
                                @keydown.enter="submitEdit" />
                    </UFormField>
                </div>
            </template>
            <template #footer>
                <UButton label="Cancel"
                         color="neutral"
                         variant="outline"
                         @click="editOpen = false" />
                <UButton label="Save"
                         :disabled="saving"
                         :loading="saving"
                         @click="submitEdit" />
            </template>
        </UModal>
    </div>
</template>
