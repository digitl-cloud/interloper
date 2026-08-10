<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { AdminOrgQuotaStatus, AdminQuotas } from '~/types/admin'

definePageMeta({
    title: 'Quotas',
    layout: 'admin',
    middleware: 'super-admin',
    pageHeader: {
        title: 'Quotas',
        description: 'Per-organisation limits and current-period usage. Limits are read-only for now; defaults come from the instance configuration.',
    },
})

const UBadge = resolveComponent('UBadge')

const adminStore = useAdminStore()

const quotas = ref<AdminQuotas | null>(null)
const loading = ref(true)

onMounted(async () => {
    try {
        quotas.value = await adminStore.getQuotas()
    }
    catch (err) {
        console.error('[Admin] Failed to load quotas', err)
    }
    finally {
        loading.value = false
    }
})

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
                ? h(UBadge, { label: 'In sync', color: 'success', variant: 'subtle' })
                : h(UBadge, {
                        label: `Drift (runs table: ${org.recomputed_successful_runs})`,
                        color: 'warning',
                        variant: 'subtle',
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
                   no-actions
                   search-placeholder="Search organisations..." />
    </div>
</template>
