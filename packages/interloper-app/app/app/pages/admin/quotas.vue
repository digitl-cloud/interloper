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
        description: 'Per-organisation limits and current-period usage. Empty limits inherit the instance defaults; edit them on the organisation page.',
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

const defaultChips = computed(() => {
    const defaults = quotas.value?.defaults
    if (!defaults) return []
    return Object.entries(defaults).map(([key, value]) => ({
        key,
        value: value != null ? value.toLocaleString() : 'unlimited',
    }))
})

function openOrg(org: AdminOrgQuotaStatus) {
    if (org.deleted_at) return
    navigateTo(`/admin/organisations/${org.id}`)
}

/** Threshold tone: ink below 75%, amber from 75%, red from 90%. */
function usageClass(used: number, limit: number | null): string {
    if (limit == null || limit <= 0) return ''
    const pct = (used / limit) * 100
    if (pct >= 90) return 'text-error font-semibold'
    if (pct >= 75) return 'text-warning font-semibold'
    return ''
}

function usageText(used: number, limit: number | null): string {
    return limit != null ? `${used.toLocaleString()} / ${limit.toLocaleString()}` : used.toLocaleString()
}

const dash = () => h('span', { class: 'text-dimmed' }, '—')

const columns: TableColumn<AdminOrgQuotaStatus>[] = [
    {
        accessorKey: 'name',
        header: 'Organisation',
        cell: ({ row }) => {
            const org = row.original
            const tint = h('span', {
                class: 'w-[5px] h-[22px] rounded shrink-0',
                style: { background: avatarColor(org.id) },
            })
            const name = org.deleted_at
                ? h('span', { class: 'text-dimmed line-through truncate' }, org.name)
                : h('span', { class: 'font-semibold text-highlighted truncate' }, org.name)
            const parts = [tint, name]
            if (org.deleted_at)
                parts.push(h(UBadge, { label: 'Deleted', color: 'neutral', variant: 'subtle', size: 'sm' }))
            return h('div', { class: 'flex items-center gap-2.5 min-w-0' }, parts)
        },
    },
    {
        id: 'sources',
        header: 'Sources',
        cell: ({ row }) => {
            const org = row.original
            if (org.deleted_at) return dash()
            return h('span', {
                class: `font-mono text-xs ${usageClass(org.sources, org.effective.max_sources)}`,
            }, usageText(org.sources, org.effective.max_sources))
        },
    },
    {
        id: 'assets',
        header: 'Assets / source (max)',
        cell: ({ row }) => {
            const org = row.original
            if (org.deleted_at) return dash()
            // Deliberately never threshold-colored: the largest source is
            // informational, not a pressure signal.
            return h('span', { class: 'font-mono text-xs text-muted' },
                usageText(org.max_assets_per_source, org.effective.max_assets_per_source))
        },
    },
    {
        id: 'runs',
        header: 'Successful runs',
        cell: ({ row }) => {
            const org = row.original
            const limit = org.effective.max_successful_runs_per_month
            const parts = [h('span', {
                class: `font-mono text-xs ${usageClass(org.successful_runs, limit)}`,
            }, usageText(org.successful_runs, limit))]
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
                        label: `Drift (runs table: ${org.recomputed_successful_runs.toLocaleString()})`,
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
            if (!set.length) return dash()
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
             class="flex flex-wrap items-center gap-2.5 text-sm text-muted shrink-0">
            <span class="flex items-center gap-1.5">
                <UIcon name="i-lucide-calendar"
                       class="size-4" />
                Current period: <b class="text-default font-semibold">{{ periodLabel }}</b>
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
                   @edit="openOrg" />
    </div>
</template>
