<script setup lang="ts">
import { h } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import { UBadge } from '#components'
import type { AdminOrgQuotaStatus, AdminQuotas } from '~/types/admin'

const adminStore = useAdminStore()
const { orgId, org } = useAdminOrganisation()

const quotas = ref<AdminQuotas | null>(null)

const quotaRow = computed<AdminOrgQuotaStatus | null>(() =>
    quotas.value?.organisations.find(row => row.id === orgId.value) ?? null)

async function reloadQuotas() {
    try {
        quotas.value = await adminStore.getQuotas()
    }
    catch (err) {
        console.error('[Admin] Failed to load quotas', err)
    }
}

const periodLabel = computed(() => {
    if (!quotas.value) return ''
    return new Date(quotas.value.period_start).toLocaleDateString(undefined, { month: 'long', year: 'numeric' })
})

const usageTiles = computed(() => {
    const row = quotaRow.value
    if (!row) return []
    const eff = row.effective
    return [
        {
            label: 'Sources',
            value: row.sources.toLocaleString(),
            sub: eff.max_sources != null ? `of ${eff.max_sources.toLocaleString()} allowed` : 'no limit set',
            used: row.sources,
            limit: eff.max_sources ?? null,
        },
        {
            label: 'Assets / source',
            value: row.max_assets_per_source.toLocaleString(),
            sub: eff.max_assets_per_source != null
                ? `largest source, of ${eff.max_assets_per_source.toLocaleString()}`
                : 'largest source',
            used: row.max_assets_per_source,
            limit: eff.max_assets_per_source ?? null,
        },
        {
            label: 'Successful runs',
            value: row.successful_runs.toLocaleString(),
            sub: eff.max_successful_runs_per_month != null
                ? `of ${eff.max_successful_runs_per_month.toLocaleString()} this period`
                : 'this period',
            used: row.successful_runs,
            limit: eff.max_successful_runs_per_month ?? null,
        },
        {
            label: 'Reserved runs',
            value: row.reserved_runs.toLocaleString(),
            sub: 'queued against the ledger',
            used: row.reserved_runs,
            limit: null,
        },
    ]
})

const limitRows = computed(() => {
    const row = quotaRow.value
    if (!row) return []
    return (quotas.value?.fields ?? []).map((field) => {
        const override = row.limits[field.key]
        const effective = row.effective[field.key]
        return {
            key: field.key,
            label: field.label,
            value: effective != null ? effective.toLocaleString() : 'Unlimited',
            overridden: override != null,
            note: override != null
                ? `Instance default is ${field.default != null ? field.default.toLocaleString() : 'unlimited'}`
                : 'Follows the instance default',
        }
    })
})

const limitColumns: TableColumn<(typeof limitRows.value)[number]>[] = [
    {
        accessorKey: 'label',
        header: 'Limit',
        meta: { class: { td: 'whitespace-nowrap font-medium text-highlighted' } },
    },
    {
        accessorKey: 'value',
        header: 'Value',
        meta: { class: { td: 'w-full' } },
        cell: ({ row }) => h('span', [
            h('span', { class: 'font-mono text-sm font-medium text-highlighted' }, row.original.value),
            h('span', { class: 'ml-2 text-xs text-dimmed' }, row.original.note),
        ]),
    },
    {
        accessorKey: 'overridden',
        header: 'Source',
        cell: ({ row }) => h(UBadge, {
            label: row.original.overridden ? 'Override' : 'Inherited',
            color: row.original.overridden ? 'info' : 'neutral',
            size: 'sm',
        }),
    },
]

/** Day-of-period progress for the usage strip (quota counters reset monthly). */
const periodElapsed = computed(() => {
    if (!quotas.value) return null
    const start = new Date(quotas.value.period_start)
    const end = new Date(start.getFullYear(), start.getMonth() + 1, 0)
    const day = Math.min(new Date().getDate(), end.getDate())
    return { label: `Day ${day} of ${end.getDate()}`, pct: Math.round((day / end.getDate()) * 100) }
})

const ledgerInSync = computed(() =>
    quotaRow.value != null && quotaRow.value.successful_runs === quotaRow.value.recomputed_successful_runs)

// Edit drawer — the AdminQuotaDrawer owns the form; we just open it.
const editOpen = ref(false)

watch(orgId, reloadQuotas, { immediate: true })
</script>

<template>
    <div class="flex flex-col gap-4 sm:gap-6">
        <div class="grid grid-cols-2 gap-4 sm:gap-6 xl:grid-cols-4">
            <UCard v-for="tile in usageTiles"
                   :key="tile.label"
                   :ui="{ body: 'flex flex-col gap-3' }">
                <div class="text-sm text-muted">{{ tile.label }}</div>
                <div class="flex min-w-0 items-baseline gap-2">
                    <span class="text-3xl font-semibold tabular-nums text-highlighted">{{ tile.value }}</span>
                    <span class="truncate text-sm text-muted">{{ tile.sub }}</span>
                </div>
                <AdminUsageMeter v-if="tile.limit != null"
                                 :used="tile.used"
                                 :limit="tile.limit"
                                 :show-label="false" />
            </UCard>
        </div>

        <UCard v-if="periodElapsed">
            <div class="flex items-baseline gap-2">
                <span class="flex-1 truncate text-sm font-medium text-highlighted">Period elapsed · {{ periodLabel }}</span>
                <span class="whitespace-nowrap text-xs text-muted">{{ periodElapsed.label }}</span>
                <span class="whitespace-nowrap text-xs font-semibold text-primary">{{ periodElapsed.pct }}%</span>
            </div>
            <div class="mt-2 h-1.5 overflow-hidden rounded-full bg-accented">
                <div class="h-full rounded-full bg-primary"
                     :style="{ width: periodElapsed.pct + '%' }" />
            </div>
        </UCard>

        <section v-if="quotaRow"
                 class="flex flex-col gap-3">
            <CardHeader title="Ledger"
                        :description="ledgerInSync
                            ? 'The runs counter and a recount from the runs table agree.'
                            : 'The runs counter and a recount from the runs table disagree — inspect recent runs.'" />
            <UCard :ui="{ body: 'p-0 sm:p-0' }">
                <div class="divide-y divide-default">
                    <div class="flex items-center gap-3 px-4 py-3 sm:px-6">
                        <span class="w-56 shrink-0 text-sm text-muted">Status</span>
                        <UBadge :label="ledgerInSync ? 'In sync' : 'Drift'"
                                :color="ledgerInSync ? 'success' : 'warning'"
                                :icon="ledgerInSync ? 'i-lucide-check' : 'i-lucide-triangle-alert'" />
                    </div>
                    <div class="flex items-center gap-3 px-4 py-3 sm:px-6">
                        <span class="w-56 shrink-0 text-sm text-muted">Counter</span>
                        <span class="font-mono text-sm font-medium">{{ quotaRow.successful_runs.toLocaleString() }}</span>
                    </div>
                    <div class="flex items-center gap-3 px-4 py-3 sm:px-6">
                        <span class="w-56 shrink-0 text-sm text-muted">Runs table</span>
                        <span class="font-mono text-sm font-medium">{{ quotaRow.recomputed_successful_runs.toLocaleString() }}</span>
                    </div>
                    <div class="flex items-center gap-3 px-4 py-3 sm:px-6">
                        <span class="w-56 shrink-0 text-sm text-muted">Reserved</span>
                        <span class="font-mono text-sm font-medium">{{ quotaRow.reserved_runs.toLocaleString() }}</span>
                    </div>
                </div>
            </UCard>
        </section>

        <section class="flex flex-col gap-3">
            <CardHeader title="Limits"
                        :description="`Current period: ${periodLabel}`">
                <UButton icon="i-lucide-pencil"
                         label="Edit limits"
                         color="neutral"
                         variant="outline"
                         size="sm"
                         @click="editOpen = true" />
            </CardHeader>
            <UCard>
                <UTable :data="limitRows"
                        :columns="limitColumns" />
            </UCard>
        </section>

        <AdminQuotaDrawer v-model:open="editOpen"
                          :org-id="orgId"
                          :org-name="org?.name ?? ''"
                          :limits="quotaRow?.limits ?? null"
                          :fields="quotas?.fields ?? []"
                          @saved="reloadQuotas" />
    </div>
</template>
