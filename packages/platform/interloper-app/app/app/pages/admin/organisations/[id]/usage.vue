<script setup lang="ts">
import type { AdminOrgQuotaStatus, AdminQuotas } from '~/types/admin'

const adminStore = useAdminStore()
const appConfig = useAppConfig()
const { orgId, org } = useAdminOrganisation()

const quotas = ref<AdminQuotas | null>(null)
const loading = ref(false)
const loadError = ref<unknown>(null)

const quotaRow = computed<AdminOrgQuotaStatus | null>(() =>
    quotas.value?.organisations.find(row => row.id === orgId.value) ?? null)

async function reloadQuotas() {
    loading.value = true
    loadError.value = null
    try {
        quotas.value = await adminStore.getQuotas()
    }
    catch (err) {
        loadError.value = err
    }
    finally {
        loading.value = false
    }
}

// Quota periods are calendar months in UTC, so their start is a day, not an instant.
const periodStart = computed(() => quotas.value?.period_start.slice(0, 10) ?? null)
const periodStartLabel = computed(() => periodStart.value ? formatCalendarDay(periodStart.value) : '')

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

/** Day-of-period progress (quota counters reset at each UTC month). */
const periodElapsed = computed(() => {
    if (!periodStart.value) return null
    const [year, month] = periodStart.value.split('-').map(Number) as [number, number]
    const days = new Date(Date.UTC(year, month, 0)).getUTCDate()
    const day = Math.min(new Date().getUTCDate(), days)
    return { label: `Day ${day} of ${days}`, pct: Math.round((day / days) * 100) }
})

const ledgerRows = computed(() => quotaRow.value
    ? [
            { label: 'Counter', value: quotaRow.value.successful_runs },
            { label: 'Runs table', value: quotaRow.value.recomputed_successful_runs },
            { label: 'Reserved', value: quotaRow.value.reserved_runs },
        ]
    : [])

const ledgerInSync = computed(() =>
    quotaRow.value != null && quotaRow.value.successful_runs === quotaRow.value.recomputed_successful_runs)

// The AdminQuotaDrawer owns the form; the page only opens it.
const editOpen = ref(false)

watch(orgId, reloadQuotas, { immediate: true })
</script>

<template>
    <div class="mx-auto flex w-full max-w-2xl flex-col gap-4 sm:gap-6">
        <div v-if="loading && !quotas"
             class="flex items-center justify-center py-12">
            <UIcon :name="appConfig.ui.icons.loading"
                   class="size-5 animate-spin text-dimmed" />
        </div>

        <LoadError v-else-if="loadError"
                   title="Couldn't load the usage"
                   :error="loadError"
                   @retry="reloadQuotas" />

        <template v-else-if="quotaRow">
            <div class="grid grid-cols-2 gap-4 sm:gap-6">
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

            <UCard v-if="periodElapsed"
                   :ui="{ body: 'flex flex-col gap-2' }">
                <div class="flex items-baseline gap-2">
                    <span class="flex-1 truncate text-sm font-medium text-highlighted">Period elapsed since {{ periodStartLabel }}</span>
                    <span class="whitespace-nowrap text-xs text-muted tabular-nums">{{ periodElapsed.label }}</span>
                    <span class="whitespace-nowrap text-xs font-semibold text-highlighted tabular-nums">{{ periodElapsed.pct }}%</span>
                </div>
                <UProgress :model-value="periodElapsed.pct" />
            </UCard>

            <section class="flex flex-col gap-4">
                <UPageCard variant="naked"
                           orientation="horizontal"
                           title="Ledger"
                           :description="ledgerInSync
                               ? 'The runs counter and a recount from the runs table agree.'
                               : 'The runs counter and a recount from the runs table disagree, so inspect recent runs.'" />
                <UCard :ui="{ body: 'flex flex-col gap-4' }">
                    <UFormField label="Status"
                                orientation="horizontal">
                        <UBadge :label="ledgerInSync ? 'In sync' : 'Drift'"
                                :color="ledgerInSync ? 'success' : 'warning'"
                                :icon="ledgerInSync ? appConfig.ui.icons.success : appConfig.ui.icons.warning" />
                    </UFormField>
                    <template v-for="row in ledgerRows"
                              :key="row.label">
                        <USeparator />
                        <UFormField :label="row.label"
                                    orientation="horizontal">
                            <span class="font-medium text-highlighted tabular-nums">{{ row.value.toLocaleString() }}</span>
                        </UFormField>
                    </template>
                </UCard>
            </section>

            <section class="flex flex-col gap-4">
                <UPageCard variant="naked"
                           orientation="horizontal"
                           title="Limits"
                           :description="`The current period started ${periodStartLabel}.`">
                    <UButton :icon="appConfig.icons.edit"
                             label="Edit limits"
                             color="neutral"
                             variant="outline"
                             class="w-fit lg:ms-auto"
                             @click="editOpen = true" />
                </UPageCard>
                <UCard :ui="{ body: 'flex flex-col gap-4' }">
                    <template v-for="(row, index) in limitRows"
                              :key="row.key">
                        <USeparator v-if="index > 0" />
                        <UFormField :label="row.label"
                                    :description="row.note"
                                    orientation="horizontal">
                            <div class="flex items-center gap-2">
                                <span class="font-medium text-highlighted tabular-nums">{{ row.value }}</span>
                                <UBadge :label="row.overridden ? 'Override' : 'Inherited'"
                                        :color="row.overridden ? 'info' : 'neutral'"
                                        size="sm" />
                            </div>
                        </UFormField>
                    </template>
                </UCard>
            </section>
        </template>

        <AdminQuotaDrawer v-model:open="editOpen"
                          :org-id="orgId"
                          :org-name="org?.name ?? ''"
                          :limits="quotaRow?.limits ?? null"
                          :fields="quotas?.fields ?? []"
                          @saved="reloadQuotas" />
    </div>
</template>
