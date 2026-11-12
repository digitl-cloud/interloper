<script setup lang="ts">
import { h } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { AdminOrganisation, AdminQuotas, AdminUser } from '~/types/admin'
import type { QuotaPeak } from '~/utils/quota'

definePageMeta({
    layout: 'admin',
    middleware: 'super-admin',
})

const adminStore = useAdminStore()
const appConfig = useAppConfig()

const orgs = ref<AdminOrganisation[]>([])
const users = ref<AdminUser[]>([])
const quotas = ref<AdminQuotas | null>(null)
const loading = ref(true)
const loadError = ref<unknown>(null)

async function load() {
    loading.value = true
    loadError.value = null
    try {
        [orgs.value, users.value, quotas.value] = await Promise.all([
            adminStore.listOrganisations(),
            adminStore.listUsers(),
            adminStore.getQuotas(),
        ])
    }
    catch (err) {
        loadError.value = err
    }
    finally {
        loading.value = false
    }
}

onMounted(load)

const orgById = computed(() => new Map(orgs.value.map(org => [org.id, org])))
const deletedOrgs = computed(() => orgs.value.filter(org => org.deleted_at))

/** Quota rows for live orgs only — soft-deleted orgs have their payload purged. */
const liveQuotaRows = computed(() => {
    const deleted = new Set(deletedOrgs.value.map(org => org.id))
    return quotas.value?.organisations.filter(row => !deleted.has(row.id)) ?? []
})

// Quota periods are calendar months in UTC, so their start is a day, not an instant.
const periodLabel = computed(() => quotas.value ? `Since ${formatCalendarDay(quotas.value.period_start.slice(0, 10))}` : '')

// -- Stat tiles ---------------------------------------------------------------

const tiles = computed(() => {
    const superAdmins = users.value.filter(user => user.is_super_admin).length
    const orphans = users.value.filter(user => user.organisations.length === 0).length
    const sources = liveQuotaRows.value.reduce((sum, row) => sum + row.sources, 0)
    const runs = (quotas.value?.organisations ?? []).reduce((sum, row) => sum + row.successful_runs, 0)
    return [
        {
            label: 'Organisations',
            value: String(orgs.value.length),
            sub: deletedOrgs.value.length
                ? `${deletedOrgs.value.length} soft-deleted, still billable`
                : 'All active',
        },
        {
            label: 'Users',
            value: String(users.value.length),
            sub: `${superAdmins} super admin${superAdmins === 1 ? '' : 's'}`
                + (orphans ? ` · ${orphans} with no org` : ''),
        },
        {
            label: 'Sources',
            value: sources.toLocaleString(),
            sub: 'Across all organisations',
        },
        {
            label: 'Successful runs',
            value: runs.toLocaleString(),
            sub: 'This quota period',
        },
    ]
})

// -- Needs attention ------------------------------------------------------------

interface AttentionItem {
    icon: string
    tone: 'error' | 'warning' | 'muted'
    title: string
    detail: string
    action: string
    to: string
}

const attention = computed<AttentionItem[]>(() => {
    const items: AttentionItem[] = []
    for (const row of liveQuotaRows.value) {
        const peak = peakQuota(row)
        if (peak && peak.pct >= 75) {
            items.push({
                icon: appConfig.icons.quota,
                tone: peak.pct >= 90 ? 'error' : 'warning',
                title: `${row.name} at ${peak.pct}% of ${peak.label.toLowerCase()} quota`,
                detail: `${peak.label} ${peak.used.toLocaleString()} / ${peak.limit.toLocaleString()}`
                    + (row.reserved_runs ? `, plus ${row.reserved_runs} reserved runs.` : '.'),
                action: 'Review',
                to: '/admin/organisations',
            })
        }
        if (row.successful_runs !== row.recomputed_successful_runs) {
            items.push({
                icon: appConfig.ui.icons.warning,
                tone: 'warning',
                title: `Ledger drift on ${row.name}`,
                detail: `Counter reads ${row.successful_runs.toLocaleString()} successful runs; recomputed `
                    + `from the runs table gives ${row.recomputed_successful_runs.toLocaleString()}.`,
                action: 'Inspect',
                to: '/admin/organisations',
            })
        }
    }
    for (const user of users.value) {
        if (user.organisations.length === 0) {
            items.push({
                icon: appConfig.icons.user,
                tone: 'muted',
                title: `${user.name || user.email} belongs to no organisation`,
                detail: `Signed up ${formatDay(user.created_at)} and was never invited anywhere — `
                    + 'cannot reach any workspace.',
                action: 'Review',
                to: '/admin/users',
            })
        }
    }
    for (const org of deletedOrgs.value) {
        items.push({
            icon: appConfig.icons.delete,
            tone: 'muted',
            title: `${org.name} soft-deleted ${relativeTime(new Date(org.deleted_at!))}`,
            detail: 'Read-only and retained for billing history.',
            action: 'Review',
            to: '/admin/organisations',
        })
    }
    return items
})

const ATTENTION_ICON: Record<AttentionItem['tone'], string> = {
    error: 'text-error',
    warning: 'text-warning',
    muted: 'text-muted',
}

// -- Quota pressure + top orgs ----------------------------------------------------

const pressure = computed(() => liveQuotaRows.value
    .map(row => ({ row, peak: peakQuota(row) }))
    .filter((entry): entry is { row: typeof entry.row, peak: QuotaPeak } => entry.peak != null)
    .sort((a, b) => b.peak.used / b.peak.limit - a.peak.used / a.peak.limit)
    .slice(0, 5)
    .map(({ row, peak }) => ({
        id: row.id,
        name: row.name,
        ...peak,
        note: Object.values(row.limits).some(value => value != null)
            ? 'Has per-organisation overrides'
            : 'Inherits instance defaults',
    })))

const topOrgs = computed(() => liveQuotaRows.value
    .slice()
    .sort((a, b) => b.successful_runs - a.successful_runs)
    .slice(0, 5)
    .map(row => ({
        id: row.id,
        name: row.name,
        runs: row.successful_runs,
        sources: row.sources,
        members: orgById.value.get(row.id)?.member_count ?? 0,
    })))

const topOrgColumns: TableColumn<(typeof topOrgs.value)[number]>[] = [
    {
        accessorKey: 'name',
        header: 'Organisation',
        meta: { class: { td: 'w-full' } },
        cell: ({ row }) => h('span', { class: 'truncate font-medium text-highlighted' }, row.original.name),
    },
    {
        accessorKey: 'runs',
        header: 'Runs',
        meta: { class: { th: 'text-right', td: 'text-right tabular-nums text-highlighted' } },
        cell: ({ row }) => row.original.runs.toLocaleString(),
    },
    {
        accessorKey: 'sources',
        header: 'Sources',
        meta: { class: { th: 'text-right', td: 'text-right tabular-nums' } },
    },
    {
        accessorKey: 'members',
        header: 'Members',
        meta: { class: { th: 'text-right', td: 'text-right tabular-nums' } },
    },
]

// -- Recent activity --------------------------------------------------------------

const activity = computed(() => {
    const entries: { when: string, icon: string, text: string, who: string }[] = []
    for (const org of orgs.value) {
        if (org.created_at)
            entries.push({ when: org.created_at, icon: appConfig.icons.organisation, text: 'Organisation created', who: org.name })
        if (org.deleted_at)
            entries.push({ when: org.deleted_at, icon: appConfig.icons.delete, text: 'Organisation deleted', who: org.name })
    }
    for (const user of users.value) {
        if (user.created_at)
            entries.push({
                when: user.created_at,
                icon: appConfig.icons.user,
                text: `${user.name || user.email} joined the platform`,
                who: user.organisations[0]?.name ?? '—',
            })
    }
    return entries
        .sort((a, b) => new Date(b.when).getTime() - new Date(a.when).getTime())
        .slice(0, 8)
        .map(entry => ({ ...entry, whenLabel: relativeTime(new Date(entry.when)) }))
})
</script>

<template>
    <UDashboardPanel id="admin">
        <template #header>
            <AppNavbar title="Overview" />
        </template>
        <template #body>
            <div v-if="loading"
                 class="flex items-center justify-center py-12">
                <UIcon :name="appConfig.ui.icons.loading"
                       class="size-5 animate-spin text-dimmed" />
            </div>

            <LoadError v-else-if="loadError"
                       title="Couldn't load the overview"
                       :error="loadError"
                       @retry="load" />

            <template v-else>
                <div class="grid grid-cols-2 gap-4 sm:gap-6 xl:grid-cols-4">
                    <UCard v-for="tile in tiles"
                           :key="tile.label"
                           :ui="{ body: 'flex flex-col gap-3' }">
                        <div class="text-sm text-muted">{{ tile.label }}</div>
                        <div class="flex min-w-0 items-baseline gap-2">
                            <span class="text-3xl font-semibold tabular-nums text-highlighted">{{ tile.value }}</span>
                            <span class="truncate text-sm text-muted">{{ tile.sub }}</span>
                        </div>
                    </UCard>
                </div>

                <div class="grid items-start gap-4 sm:gap-6 lg:grid-cols-2">
                    <UCard title="Needs attention"
                           :description="`${attention.length} item${attention.length === 1 ? '' : 's'}`">
                        <UEmpty v-if="attention.length === 0"
                                variant="naked"
                                size="sm"
                                :icon="appConfig.ui.icons.success"
                                title="All clear"
                                description="Nothing needs attention." />
                        <div v-else
                             class="flex flex-col divide-y divide-default">
                            <div v-for="item in attention"
                                 :key="item.title"
                                 class="flex items-center gap-3 py-3 first:pt-0 last:pb-0">
                                <UIcon :name="item.icon"
                                       class="size-5 shrink-0"
                                       :class="ATTENTION_ICON[item.tone]" />
                                <div class="min-w-0 flex-1">
                                    <div class="text-sm font-medium text-highlighted">{{ item.title }}</div>
                                    <div class="mt-0.5 text-xs text-muted">{{ item.detail }}</div>
                                </div>
                                <UButton :label="item.action"
                                         :to="item.to"
                                         color="neutral"
                                         variant="outline"
                                         size="sm"
                                         class="shrink-0" />
                            </div>
                        </div>
                    </UCard>

                    <UCard>
                        <template #header>
                            <CardHeader title="Quota pressure">
                                <UButton label="All organisations"
                                         to="/admin/organisations"
                                         color="neutral"
                                         variant="outline"
                                         size="sm" />
                            </CardHeader>
                        </template>
                        <UEmpty v-if="pressure.length === 0"
                                variant="naked"
                                size="sm"
                                title="No quota limits configured"
                                description="Usage is unmetered, so nothing is under pressure." />
                        <div v-else
                             class="flex flex-col gap-4">
                            <div v-for="entry in pressure"
                                 :key="entry.id">
                                <div class="flex items-baseline gap-2">
                                    <span class="min-w-0 flex-1 truncate text-sm font-medium text-highlighted">{{ entry.name }}</span>
                                    <span class="text-xs text-muted tabular-nums">
                                        {{ entry.label }} {{ entry.used.toLocaleString() }} / {{ entry.limit.toLocaleString() }}
                                    </span>
                                </div>
                                <AdminUsageMeter :used="entry.used"
                                                 :limit="entry.limit"
                                                 class="mt-1.5" />
                                <div class="mt-1 text-xs text-dimmed">{{ entry.note }}</div>
                            </div>
                        </div>
                    </UCard>
                </div>

                <div class="grid items-start gap-4 sm:gap-6 lg:grid-cols-2">
                    <UCard title="Top organisations by usage"
                           :description="periodLabel">
                        <UTable :data="topOrgs"
                                :columns="topOrgColumns"
                                empty="No usage recorded this period."
                                :ui="{ tr: 'cursor-pointer' }"
                                @select="(_e: Event, row: any) => navigateTo(`/admin/organisations/${row.original.id}`)" />
                    </UCard>

                    <UCard title="Recent activity">
                        <UEmpty v-if="activity.length === 0"
                                variant="naked"
                                size="sm"
                                title="Nothing yet" />
                        <div v-else
                             class="flex flex-col divide-y divide-default">
                            <div v-for="entry in activity"
                                 :key="entry.when + entry.text"
                                 class="flex items-start gap-3 py-3 first:pt-0 last:pb-0">
                                <UIcon :name="entry.icon"
                                       class="mt-0.5 size-4 shrink-0 text-muted" />
                                <div class="min-w-0 flex-1">
                                    <div class="text-sm">{{ entry.text }}</div>
                                    <div class="mt-0.5 text-xs text-dimmed">
                                        {{ entry.who }} · <span class="tabular-nums">{{ entry.whenLabel }}</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </UCard>
                </div>
            </template>
        </template>
    </UDashboardPanel>
</template>
