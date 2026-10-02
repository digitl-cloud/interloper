<script setup lang="ts">
import { h } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { AdminOrganisation, AdminQuotaLimits, AdminQuotas, AdminUser } from '~/types/admin'

definePageMeta({
    layout: 'admin',
    middleware: 'super-admin',
})

const adminStore = useAdminStore()

const orgs = ref<AdminOrganisation[]>([])
const users = ref<AdminUser[]>([])
const quotas = ref<AdminQuotas | null>(null)
const loading = ref(true)

onMounted(async () => {
    try {
        [orgs.value, users.value, quotas.value] = await Promise.all([
            adminStore.listOrganisations(),
            adminStore.listUsers(),
            adminStore.getQuotas(),
        ])
    }
    catch (err) {
        console.error('[Admin] Failed to load overview', err)
    }
    finally {
        loading.value = false
    }
})

const orgById = computed(() => new Map(orgs.value.map(org => [org.id, org])))
const deletedOrgs = computed(() => orgs.value.filter(org => org.deleted_at))

/** Quota rows for live orgs only — soft-deleted orgs have their payload purged. */
const liveQuotaRows = computed(() => {
    const deleted = new Set(deletedOrgs.value.map(org => org.id))
    return quotas.value?.organisations.filter(row => !deleted.has(row.id)) ?? []
})

const periodLabel = computed(() => {
    if (!quotas.value) return ''
    return new Date(quotas.value.period_start).toLocaleDateString(undefined, { month: 'long', year: 'numeric' })
})

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
    tone: 'error' | 'warning' | 'neutral'
    title: string
    detail: string
    action: string
    to: string
}

function runPct(row: { successful_runs: number, effective: AdminQuotaLimits }) {
    const limit = row.effective.max_successful_runs_per_month
    if (limit == null || limit <= 0) return null
    return Math.round((row.successful_runs / limit) * 100)
}

const attention = computed<AttentionItem[]>(() => {
    const items: AttentionItem[] = []
    for (const row of liveQuotaRows.value) {
        const pct = runPct(row)
        const limit = row.effective.max_successful_runs_per_month
        if (pct != null && limit != null && pct >= 75) {
            items.push({
                icon: 'i-lucide-gauge',
                tone: pct >= 90 ? 'error' : 'warning',
                title: `${row.name} at ${pct}% of run quota`,
                detail: `${row.successful_runs.toLocaleString()} of ${limit.toLocaleString()} successful runs`
                    + (row.reserved_runs ? `, plus ${row.reserved_runs} reserved.` : '.'),
                action: 'Review',
                to: '/admin/organisations',
            })
        }
        if (row.successful_runs !== row.recomputed_successful_runs) {
            items.push({
                icon: 'i-lucide-triangle-alert',
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
                icon: 'i-lucide-user-minus',
                tone: 'neutral',
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
            icon: 'i-lucide-trash-2',
            tone: 'neutral',
            title: `${org.name} soft-deleted ${timeSince(new Date(org.deleted_at!))} ago`,
            detail: 'Read-only and retained for billing history.',
            action: 'Review',
            to: '/admin/organisations',
        })
    }
    return items
})

const ATTENTION_TILE: Record<AttentionItem['tone'], string> = {
    error: 'bg-error/10 text-error',
    warning: 'bg-warning/10 text-warning',
    neutral: 'bg-elevated text-muted',
}

// -- Quota pressure + top orgs ----------------------------------------------------

const pressure = computed(() => liveQuotaRows.value
    .map(row => ({ row, pct: runPct(row) }))
    .filter((entry): entry is { row: typeof entry.row, pct: number } => entry.pct != null)
    .sort((a, b) => b.pct - a.pct)
    .slice(0, 5)
    .map(({ row, pct }) => ({
        id: row.id,
        name: row.name,
        used: row.successful_runs,
        limit: row.effective.max_successful_runs_per_month!,
        pct,
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
        tint: avatarColor(row.id),
        runs: row.successful_runs,
        sources: row.sources,
        members: orgById.value.get(row.id)?.member_count ?? 0,
    })))

function pctTone(pct: number): string {
    if (pct >= 90) return 'text-error'
    if (pct >= 75) return 'text-warning'
    return 'text-success'
}

const topOrgColumns: TableColumn<(typeof topOrgs.value)[number]>[] = [
    {
        accessorKey: 'name',
        header: 'Organisation',
        meta: { class: { td: 'w-full' } },
        cell: ({ row }) => h('span', { class: 'flex items-center gap-2.5' }, [
            h('span', { class: 'h-5 w-1 shrink-0 rounded', style: { background: row.original.tint } }),
            h('span', { class: 'truncate font-medium text-highlighted' }, row.original.name),
        ]),
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
            entries.push({ when: org.created_at, icon: 'i-lucide-building-2', text: 'Organisation created', who: org.name })
        if (org.deleted_at)
            entries.push({ when: org.deleted_at, icon: 'i-lucide-trash-2', text: 'Organisation deleted', who: org.name })
    }
    for (const user of users.value) {
        if (user.created_at)
            entries.push({
                when: user.created_at,
                icon: 'i-lucide-user-plus',
                text: `${user.name || user.email} joined the platform`,
                who: user.organisations[0]?.name ?? '—',
            })
    }
    return entries
        .sort((a, b) => new Date(b.when).getTime() - new Date(a.when).getTime())
        .slice(0, 8)
        .map(entry => ({ ...entry, whenLabel: `${timeSince(new Date(entry.when))} ago` }))
})
</script>

<template>
    <UDashboardPanel id="admin">
        <template #header>
            <AppNavbar title="Overview" />
        </template>
        <template #body>
            <div v-if="loading"
                 class="flex items-center justify-center py-16">
                <UIcon name="i-lucide-loader-circle"
                       class="size-5 animate-spin text-dimmed" />
            </div>

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
                           :description="`${attention.length} item${attention.length === 1 ? '' : 's'}`"
                           :ui="{ body: 'p-0 sm:p-0' }">
                        <div class="divide-y divide-default border-t border-default">
                            <div v-if="attention.length === 0"
                                 class="flex items-center gap-2.5 px-4 py-5 text-sm text-muted sm:px-6">
                                <UIcon name="i-lucide-check-circle"
                                       class="size-4 text-success" />
                                All clear — nothing needs attention.
                            </div>
                            <div v-for="item in attention"
                                 :key="item.title"
                                 class="flex items-center gap-3.5 px-4 py-3 sm:px-6">
                                <span class="flex size-8 shrink-0 items-center justify-center rounded-full"
                                      :class="ATTENTION_TILE[item.tone]">
                                    <UIcon :name="item.icon"
                                           class="size-4" />
                                </span>
                                <div class="min-w-0 flex-1">
                                    <div class="text-sm font-medium leading-snug text-highlighted">{{ item.title }}</div>
                                    <div class="mt-0.5 text-xs leading-normal text-muted">{{ item.detail }}</div>
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
                        <div v-if="pressure.length === 0"
                             class="text-sm text-muted">
                            No run limits configured — usage is unmetered pressure-wise.
                        </div>
                        <div v-else
                             class="flex flex-col gap-4">
                            <div v-for="entry in pressure"
                                 :key="entry.id">
                                <div class="flex items-baseline gap-2">
                                    <span class="min-w-0 flex-1 truncate text-sm font-semibold">{{ entry.name }}</span>
                                    <span class="font-mono text-xs text-muted">
                                        {{ entry.used.toLocaleString() }} / {{ entry.limit.toLocaleString() }}
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

                    <UCard title="Recent activity"
                           :ui="{ body: 'p-0 sm:p-0' }">
                        <div class="divide-y divide-default border-t border-default">
                            <div v-if="activity.length === 0"
                                 class="px-4 py-5 text-sm text-muted sm:px-6">
                                Nothing yet.
                            </div>
                            <div v-for="entry in activity"
                                 :key="entry.when + entry.text"
                                 class="flex items-start gap-3 px-4 py-3 sm:px-6">
                                <span class="mt-0.5 flex size-6 shrink-0 items-center justify-center rounded-md bg-elevated text-muted">
                                    <UIcon :name="entry.icon"
                                           class="size-3.5" />
                                </span>
                                <div class="min-w-0 flex-1">
                                    <div class="text-sm leading-snug">{{ entry.text }}</div>
                                    <div class="mt-0.5 text-xs text-dimmed">{{ entry.who }} · {{ entry.whenLabel }}</div>
                                </div>
                            </div>
                        </div>
                    </UCard>
                </div>
            </template>
        </template>
    </UDashboardPanel>
</template>
