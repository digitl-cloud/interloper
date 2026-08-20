<script setup lang="ts">
import type { AdminOrganisation, AdminQuotaLimits, AdminQuotas, AdminUser } from '~/types/admin'

definePageMeta({
    title: 'Admin overview',
    layout: 'admin',
    middleware: 'super-admin',
    pageHeader: {
        title: 'Overview',
        description: 'Platform-wide health and activity at a glance.',
    },
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
            icon: 'i-lucide-building-2',
            label: 'Organisations',
            value: String(orgs.value.length),
            sub: deletedOrgs.value.length
                ? `${deletedOrgs.value.length} soft-deleted, still billable`
                : 'All active',
        },
        {
            icon: 'i-lucide-users',
            label: 'Users',
            value: String(users.value.length),
            sub: `${superAdmins} super admin${superAdmins === 1 ? '' : 's'}`
                + (orphans ? ` · ${orphans} with no org` : ''),
        },
        {
            icon: 'i-lucide-plug',
            label: 'Sources',
            value: sources.toLocaleString(),
            sub: 'Across all organisations',
        },
        {
            icon: 'i-lucide-check-circle',
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
    <div v-if="loading"
         class="flex items-center justify-center py-16">
        <UIcon name="i-lucide-loader-circle"
               class="size-5 animate-spin text-dimmed" />
    </div>

    <div v-else
         class="flex flex-col gap-3">
        <div class="grid grid-cols-2 xl:grid-cols-4 gap-3">
            <div v-for="tile in tiles"
                 :key="tile.label"
                 class="rounded-lg border border-default bg-default px-4 py-3.5">
                <div class="flex items-center gap-1.5 text-[11.5px] font-semibold uppercase tracking-wider text-dimmed">
                    <UIcon :name="tile.icon"
                           class="size-3.5" />
                    {{ tile.label }}
                </div>
                <div class="text-[27px] font-bold tracking-tight tabular-nums mt-2">{{ tile.value }}</div>
                <div class="text-xs text-muted mt-0.5">{{ tile.sub }}</div>
            </div>
        </div>

        <div class="grid lg:grid-cols-2 gap-3 items-start">
            <section class="overflow-hidden rounded-lg border border-default bg-default">
                <div class="flex items-center gap-2 border-b border-default px-4 py-2.5">
                    <UIcon name="i-lucide-alert-triangle"
                           class="size-4 text-warning" />
                    <span class="text-sm font-semibold">Needs attention</span>
                    <UBadge :label="String(attention.length)"
                            color="warning"
                            variant="subtle"
                            size="sm"
                            class="ml-auto" />
                </div>
                <div v-if="attention.length === 0"
                     class="flex items-center gap-2.5 px-4 py-6 text-sm text-muted">
                    <UIcon name="i-lucide-check-circle"
                           class="size-4 text-success" />
                    All clear — nothing needs attention.
                </div>
                <div v-for="item in attention"
                     :key="item.title"
                     class="flex items-start gap-3 px-4 py-3 border-b border-muted last:border-b-0">
                    <span class="flex size-6.5 shrink-0 items-center justify-center rounded-lg mt-0.5"
                          :class="ATTENTION_TILE[item.tone]">
                        <UIcon :name="item.icon"
                               class="size-3.5" />
                    </span>
                    <div class="flex-1 min-w-0">
                        <div class="text-[13.5px] font-semibold">{{ item.title }}</div>
                        <div class="text-xs text-muted leading-normal mt-0.5">{{ item.detail }}</div>
                    </div>
                    <NuxtLink :to="item.to"
                              class="shrink-0 text-xs font-semibold text-primary mt-0.5 whitespace-nowrap">
                        {{ item.action }} →
                    </NuxtLink>
                </div>
            </section>

            <section class="overflow-hidden rounded-lg border border-default bg-default">
                <div class="flex items-center gap-2 border-b border-default px-4 py-2.5">
                    <UIcon name="i-lucide-gauge"
                           class="size-4 text-muted" />
                    <span class="text-sm font-semibold">Quota pressure</span>
                    <NuxtLink to="/admin/organisations"
                              class="ml-auto text-xs font-semibold text-primary">
                        All organisations →
                    </NuxtLink>
                </div>
                <div v-if="pressure.length === 0"
                     class="px-4 py-6 text-sm text-muted">
                    No run limits configured — usage is unmetered pressure-wise.
                </div>
                <div v-else
                     class="px-4 pb-3.5 pt-1">
                    <div v-for="entry in pressure"
                         :key="entry.id"
                         class="py-2.5">
                        <div class="flex items-baseline gap-2">
                            <span class="flex-1 min-w-0 truncate text-[13px] font-semibold">{{ entry.name }}</span>
                            <span class="font-mono text-[11.5px] text-muted">
                                {{ entry.used.toLocaleString() }} / {{ entry.limit.toLocaleString() }}
                            </span>
                        </div>
                        <AdminUsageMeter :used="entry.used"
                                         :limit="entry.limit"
                                         class="mt-1.5" />
                        <div class="text-[11.5px] text-dimmed mt-1">{{ entry.note }}</div>
                    </div>
                </div>
            </section>
        </div>

        <div class="grid lg:grid-cols-2 gap-3 items-start">
            <section class="overflow-hidden rounded-lg border border-default bg-default">
                <div class="flex items-center gap-2 border-b border-default px-4 py-2.5">
                    <UIcon name="i-lucide-trending-up"
                           class="size-4 text-muted" />
                    <span class="text-sm font-semibold">Top organisations by usage</span>
                    <span class="ml-auto text-[11.5px] text-dimmed">{{ periodLabel }}</span>
                </div>
                <div class="flex items-center gap-3 px-4 py-2 border-b border-muted text-xs font-medium text-muted">
                    <span class="flex-1 min-w-0">Organisation</span>
                    <span class="w-16 text-right shrink-0">Runs</span>
                    <span class="w-14 text-right shrink-0">Sources</span>
                    <span class="w-14 text-right shrink-0">Members</span>
                </div>
                <div v-if="topOrgs.length === 0"
                     class="px-4 py-6 text-sm text-muted">
                    No usage recorded this period.
                </div>
                <NuxtLink v-for="org in topOrgs"
                          :key="org.id"
                          :to="`/admin/organisations/${org.id}`"
                          class="flex items-center gap-3 px-4 py-3 border-b border-muted last:border-b-0 text-[13px] hover:bg-elevated/50">
                    <span class="flex-1 min-w-0 flex items-center gap-2.5">
                        <span class="w-[5px] h-[22px] rounded shrink-0"
                              :style="{ background: org.tint }" />
                        <span class="font-semibold truncate">{{ org.name }}</span>
                    </span>
                    <span class="w-16 text-right tabular-nums shrink-0">{{ org.runs.toLocaleString() }}</span>
                    <span class="w-14 text-right tabular-nums text-muted shrink-0">{{ org.sources }}</span>
                    <span class="w-14 text-right tabular-nums text-muted shrink-0">{{ org.members }}</span>
                </NuxtLink>
            </section>

            <section class="overflow-hidden rounded-lg border border-default bg-default">
                <div class="flex items-center gap-2 border-b border-default px-4 py-2.5">
                    <UIcon name="i-lucide-activity"
                           class="size-4 text-muted" />
                    <span class="text-sm font-semibold">Recent activity</span>
                </div>
                <div v-if="activity.length === 0"
                     class="px-4 py-6 text-sm text-muted">
                    Nothing yet.
                </div>
                <div v-for="entry in activity"
                     :key="entry.when + entry.text"
                     class="flex items-start gap-3 px-4 py-2.5 border-b border-muted last:border-b-0">
                    <span class="flex size-6 shrink-0 items-center justify-center rounded-md bg-elevated text-muted mt-0.5">
                        <UIcon :name="entry.icon"
                               class="size-3.5" />
                    </span>
                    <div class="flex-1 min-w-0">
                        <div class="text-[13px] leading-snug">{{ entry.text }}</div>
                        <div class="text-[11.5px] text-dimmed mt-0.5">{{ entry.who }} · {{ entry.whenLabel }}</div>
                    </div>
                </div>
            </section>
        </div>
    </div>
</template>
