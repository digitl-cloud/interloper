<script setup lang="ts">
import type { AdminConfig } from '~/types/admin'

definePageMeta({
    title: 'Config',
    layout: 'admin',
    middleware: 'super-admin',
    pageHeader: {
        title: 'Config',
        description: 'Read-only snapshot of this instance\'s runtime configuration. Secrets are redacted; dimmed values are class defaults.',
    },
})

const adminStore = useAdminStore()

const config = ref<AdminConfig | null>(null)
const loading = ref(true)

onMounted(async () => {
    try {
        config.value = await adminStore.getConfig()
    }
    catch (err) {
        console.error('[Admin] Failed to load instance config', err)
    }
    finally {
        loading.value = false
    }
})

type Pill = { label: string, color: 'success' | 'warning' | 'error' | 'neutral' }

interface ConfigRow {
    label: string
    value?: string
    mono?: boolean
    pill?: Pill
    badges?: string[]
    /** Sub-attributes rendered as key=value chips (tuning, hosts, URIs). Dim = class default, not configured. */
    attrs?: { key: string, value: string, dim?: boolean }[]
    /** Mono list under the value (catalog import paths). */
    lines?: string[]
}

interface ConfigSection {
    title: string
    icon: string
    rows: ConfigRow[]
}

function enabledPill(enabled: boolean): Pill {
    return enabled
        ? { label: 'Enabled', color: 'success' }
        : { label: 'Disabled', color: 'neutral' }
}

/** Flatten a (possibly nested) config dict into dotted key=value attribute pairs. */
function configAttrs(config: Record<string, unknown>, prefix = ''): { key: string, value: string }[] {
    return Object.entries(config).flatMap(([key, value]) => {
        const path = prefix ? `${prefix}.${key}` : key
        if (value && typeof value === 'object' && !Array.isArray(value))
            return configAttrs(value as Record<string, unknown>, path)
        return [{ key: path, value: Array.isArray(value) ? value.join(',') : String(value) }]
    })
}

const sections = computed<ConfigSection[]>(() => {
    if (!config.value) return []
    const { deployment, auth, services, data, quotas } = config.value
    const launcher = deployment.launcher

    const deploymentRows: ConfigRow[] = [
        { label: 'Version', value: deployment.version ?? '—', mono: true },
        {
            label: 'Launcher',
            value: launcher.type,
            mono: true,
            attrs: [
                ...configAttrs(launcher.config),
                ...configAttrs(launcher.defaults).map(attr => ({ ...attr, dim: true })),
            ],
        },
        {
            label: 'Runner',
            value: deployment.runner.type,
            mono: true,
            attrs: [
                ...configAttrs(deployment.runner.config),
                ...configAttrs(deployment.runner.defaults).map(attr => ({ ...attr, dim: true })),
            ],
        },
        {
            label: 'Agent',
            pill: enabledPill(deployment.features.agent ?? false),
            ...(deployment.features.agent && deployment.agent_model
                ? { attrs: [{ key: 'model', value: deployment.agent_model }] }
                : {}),
        },
    ]

    const authRows: ConfigRow[] = [
        {
            label: 'Allowed domains',
            ...(auth.allowed_domains.length
                ? { badges: auth.allowed_domains }
                : { pill: { label: '*', color: 'warning' } }),
        },
        auth.super_admin_emails.length
            ? { label: 'Super admins', badges: auth.super_admin_emails }
            : { label: 'Super admins', value: '—' },
        {
            label: 'Google OAuth',
            pill: auth.google_oauth_configured
                ? { label: 'Configured', color: 'success' }
                : { label: 'Not configured', color: 'error' },
            ...(auth.google_redirect_uri
                ? { attrs: [{ key: 'redirect_uri', value: auth.google_redirect_uri }] }
                : {}),
        },
        { label: 'Session expiry', value: `${auth.session_expiry_days} days` },
        { label: 'Secure cookies', pill: enabledPill(auth.cookie_secure) },
    ]

    const serviceRows: ConfigRow[] = [
        {
            label: 'Cron',
            pill: enabledPill(services.cron.enabled),
            attrs: [
                { key: 'reconcile', value: `${services.cron.reconcile_interval}s` },
                { key: 'batch', value: String(services.cron.batch_size) },
                ...(services.cron.max_execution_delay != null
                    ? [{ key: 'max_delay', value: `${services.cron.max_execution_delay}s` }]
                    : []),
            ],
        },
        {
            label: 'Worker',
            pill: enabledPill(services.worker.enabled),
            attrs: [{ key: 'poll', value: `${services.worker.poll_interval}s` }],
        },
        {
            label: 'Reaper',
            pill: enabledPill(services.reaper.enabled),
            attrs: [
                { key: 'timeout', value: `${services.reaper.timeout}s` },
                { key: 'poll', value: `${services.reaper.poll_interval}s` },
            ],
        },
        {
            label: 'SMTP',
            pill: services.smtp.enabled
                ? { label: 'Configured', color: 'success' }
                : { label: 'Not configured', color: 'neutral' },
            ...(services.smtp.enabled
                ? {
                        attrs: [
                            { key: 'host', value: services.smtp.host },
                            { key: 'from', value: services.smtp.from_addr },
                        ],
                    }
                : {}),
        },
        ...(services.mcp_external_url ? [{ label: 'MCP URL', value: services.mcp_external_url, mono: true }] : []),
    ]

    const catalogRows: ConfigRow[] = Object.entries(data.catalog).map(([kind, keys]) => ({
        label: `${kindLabel(kind)} (${keys.length})`,
        lines: keys,
    }))

    const dataRows: ConfigRow[] = [
        {
            label: 'Encryption at rest',
            pill: data.encryption_configured
                ? { label: 'Configured', color: 'success' }
                : { label: 'Missing', color: 'error' },
        },
        ...(catalogRows.length ? catalogRows : [{ label: 'Catalog', value: 'empty' }]),
    ]

    const limitValue = (limit: number | null) => (limit != null ? String(limit) : 'unlimited')
    const quotaRows: ConfigRow[] = [
        { label: 'Max sources', value: limitValue(quotas.max_sources) },
        { label: 'Max assets per source', value: limitValue(quotas.max_assets_per_source) },
        { label: 'Max successful runs / month', value: limitValue(quotas.max_successful_runs_per_month) },
    ]

    return [
        { title: 'Deployment', icon: 'i-lucide-server', rows: deploymentRows },
        { title: 'Authentication', icon: 'i-lucide-key-round', rows: authRows },
        { title: 'Services', icon: 'i-lucide-cog', rows: serviceRows },
        { title: 'Data', icon: 'i-lucide-database', rows: dataRows },
        { title: 'Quota defaults', icon: 'i-lucide-gauge', rows: quotaRows },
    ]
})
</script>

<template>
    <div class="flex flex-col gap-3">
        <div v-if="loading"
             class="flex items-center justify-center py-16">
            <UIcon name="i-lucide-loader-circle"
                   class="size-5 animate-spin text-dimmed" />
        </div>

        <UAlert v-else-if="!config"
                color="error"
                icon="i-lucide-alert-circle"
                title="Instance configuration unavailable" />

        <div v-else
             class="flex flex-col gap-4">
            <section v-for="section in sections"
                     :key="section.title"
                     class="overflow-hidden rounded-lg border border-default bg-default">
                <div class="flex items-center gap-2 border-b border-default bg-muted px-4 py-2.5">
                    <UIcon :name="section.icon"
                           class="size-4 text-muted" />
                    <span class="text-sm font-semibold text-highlighted">{{ section.title }}</span>
                </div>

                <div class="divide-y divide-default">
                    <div v-for="row in section.rows"
                         :key="row.label"
                         class="flex items-start gap-4 px-4 py-2.5 text-sm">
                        <span class="w-56 shrink-0 pt-px text-muted">{{ row.label }}</span>
                        <div class="min-w-0 flex-1">
                            <div v-if="row.pill || row.badges || row.value || row.attrs"
                                 class="flex flex-wrap items-center gap-1.5">
                                <UBadge v-if="row.pill"
                                        :color="row.pill.color"
                                        variant="subtle">
                                    {{ row.pill.label }}
                                </UBadge>
                                <UBadge v-for="badge in row.badges"
                                        :key="badge"
                                        color="neutral"
                                        variant="outline">
                                    {{ badge }}
                                </UBadge>
                                <span v-if="row.value"
                                      class="min-w-0 break-all font-medium"
                                      :class="row.mono && 'font-mono text-[13px]'">{{ row.value }}</span>
                                <span v-for="attr in row.attrs"
                                      :key="attr.key"
                                      class="min-w-0 break-all rounded-[5px] px-1.5 py-0.5 font-mono text-xs"
                                      :class="attr.dim ? 'bg-elevated/50 text-dimmed' : 'bg-elevated'"
                                      :title="attr.dim ? 'Class default (not configured)' : undefined">
                                    <span class="text-muted">{{ attr.key }}=</span>{{ attr.value }}
                                </span>
                            </div>
                            <div v-if="row.lines"
                                 class="grid grid-cols-1 gap-x-6 gap-y-0.5 lg:grid-cols-2">
                                <span v-for="line in row.lines"
                                      :key="line"
                                      class="break-all font-mono text-xs">{{ line }}</span>
                            </div>
                        </div>
                    </div>
                </div>
            </section>
        </div>
    </div>
</template>
