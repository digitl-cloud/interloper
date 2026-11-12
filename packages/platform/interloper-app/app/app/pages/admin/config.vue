<script setup lang="ts">
import type { AdminConfig } from '~/types/admin'

definePageMeta({ layout: 'admin', middleware: 'super-admin' })

const adminStore = useAdminStore()
const appConfig = useAppConfig()

const config = ref<AdminConfig | null>(null)
const loading = ref(true)
const loadError = ref<unknown>(null)

async function load() {
    loading.value = true
    loadError.value = null
    try {
        config.value = await adminStore.getConfig()
    }
    catch (err) {
        loadError.value = err
    }
    finally {
        loading.value = false
    }
}

onMounted(load)

type Pill = { label: string, color: 'success' | 'warning' | 'error' | 'neutral' }

interface ConfigRow {
    label: string
    value?: string
    pill?: Pill
    badges?: string[]
    /** Configured sub-attributes (tuning, hosts, URIs), as key=value badges. */
    attrs?: { key: string, value: string }[]
    /** Class defaults the instance does not configure, listed apart from what it does. */
    defaults?: { key: string, value: string }[]
    /** Catalog import paths, listed under the value. */
    lines?: string[]
}

interface ConfigSection {
    title: string
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
    const { deployment, auth, services, data } = config.value
    const launcher = deployment.launcher

    const deploymentRows: ConfigRow[] = [
        { label: 'Version', value: deployment.version ?? '—' },
        {
            label: 'Launcher',
            value: launcher.type,
            attrs: configAttrs(launcher.config),
            defaults: configAttrs(launcher.defaults),
        },
        {
            label: 'Runner',
            value: deployment.runner.type,
            attrs: configAttrs(deployment.runner.config),
            defaults: configAttrs(deployment.runner.defaults),
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
                { key: 'max_delay', value: `${services.cron.max_execution_delay}s` },
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
                { key: 'poll', value: `${services.reaper.poll_interval}s` },
                { key: 'startup', value: `${services.reaper.startup_timeout}s` },
                { key: 'heartbeat', value: `${services.reaper.heartbeat_interval}s / ${services.reaper.heartbeat_timeout}s` },
                { key: 'run_timeout', value: services.reaper.run_timeout == null ? 'none' : `${services.reaper.run_timeout}s` },
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
        ...(services.app_external_url ? [{ label: 'App URL', value: services.app_external_url }] : []),
    ]

    const CATALOG_PREVIEW = 4
    const catalogRows: ConfigRow[] = Object.entries(data.catalog).map(([kind, keys]) => ({
        label: `${kindLabel(kind)} (${keys.length})`,
        lines: keys.length > CATALOG_PREVIEW
            ? [...keys.slice(0, CATALOG_PREVIEW), `+ ${keys.length - CATALOG_PREVIEW} more`]
            : keys,
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

    return [
        { title: 'Deployment', rows: deploymentRows },
        { title: 'Authentication', rows: authRows },
        { title: 'Services', rows: serviceRows },
        { title: 'Data', rows: dataRows },
    ]
})
</script>

<template>
    <UDashboardPanel id="admin-config">
        <template #header>
            <AppNavbar title="Instance configuration" />
        </template>
        <template #body>
            <div class="mx-auto flex w-full max-w-2xl flex-col gap-4 sm:gap-6">
                <p class="text-sm text-muted">
                    How this deployment is wired, read from the running instance. Secrets are redacted.
                </p>

                <div v-if="loading"
                     class="flex items-center justify-center py-12">
                    <UIcon :name="appConfig.ui.icons.loading"
                           class="size-5 animate-spin text-dimmed" />
                </div>

                <LoadError v-else-if="loadError"
                           title="Couldn't load the instance configuration"
                           :error="loadError"
                           @retry="load" />

                <template v-else>
                    <section v-for="section in sections"
                             :key="section.title"
                             class="flex flex-col gap-4">
                        <UPageCard variant="naked"
                                   orientation="horizontal"
                                   :title="section.title" />
                        <UCard :ui="{ body: 'flex flex-col gap-4' }">
                            <template v-for="(row, index) in section.rows"
                                      :key="row.label">
                                <USeparator v-if="index > 0" />
                                <UFormField :label="row.label"
                                            orientation="horizontal">
                                    <div class="flex max-w-sm flex-col gap-1.5 sm:items-end sm:text-right">
                                        <div v-if="row.pill || row.badges || row.value"
                                             class="flex flex-wrap items-center gap-1.5 sm:justify-end">
                                            <UBadge v-if="row.pill"
                                                    :color="row.pill.color"
                                                    :label="row.pill.label" />
                                            <UBadge v-for="badge in row.badges"
                                                    :key="badge"
                                                    color="neutral"
                                                    :label="badge" />
                                            <span v-if="row.value"
                                                  class="min-w-0 break-all font-medium text-highlighted tabular-nums">{{ row.value }}</span>
                                        </div>
                                        <div v-if="row.attrs?.length"
                                             class="flex flex-wrap gap-1.5 sm:justify-end">
                                            <UBadge v-for="attr in row.attrs"
                                                    :key="attr.key"
                                                    color="neutral"
                                                    size="sm"
                                                    :label="`${attr.key}=${attr.value}`" />
                                        </div>
                                        <p v-if="row.defaults?.length"
                                           class="text-xs text-dimmed">
                                            Defaults: {{ row.defaults.map(attr => `${attr.key}=${attr.value}`).join(', ') }}
                                        </p>
                                        <div v-if="row.lines"
                                             class="flex flex-col gap-1 sm:items-end">
                                            <span v-for="line in row.lines"
                                                  :key="line"
                                                  class="break-all font-mono text-xs">{{ line }}</span>
                                        </div>
                                    </div>
                                </UFormField>
                            </template>
                        </UCard>
                    </section>
                </template>
            </div>
        </template>
    </UDashboardPanel>
</template>
