<script setup lang="ts">
import type { AdminConfig, AdminSettingsField } from '~/types/admin'

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
    /** The field's description, shown on hover over its label. */
    description?: string
    value?: string
    pill?: Pill
    /** The public keys of a mapping field, as key=value badges. */
    attrs?: { key: string, value: string }[]
    /** A list value, one entry per line. */
    items?: string[]
    /** Catalog import paths, listed under the value. */
    lines?: string[]
}

interface ConfigSection {
    title: string
    description?: string
    /** The extension declaring a settings section. */
    extension?: string
    rows: ConfigRow[]
}

/** Flatten a (possibly nested) mapping into dotted key=value attribute pairs. */
function configAttrs(config: Record<string, unknown>, prefix = ''): { key: string, value: string }[] {
    return Object.entries(config).flatMap(([key, value]) => {
        const path = prefix ? `${prefix}.${key}` : key
        if (value && typeof value === 'object' && !Array.isArray(value))
            return configAttrs(value as Record<string, unknown>, path)
        return [{ key: path, value: Array.isArray(value) ? value.join(',') : String(value) }]
    })
}

/** How a public value reads: booleans as pills, lists one entry per line, mappings as badges, the rest as text. */
function valueRow(value: unknown): Partial<ConfigRow> {
    if (typeof value === 'boolean')
        return { pill: value ? { label: 'On', color: 'success' } : { label: 'Off', color: 'neutral' } }
    if (Array.isArray(value))
        return value.length ? { items: value.map(String) } : { value: 'none' }
    if (value && typeof value === 'object')
        return { attrs: configAttrs(value as Record<string, unknown>) }
    if (value === null || value === undefined || value === '')
        return { value: 'unset' }
    return { value: String(value) }
}

/** One row per field; a mapping's public keys are badges on its own row rather than rows of their own. */
function fieldRows(fields: AdminSettingsField[]): ConfigRow[] {
    return fields
        .filter(field => !field.name.includes('.'))
        .map((field) => {
            const keys = fields
                .filter(child => child.name.startsWith(`${field.name}.`))
                .map(child => ({ key: child.name.slice(field.name.length + 1), value: String(child.value) }))
            const row: ConfigRow = { label: field.name, description: field.description ?? undefined }
            if (field.public)
                return { ...row, ...valueRow(field.value) }
            return {
                ...row,
                pill: field.configured
                    ? { label: 'Configured', color: 'success' }
                    : { label: 'Not configured', color: 'neutral' },
                ...(keys.length ? { attrs: keys } : {}),
            }
        })
}

const sections = computed<ConfigSection[]>(() => {
    if (!config.value) return []
    const { version, features, catalog } = config.value

    const deploymentRows: ConfigRow[] = [
        { label: 'Version', value: version ?? 'unknown' },
        ...Object.entries(features).map(([feature, available]) => ({
            label: feature,
            pill: available
                ? { label: 'Available', color: 'success' as const }
                : { label: 'Unavailable', color: 'neutral' as const },
        })),
    ]

    const CATALOG_PREVIEW = 4
    const catalogRows: ConfigRow[] = Object.entries(catalog).map(([kind, keys]) => ({
        label: `${kindLabel(kind)} (${keys.length})`,
        lines: keys.length > CATALOG_PREVIEW
            ? [...keys.slice(0, CATALOG_PREVIEW), `+ ${keys.length - CATALOG_PREVIEW} more`]
            : keys,
    }))

    return [
        { title: 'Deployment', rows: deploymentRows },
        { title: 'Catalog', rows: catalogRows.length ? catalogRows : [{ label: 'Catalog', value: 'empty' }] },
        ...config.value.sections.map(section => ({
            title: section.name,
            description: section.description ?? undefined,
            extension: section.extension,
            rows: fieldRows(section.fields),
        })),
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
                    How this deployment is wired, read from the running instance: one card per settings section the
                    installed packages declare. A secret shows only whether it is configured.
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
                                   :title="section.title"
                                   :description="section.description">
                            <div v-if="section.extension"
                                 class="flex sm:justify-end">
                                <UBadge color="neutral"
                                        size="sm"
                                        :label="section.extension" />
                            </div>
                        </UPageCard>
                        <UCard :ui="{ body: 'flex flex-col gap-4' }">
                            <template v-for="(row, index) in section.rows"
                                      :key="row.label">
                                <USeparator v-if="index > 0" />
                                <UFormField orientation="horizontal">
                                    <template #label>
                                        <UTooltip :text="row.description"
                                                  :disabled="!row.description">
                                            <span>{{ row.label }}</span>
                                        </UTooltip>
                                    </template>
                                    <div class="flex max-w-sm flex-col gap-1.5 sm:items-end sm:text-right">
                                        <div v-if="row.pill || row.value"
                                             class="flex flex-wrap items-center gap-1.5 sm:justify-end">
                                            <UBadge v-if="row.pill"
                                                    :color="row.pill.color"
                                                    :label="row.pill.label" />
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
                                        <div v-if="row.items"
                                             class="flex flex-col gap-1 sm:items-end">
                                            <span v-for="item in row.items"
                                                  :key="item"
                                                  class="break-all font-medium text-highlighted">{{ item }}</span>
                                        </div>
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
