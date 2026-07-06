<script setup lang="ts">
import type { JsonSchemaProperty } from '~/types/catalog'
import { parseQualifiedKey, requiredDependencies } from '~/types/catalog'
import type { ComponentRecord } from '~/types/component'
import { relationIds } from '~/types/component'
import type { Run } from '~/types/run'

const props = defineProps<{
    asset: ComponentRecord
    assetDefn: AssetDefinition | undefined
    source: ComponentRecord
}>()

const emit = defineEmits<{
    close: []
}>()

const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()
const toast = useToast()
const { apiFetch } = useApi()
const { statusBadge } = useDrift()
const sourceDefn = computed(() => catalogStore.getSourceDefinition(props.source.key))

const driftBadge = computed(() => statusBadge(props.asset.status))

const icon = computed(() => props.assetDefn ? componentIcon(props.assetDefn.key) : 'i-lucide-box')
const sourceIcon = computed(() => componentIcon(props.source.key))

onMounted(() => {
    if (componentsStore.byKind('destination').length === 0 && !componentsStore.loading) {
        componentsStore.fetchAll(['destination'])
    }
})

// ── Partition row counts ────────────────────────────────────────

interface PartitionData {
    partition: string
    rowCount: number
}

const partitionData = ref<PartitionData[]>([])
const partitionLoading = ref(false)
const partitionError = ref<string | null>(null)
const partitionNotMaterialized = ref(false)

const isPartitioned = computed(() => !!props.assetDefn?.partitioning)

async function fetchPartitionCounts() {
    if (!isPartitioned.value) return
    partitionLoading.value = true
    partitionError.value = null
    partitionNotMaterialized.value = false
    try {
        const response = await componentsStore.fetchPartitionRowCounts(props.asset.id)
        partitionData.value = response.counts.map(c => ({
            partition: c.partition,
            rowCount: c.row_count,
        }))
    }
    catch (e: any) {
        if (e?.status === 404 || e?.statusCode === 404) {
            partitionNotMaterialized.value = true
        }
        else {
            partitionError.value = e?.data?.detail ?? e?.message ?? 'Failed to load partition data'
        }
        partitionData.value = []
    }
    finally {
        partitionLoading.value = false
    }
}

watch(() => props.asset.id, () => fetchPartitionCounts(), { immediate: true })

const destinations = computed(() => {
    return relationIds(props.source, 'destination')
        .map(id => componentsStore.byId(id))
        .filter((d): d is ComponentRecord => !!d)
        .map((dest) => {
            const defn = catalogStore.catalog[dest.key]
            return {
                id: dest.id,
                key: dest.key,
                label: dest.name ?? defn?.name ?? dest.key,
                icon: componentIcon(dest.key, 'i-lucide-hard-drive'),
            }
        })
})

/** Parse JSON Schema properties into a flat list of fields for display. */
const schemaFields = computed(() => {
    const schema = props.assetDefn?.asset_schema
    if (!schema?.properties) return []

    const required = new Set(schema.required ?? [])

    return Object.entries(schema.properties).map(([name, prop]) => ({
        name,
        type: resolveSchemaType(prop),
        description: prop.description ?? prop.title ?? '',
        required: required.has(name),
    }))
})

/** Resolve a human-readable type string from a JSON Schema property. */
function resolveSchemaType(prop: JsonSchemaProperty): string {
    // Handle anyOf (e.g. nullable types: anyOf: [{type: "string"}, {type: "null"}])
    if (prop.anyOf) {
        const types = prop.anyOf
            .map(v => formatType(v.type as string | undefined, v.format))
            .filter(t => t !== 'null')
        const base = types.join(' | ') || 'any'
        const nullable = prop.anyOf.some(v => v.type === 'null')
        return nullable ? `${base}?` : base
    }

    // Handle array type (e.g. type: ["string", "null"])
    if (Array.isArray(prop.type)) {
        const types = prop.type.filter(t => t !== 'null').map(t => formatType(t))
        const base = types.join(' | ') || 'any'
        const nullable = prop.type.includes('null')
        return nullable ? `${base}?` : base
    }

    return formatType(prop.type, prop.format)
}

function formatType(type?: string, format?: string): string {
    if (!type) return 'any'
    if (format === 'date-time') return 'datetime'
    if (format === 'date') return 'date'
    if (format === 'uri') return 'uri'
    return type
}

// ── Config ──────────────────────────────────────────────────────

const configSchema = computed(() => props.assetDefn?.config_schema)
const hasConfig = computed(() => {
    const properties = configSchema.value?.properties as Record<string, unknown> | undefined
    return Object.keys(properties ?? {}).length > 0
})

const configData = ref<Record<string, any>>({})
const configValid = ref(true)
const configSaving = ref(false)

watch(() => props.asset, (asset) => {
    configData.value = { ...(asset.config ?? {}) }
}, { immediate: true })

/**
 * Save the whole config object through the components store — the single
 * write path for asset config (any quick-toggle must go through this too).
 */
async function saveConfig() {
    configSaving.value = true
    try {
        await componentsStore.update(props.asset.id, { config: { ...configData.value } })
        // The asset lives in its source's `children` — refresh the parent so
        // the graph/catalog views reflect the new config.
        if (props.asset.parent_id) await componentsStore.fetchOne(props.asset.parent_id)
        toast.add({ title: 'Asset config saved', color: 'success' })
    }
    catch {
        toast.add({ title: 'Failed to save asset config', color: 'error' })
    }
    finally {
        configSaving.value = false
    }
}

// ── Latest materialization + schedule ───────────────────────────
const { jobsForSource } = useSchedule()

// Recent runs of the job that materialises this asset (newest first, max 7).
// Fetched directly to avoid clobbering the runs store.
const recentRuns = ref<Run[]>([])

async function fetchRecentRuns() {
    const job = jobsForSource(props.source)[0]
    if (!job) {
        recentRuns.value = []
        return
    }
    try {
        recentRuns.value = await apiFetch<Run[]>(`/runs?job_id=${job.id}&limit=7`)
    }
    catch {
        recentRuns.value = []
    }
}
watch(() => props.asset.id, fetchRecentRuns, { immediate: true })

const latestRun = computed(() => recentRuns.value[0])
/** Oldest → newest, for the materialization history heatmap. */
const history = computed(() => [...recentRuns.value].reverse())

const lastRunText = computed(() => {
    const at = latestRun.value?.completed_at ?? latestRun.value?.started_at
    return at ? `${timeSince(new Date(at))} ago` : null
})

/** Latest-materialization presentation: icon, label, colour. */
const materialization = computed(() => {
    switch (latestRun.value?.status) {
        case 'success': return { label: 'Healthy', icon: 'i-lucide-circle-check', color: 'text-[var(--ui-success)]', spin: false }
        case 'failed': return { label: 'Failed', icon: 'i-lucide-circle-x', color: 'text-[var(--ui-error)]', spin: false }
        case 'running': return { label: 'Running', icon: 'i-lucide-loader-circle', color: 'text-[var(--ui-info)]', spin: true }
        case 'canceled': return { label: 'Canceled', icon: 'i-lucide-circle-slash', color: 'text-[var(--ui-warning)]', spin: false }
        case undefined: return { label: 'Not materialized', icon: 'i-lucide-circle-dashed', color: 'text-dimmed', spin: false }
        default: return { label: statusLabel(latestRun.value!.status), icon: 'i-lucide-circle-dot', color: 'text-muted', spin: false }
    }
})

/** Job that materialises this asset (from the latest run, else the source's first job). */
const jobName = computed(() => latestRun.value?.job?.name ?? jobsForSource(props.source)[0]?.name ?? null)

const materializationMeta = computed(() => {
    const head = lastRunText.value ? `Last run ${lastRunText.value}` : 'Not yet materialized'
    return jobName.value ? `${head} · ${jobName.value}` : head
})

/** Heatmap cell colour for a run status. */
function heatColor(status: string): string {
    const color = statusColor(status)
    return color === 'neutral' ? 'var(--ui-text-dimmed)' : `var(--ui-${color})`
}

function historyTooltip(run: Run): string {
    const elapsed = formatElapsed(run.started_at, run.completed_at)
    return `${statusLabel(run.status)}${elapsed ? ` · ${elapsed}` : ''} · ${formatDate(run.started_at)}`
}

/** Upstream dependencies as display rows (param → resolved upstream asset). */
const dependencyRows = computed(() => {
    const reqs = props.assetDefn ? requiredDependencies(props.assetDefn) : {}
    return Object.entries(reqs).map(([param, qk]) => {
        const { sourceKey } = parseQualifiedKey(qk)
        return {
            param,
            qk,
            name: catalogStore.getAssetDefinition(qk)?.name ?? qk,
            icon: componentIcon(sourceKey || qk),
            sourceKey,
        }
    })
})
</script>

<template>
    <div class="flex flex-col h-full">
        <!-- Header -->
        <div class="shrink-0 border-l border-default px-5 py-5">
            <div class="flex items-center gap-3">
                <div class="flex items-center justify-center size-10 rounded-lg bg-elevated shrink-0">
                    <UIcon :name="sourceIcon"
                           class="size-5" />
                </div>
                <div class="min-w-0 mr-4">
                    <h2 class="text-base font-semibold truncate leading-tight">
                        {{ assetDefn?.name ?? asset.key }}
                    </h2>
                    <p class="text-xs text-muted mt-0.5 truncate">
                        {{ sourceDefn?.name ?? source.key }}
                    </p>
                </div>
                <UButton class="shrink-0 ml-auto"
                         icon="i-lucide-x"
                         color="neutral"
                         variant="ghost"
                         size="xs"
                         @click="emit('close')" />
            </div>
        </div>

        <div class="flex-1 min-h-0 border-l border-t border-default overflow-auto">
            <!-- Drift notice — this asset no longer resolves against the catalog. -->
            <UAlert v-if="driftBadge"
                    :color="driftBadge.color"
                    :icon="driftBadge.icon"
                    variant="subtle"
                    class="m-5 mb-0"
                    :title="driftBadge.label"
                    :description="asset.status === 'missing'
                        ? 'Its catalog key was renamed or removed in code. It can\'t materialize — remove the source (or edit it to drop this asset), or restore the catalog entry.'
                        : 'This component is not enabled in the current deployment.'" />

            <!-- Materialization -->
            <UCollapsible default-open
                          class="border-b border-default">
                <button class="flex items-center gap-2 w-full px-5 py-4.5 group cursor-pointer">
                    <UIcon name="i-lucide-chevron-right"
                           class="size-3.5 shrink-0 text-dimmed group-data-[state=open]:rotate-90 transition-transform duration-200" />
                    <span class="text-xs font-semibold text-muted uppercase tracking-wide">Materialization</span>
                </button>

                <template #content>
                    <div class="px-5 pb-4 flex flex-col gap-2">
                        <!-- Latest materialization -->
                        <UCard :ui="{ body: 'flex items-center gap-4 !p-4' }">
                            <UIcon :name="materialization.icon"
                                   class="size-10 shrink-0"
                                   :class="[materialization.color, materialization.spin && 'animate-spin']" />
                            <div class="min-w-0 flex-1">
                                <div class="text-sm font-medium text-highlighted">
                                    Latest materialization: <span :class="materialization.color">{{ materialization.label }}</span>
                                </div>
                                <div class="truncate text-xs text-muted">{{ materializationMeta }}</div>
                            </div>
                        </UCard>

                        <!-- History heatmap (max last 7 runs) -->
                        <UCard v-if="history.length"
                               :ui="{ body: '!p-4' }">
                            <div class="mb-2.5 text-xs font-medium text-muted">Materialization history</div>
                            <div class="flex gap-1.5">
                                <div v-for="run in history"
                                     :key="run.id"
                                     class="size-7 rounded-md"
                                     :style="{ backgroundColor: heatColor(run.status) }"
                                     :title="historyTooltip(run)" />
                            </div>
                        </UCard>
                    </div>
                </template>
            </UCollapsible>

            <!-- Config -->
            <UCollapsible v-if="hasConfig"
                          default-open
                          class="border-b border-default">
                <button class="flex items-center gap-2 w-full px-5 py-4.5 group cursor-pointer">
                    <UIcon name="i-lucide-chevron-right"
                           class="size-3.5 shrink-0 text-dimmed group-data-[state=open]:rotate-90 transition-transform duration-200" />
                    <span class="text-xs font-semibold text-muted uppercase tracking-wide">Config</span>
                </button>

                <template #content>
                    <div class="px-5 pb-4 flex flex-col gap-4">
                        <SchemaForm v-model:data="configData"
                                    v-model:is-valid="configValid"
                                    :schema="configSchema!"
                                    :component-key="assetDefn!.key" />
                        <UButton label="Save"
                                 size="sm"
                                 class="self-end"
                                 :loading="configSaving"
                                 :disabled="!configValid"
                                 @click="saveConfig" />
                    </div>
                </template>
            </UCollapsible>

            <!-- Description -->
            <UCollapsible default-open
                          class="border-b border-default">
                <button class="flex items-center gap-2 w-full px-5 py-4.5 group cursor-pointer">
                    <UIcon name="i-lucide-chevron-right"
                           class="size-3.5 shrink-0 text-dimmed group-data-[state=open]:rotate-90 transition-transform duration-200" />
                    <span class="text-xs font-semibold text-muted uppercase tracking-wide">Description</span>
                </button>

                <template #content>
                    <div class="px-5 pb-4">
                        <p v-if="assetDefn?.description"
                           class="text-sm">
                            {{ assetDefn.description }}
                        </p>
                        <p v-else
                           class="text-sm text-dimmed italic">
                            No description
                        </p>
                    </div>
                </template>
            </UCollapsible>

            <!-- Schema -->
            <UCollapsible v-if="schemaFields.length"
                          default-open
                          class="border-b border-default">
                <button class="flex items-center gap-2 w-full px-5 py-4.5 group cursor-pointer">
                    <UIcon name="i-lucide-chevron-right"
                           class="size-3.5 shrink-0 text-dimmed group-data-[state=open]:rotate-90 transition-transform duration-200" />
                    <span class="text-xs font-semibold text-muted uppercase tracking-wide">Schema</span>
                    <UBadge color="neutral"
                            variant="subtle"
                            class="ml-auto">
                        {{ schemaFields.length }}
                    </UBadge>
                </button>

                <template #content>
                    <div class="px-5 pb-4">
                        <div class="bg-muted rounded-md p-2 overflow-x-auto">
                            <table class="w-full text-sm">
                                <thead>
                                    <tr class="border-b border-default text-left text-xs text-muted">
                                        <th class="p-1.5 font-medium whitespace-nowrap">Name</th>
                                        <th class="p-1.5 font-medium whitespace-nowrap">Type</th>
                                        <th class="p-1.5 font-medium whitespace-nowrap">Description</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr v-for="field in schemaFields"
                                        :key="field.name"
                                        class="border-b border-default last:border-0">
                                        <td class="p-1.5 font-mono text-xs whitespace-nowrap">{{ field.name }}</td>
                                        <td class="p-1.5 whitespace-nowrap">
                                            <UBadge color="neutral"
                                                    variant="subtle">
                                                {{ field.type }}
                                            </UBadge>
                                        </td>
                                        <td class="p-1.5 text-muted whitespace-nowrap">{{ field.description }}</td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </template>
            </UCollapsible>

            <!-- Dependencies -->
            <UCollapsible v-if="dependencyRows.length"
                          default-open
                          class="border-b border-default">
                <button class="flex items-center gap-2 w-full px-5 py-4.5 group cursor-pointer">
                    <UIcon name="i-lucide-chevron-right"
                           class="size-3.5 shrink-0 text-dimmed group-data-[state=open]:rotate-90 transition-transform duration-200" />
                    <span class="text-xs font-semibold text-muted uppercase tracking-wide">Upstream dependencies</span>
                    <UBadge color="neutral"
                            variant="subtle"
                            class="ml-auto">
                        {{ dependencyRows.length }}
                    </UBadge>
                </button>

                <template #content>
                    <div class="px-5 pb-4 flex flex-col gap-1.5">
                        <div v-for="dep in dependencyRows"
                             :key="dep.param"
                             class="flex items-center gap-2.5 rounded-md bg-muted px-3 py-2">
                            <UIcon :name="dep.icon"
                                   class="size-4 shrink-0 text-muted" />
                            <div class="min-w-0 flex-1">
                                <div class="truncate text-sm">{{ dep.name }}</div>
                                <div class="truncate font-mono text-xs text-dimmed">{{ dep.qk }}</div>
                            </div>
                            <UBadge color="neutral"
                                    variant="subtle"
                                    size="sm"
                                    class="shrink-0 font-mono">
                                {{ dep.param }}
                            </UBadge>
                        </div>
                    </div>
                </template>
            </UCollapsible>

            <!-- Destinations -->
            <UCollapsible default-open
                          class="border-b border-default">
                <button class="flex items-center gap-2 w-full px-5 py-4.5 group cursor-pointer">
                    <UIcon name="i-lucide-chevron-right"
                           class="size-3.5 shrink-0 text-dimmed group-data-[state=open]:rotate-90 transition-transform duration-200" />
                    <span class="text-xs font-semibold text-muted uppercase tracking-wide">Destinations</span>
                    <UBadge v-if="destinations.length"
                            color="neutral"
                            variant="subtle"
                            class="ml-auto">
                        {{ destinations.length }}
                    </UBadge>
                </button>

                <template #content>
                    <div class="px-5 pb-4">
                        <div v-if="destinations.length"
                             class="flex flex-col gap-2">
                            <UCard v-for="dest in destinations"
                                   :key="dest.id"
                                   :ui="{ body: 'flex items-center gap-4 !p-4' }">
                                <UIcon :name="dest.icon"
                                       class="size-10 shrink-0" />
                                <div class="min-w-0 flex-1">
                                    <div class="text-sm font-medium">{{ dest.label }}</div>
                                    <div class="text-xs text-muted">{{ dest.key }}</div>
                                </div>
                            </UCard>
                        </div>
                        <p v-else
                           class="text-sm text-dimmed italic">
                            No destinations configured
                        </p>
                    </div>
                </template>
            </UCollapsible>

            <!-- Partitions -->
            <UCollapsible v-if="isPartitioned"
                          default-open
                          class="border-b border-default">
                <button class="flex items-center gap-2 w-full px-5 py-4.5 group cursor-pointer">
                    <UIcon name="i-lucide-chevron-right"
                           class="size-3.5 shrink-0 text-dimmed group-data-[state=open]:rotate-90 transition-transform duration-200" />
                    <span class="text-xs font-semibold text-muted uppercase tracking-wide">Partitions</span>
                    <UBadge v-if="partitionData.length"
                            color="neutral"
                            variant="subtle"
                            class="ml-auto">
                        {{ partitionData.length }}
                    </UBadge>
                </button>

                <template #content>
                    <div class="px-5 pb-4">
                        <!-- Loading -->
                        <div v-if="partitionLoading"
                             class="flex items-center justify-center py-6">
                            <UIcon name="i-lucide-loader-circle"
                                   class="size-5 animate-spin text-muted" />
                        </div>

                        <!-- Not materialized -->
                        <UAlert v-else-if="partitionNotMaterialized"
                                icon="i-lucide-info"
                                color="warning"
                                class="text-sm"
                                title="Asset has not been materialized yet." />

                        <!-- Error -->
                        <UAlert v-else-if="partitionError"
                                icon="i-lucide-triangle-alert"
                                color="error"
                                class="text-sm"
                                :title="partitionError" />

                        <!-- Chart -->
                        <ChartPartitionRowCounts v-else-if="partitionData.length > 0"
                                                 :data="partitionData" />

                        <!-- Empty -->
                        <p v-else
                           class="text-sm text-dimmed italic">
                            No partition data available.
                        </p>
                    </div>
                </template>
            </UCollapsible>

            <!-- Tags -->
            <UCollapsible v-if="assetDefn?.tags?.length"
                          default-open
                          class="border-b border-default">
                <button class="flex items-center gap-2 w-full px-5 py-4.5 group cursor-pointer">
                    <UIcon name="i-lucide-chevron-right"
                           class="size-3.5 shrink-0 text-dimmed group-data-[state=open]:rotate-90 transition-transform duration-200" />
                    <span class="text-xs font-semibold text-muted uppercase tracking-wide">Tags</span>
                </button>

                <template #content>
                    <div class="px-5 pb-4 flex flex-wrap gap-1">
                        <UBadge v-for="tag in assetDefn.tags"
                                :key="tag"
                                variant="subtle"
                                size="sm">
                            {{ tag }}
                        </UBadge>
                    </div>
                </template>
            </UCollapsible>
        </div>
    </div>
</template>
