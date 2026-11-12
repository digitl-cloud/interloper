<script setup lang="ts">
import type { JsonSchemaProperty } from '~/types/catalog'
import { parseQualifiedKey, requiredUpstreams } from '~/types/catalog'
import type { ComponentRecord } from '~/types/component'
import { relationIds } from '~/types/component'
import type { Page } from '~/composables/api'
import type { Execution } from '~/types/execution'

/**
 * The open asset's side panel: its identity and warnings, then folding
 * sections for its latest materializations, destinations, partitions,
 * upstream dependencies and schema.
 */
const props = defineProps<{
    asset: ComponentRecord
    assetDefn: AssetDefinition | undefined
    source: ComponentRecord
}>()

const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()
const { apiFetch } = useOrgApi()
const { statusBadge } = useDrift()
const { getWarnings } = useAssetWarnings()
const appConfig = useAppConfig()
const sourceDefn = computed(() => catalogStore.getSourceDefinition(props.source.key))

const driftBadge = computed(() => statusBadge(props.asset.status))

/** Same warnings as the graph node badge; drift supersedes them for missing assets. */
const warnings = computed(() =>
    props.asset.status === 'missing' ? [] : getWarnings(props.asset.id, props.asset.key),
)

const sourceIcon = computed(() => componentIcon(props.source.key, appConfig.icons.source))

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
const partitionError = ref<unknown>(null)
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
            partitionError.value = e
        }
        partitionData.value = []
    }
    finally {
        partitionLoading.value = false
    }
}

watch(() => props.asset.id, () => fetchPartitionCounts(), { immediate: true })

const destinations = computed(() => {
    return relationIds(props.source, 'destinations')
        .map(id => componentsStore.byId(id))
        .filter((d): d is ComponentRecord => !!d)
        .map((dest) => {
            const defn = catalogStore.catalog[dest.key]
            return {
                id: dest.id,
                label: dest.name ?? defn?.name ?? dest.key,
                typeName: defn?.name ?? dest.key,
                icon: componentIcon(dest.key, appConfig.icons.destination),
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

const schemaColumns = [
    { accessorKey: 'name', header: 'Name' },
    { accessorKey: 'type', header: 'Type' },
    { accessorKey: 'description', header: 'Description' },
]

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

// ── Latest materialization + schedule ───────────────────────────
const { jobsForSource } = useSchedule()

// This asset's own executions, not its job's runs: a run spans every asset of
// the source, so a retry of one asset would show the others as running.
const { latestByAssetId } = storeToRefs(useExecutionsStore())
const recentExecutions = ref<Execution[]>([])

async function fetchRecentExecutions() {
    try {
        recentExecutions.value = (await apiFetch<Page<Execution>>(`/executions?component_id=${props.asset.id}&sort=-created_at&limit=7`)).items
    }
    catch {
        recentExecutions.value = []
    }
}
watch(
    [() => props.asset.id, () => latestByAssetId.value.get(props.asset.id)],
    fetchRecentExecutions,
    { immediate: true },
)

const latestExecution = computed(() => recentExecutions.value[0])
/** Oldest → newest, for the materialization history heatmap. */
const history = computed(() => [...recentExecutions.value].reverse())

const lastRunText = computed(() => {
    const at = latestExecution.value?.completed_at ?? latestExecution.value?.started_at
    return at ? relativeTime(new Date(at)) : null
})

/** Job that materialises this asset (the source's first job). */
const jobName = computed(() => jobsForSource(props.source)[0]?.name ?? null)

const materializationMeta = computed(() => {
    const head = lastRunText.value ? `Last run ${lastRunText.value}` : 'Not yet materialized'
    return jobName.value ? `${head} · ${jobName.value}` : head
})

function historyTooltip(execution: Execution): string {
    const elapsed = formatElapsed(execution.started_at, execution.completed_at)
    return `${statusMeta(execution.status).label}${elapsed ? ` · ${elapsed}` : ''} · ${formatDate(execution.started_at)}`
}

/** Upstream dependencies as display rows (param → resolved upstream asset). */
const dependencyRows = computed(() => {
    const reqs = props.assetDefn ? requiredUpstreams(props.assetDefn) : {}
    return Object.entries(reqs).map(([param, qk]) => {
        const { sourceKey } = parseQualifiedKey(qk)
        return {
            param,
            name: catalogStore.getAssetDefinition(qk)?.name ?? qk,
            sourceName: (sourceKey && catalogStore.getSourceDefinition(sourceKey)?.name) || sourceKey || qk,
            icon: componentIcon(sourceKey || qk, appConfig.icons.source),
        }
    })
})
</script>

<template>
    <div class="flex flex-col gap-3">
        <!-- Folded, the header keeps the asset's identity: logo, name, source and type. -->
        <GraphPanelSection class="shrink-0">
            <template #title>
                <span class="flex min-w-0 flex-1 items-center gap-3 text-left">
                    <span class="flex size-10 shrink-0 items-center justify-center rounded-md bg-elevated">
                        <UIcon :name="sourceIcon"
                               class="size-6" />
                    </span>
                    <span class="min-w-0 flex-1">
                        <span class="block truncate font-semibold text-highlighted">
                            {{ assetDefn?.name ?? asset.key }}
                        </span>
                        <span class="mt-0.5 flex min-w-0 items-center gap-2">
                            <span class="truncate text-xs text-muted">{{ sourceDefn?.name ?? source.key }}</span>
                            <UBadge v-for="tag in assetDefn?.tags ?? []"
                                    :key="tag"
                                    color="neutral"
                                    size="sm"
                                    :label="tag" />
                        </span>
                    </span>
                </span>
            </template>

            <p v-if="assetDefn?.description"
               class="text-sm text-muted">
                {{ assetDefn.description }}
            </p>
            <p v-else
               class="text-sm text-dimmed italic">
                No description
            </p>

            <!-- Warnings: the same set as the graph node's badge. -->
            <UAlert v-if="warnings.length"
                    color="warning"
                    :icon="appConfig.ui.icons.warning"
                    variant="subtle"
                    class="mt-3"
                    :title="warnings.length === 1 ? 'Warning' : `${warnings.length} warnings`">
                <template #description>
                    <ul class="flex flex-col gap-1"
                        :class="warnings.length > 1 && 'list-disc pl-4'">
                        <li v-for="(w, i) in warnings"
                            :key="i">
                            {{ w.message }}
                        </li>
                    </ul>
                </template>
            </UAlert>
        </GraphPanelSection>

        <!-- p-px keeps the cards' rings inside the scroll box, which clips overflow -->
        <div class="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto p-px">
            <!-- Drift notice: this asset no longer resolves against the catalog. -->
            <UAlert v-if="driftBadge"
                    :color="driftBadge.color"
                    :icon="driftBadge.icon"
                    variant="subtle"
                    :title="driftBadge.label"
                    :description="asset.status === 'missing'
                        ? 'Its catalog key was renamed or removed in code, so it can\'t materialize. Remove the source (or edit it to drop this asset), or restore the catalog entry.'
                        : 'This component is not enabled in the current deployment.'" />

            <GraphPanelSection title="Materialization">
                <div class="flex flex-col gap-4">
                    <div class="flex items-center gap-3">
                        <div class="min-w-0 flex-1">
                            <div class="text-sm font-medium text-highlighted">Latest materialization</div>
                            <div class="truncate text-xs text-muted tabular-nums">{{ materializationMeta }}</div>
                        </div>
                        <StatusBadge v-if="latestExecution"
                                     :status="latestExecution.status" />
                        <UBadge v-else
                                color="neutral"
                                label="Not materialized" />
                    </div>

                    <!-- History heatmap (max last 7 runs) -->
                    <div v-if="history.length">
                        <div class="caption mb-2 text-muted">History</div>
                        <div class="flex gap-1.5">
                            <UTooltip v-for="execution in history"
                                      :key="execution.run_id"
                                      :text="historyTooltip(execution)"
                                      :delay-duration="0">
                                <div class="size-7 rounded-md"
                                     :class="STATUS_BG[statusMeta(execution.status).color]" />
                            </UTooltip>
                        </div>
                    </div>
                </div>
            </GraphPanelSection>

            <GraphPanelSection title="Destinations"
                               :count="destinations.length">
                <ul v-if="destinations.length"
                    class="divide-y divide-default">
                    <li v-for="dest in destinations"
                        :key="dest.id"
                        class="flex items-center gap-3 py-2">
                        <UIcon :name="dest.icon"
                               class="size-5 shrink-0" />
                        <div class="min-w-0 flex-1">
                            <div class="truncate text-sm font-medium">{{ dest.label }}</div>
                            <div class="text-xs text-muted">{{ dest.typeName }}</div>
                        </div>
                    </li>
                </ul>
                <UEmpty v-else
                        variant="naked"
                        size="sm"
                        :icon="appConfig.icons.destination"
                        title="No destinations configured" />
            </GraphPanelSection>

            <GraphPanelSection v-if="isPartitioned"
                               title="Partitions"
                               :count="partitionData.length">
                <div v-if="partitionLoading"
                     class="flex items-center justify-center py-6">
                    <UIcon :name="appConfig.ui.icons.loading"
                           class="size-5 animate-spin text-dimmed" />
                </div>
                <UEmpty v-else-if="partitionNotMaterialized"
                        variant="naked"
                        size="sm"
                        :icon="appConfig.icons.missing"
                        title="Not materialized yet" />
                <LoadError v-else-if="partitionError"
                           title="Couldn't load partitions"
                           :error="partitionError"
                           @retry="fetchPartitionCounts" />
                <ChartPartitionRowCounts v-else-if="partitionData.length > 0"
                                         :data="partitionData" />
                <UEmpty v-else
                        variant="naked"
                        size="sm"
                        :icon="appConfig.icons.partition"
                        title="No partition data" />
            </GraphPanelSection>

            <GraphPanelSection v-if="dependencyRows.length"
                               title="Upstream dependencies"
                               :count="dependencyRows.length">
                <ul class="divide-y divide-default">
                    <li v-for="dep in dependencyRows"
                        :key="dep.param"
                        class="flex items-center gap-3 py-2">
                        <UIcon :name="dep.icon"
                               class="size-4 shrink-0 text-muted" />
                        <div class="min-w-0 flex-1">
                            <div class="truncate text-sm">{{ dep.name }}</div>
                            <div class="truncate text-xs text-muted">{{ dep.sourceName }}</div>
                        </div>
                        <UBadge color="neutral"
                                size="sm"
                                class="shrink-0"
                                :label="dep.param" />
                    </li>
                </ul>
            </GraphPanelSection>

            <GraphPanelSection v-if="schemaFields.length"
                               title="Schema"
                               :count="schemaFields.length">
                <UTable :data="schemaFields"
                        :columns="schemaColumns"
                        :ui="{ td: 'whitespace-nowrap' }">
                    <template #name-cell="{ row }">
                        <span class="text-highlighted">{{ row.original.name }}</span>
                    </template>
                    <template #type-cell="{ row }">
                        <UBadge color="neutral"
                                size="sm"
                                :label="row.original.type" />
                    </template>
                </UTable>
            </GraphPanelSection>
        </div>
    </div>
</template>
