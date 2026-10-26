<script setup lang="ts">
import type { JsonSchemaProperty } from '~/types/catalog'
import { parseQualifiedKey, requiredUpstreams } from '~/types/catalog'
import type { ComponentRecord } from '~/types/component'
import { relationIds } from '~/types/component'
import type { Page } from '~/composables/api'
import type { Execution } from '~/types/execution'

const props = defineProps<{
    asset: ComponentRecord
    assetDefn: AssetDefinition | undefined
    source: ComponentRecord
}>()

const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()
const { apiFetch } = useApi()
const { statusBadge } = useDrift()
const { getWarnings } = useAssetWarnings()
const sourceDefn = computed(() => catalogStore.getSourceDefinition(props.source.key))

const driftBadge = computed(() => statusBadge(props.asset.status))

/** Same warnings as the graph node badge; drift supersedes them for missing assets. */
const warnings = computed(() =>
    props.asset.status === 'missing' ? [] : getWarnings(props.asset.id, props.asset.key),
)

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
            partitionError.value = errorDetail(e) ?? 'Failed to load partition data'
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
    return at ? `${timeSince(new Date(at))} ago` : null
})

/** Latest-materialization presentation: icon, label, colour. */
const materialization = computed(() => {
    switch (latestExecution.value?.status) {
        case 'success': return { label: 'Healthy', icon: 'i-lucide-circle-check', color: 'text-[var(--ui-success)]', spin: false }
        case 'failed': return { label: 'Failed', icon: 'i-lucide-circle-x', color: 'text-[var(--ui-error)]', spin: false }
        case 'running': return { label: 'Running', icon: 'i-lucide-loader-circle', color: 'text-[var(--ui-info)]', spin: true }
        case 'canceled': return { label: 'Canceled', icon: 'i-lucide-circle-slash', color: 'text-[var(--ui-warning)]', spin: false }
        case undefined: return { label: 'Not materialized', icon: 'i-lucide-circle-dashed', color: 'text-dimmed', spin: false }
        default: return { label: statusLabel(latestExecution.value!.status), icon: 'i-lucide-circle-dot', color: 'text-muted', spin: false }
    }
})

/** Job that materialises this asset (the source's first job). */
const jobName = computed(() => jobsForSource(props.source)[0]?.name ?? null)

const materializationMeta = computed(() => {
    const head = lastRunText.value ? `Last run ${lastRunText.value}` : 'Not yet materialized'
    return jobName.value ? `${head} · ${jobName.value}` : head
})

/** Heatmap cell colour for a run status. */
function heatColor(status: string): string {
    const color = statusColor(status)
    return color === 'neutral' ? 'var(--ui-text-dimmed)' : `var(--ui-${color})`
}

function historyTooltip(execution: Execution): string {
    const elapsed = formatElapsed(execution.started_at, execution.completed_at)
    return `${statusLabel(execution.status)}${elapsed ? ` · ${elapsed}` : ''} · ${formatDate(execution.started_at)}`
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
            icon: componentIcon(sourceKey || qk),
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
                    <span class="flex items-center justify-center size-10 rounded-lg bg-elevated shrink-0">
                        <UIcon :name="sourceIcon"
                               class="size-5" />
                    </span>
                    <span class="min-w-0 flex-1">
                        <span class="block text-base font-semibold truncate leading-tight text-highlighted">
                            {{ assetDefn?.name ?? asset.key }}
                        </span>
                        <span class="mt-0.5 flex min-w-0 items-center gap-2">
                            <span class="truncate text-xs text-muted">{{ sourceDefn?.name ?? source.key }}</span>
                            <UBadge v-for="tag in assetDefn?.tags ?? []"
                                    :key="tag"
                                    :color="tagColor(tag)"
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

            <!-- Warnings — same set as the graph node badge. -->
            <UAlert v-if="warnings.length"
                    color="warning"
                    icon="i-lucide-triangle-alert"
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
            <!-- Drift notice — this asset no longer resolves against the catalog. -->
            <UAlert v-if="driftBadge"
                    :color="driftBadge.color"
                    :icon="driftBadge.icon"
                    variant="subtle"
                    :title="driftBadge.label"
                    :description="asset.status === 'missing'
                        ? 'Its catalog key was renamed or removed in code. It can\'t materialize — remove the source (or edit it to drop this asset), or restore the catalog entry.'
                        : 'This component is not enabled in the current deployment.'" />

            <GraphPanelSection title="Materialization">
                <div class="flex flex-col gap-2">
                    <div class="flex items-center gap-4 rounded-md bg-elevated p-4">
                        <UIcon :name="materialization.icon"
                               class="size-10 shrink-0"
                               :class="[materialization.color, materialization.spin && 'animate-spin']" />
                        <div class="min-w-0 flex-1">
                            <div class="text-sm font-medium text-highlighted">
                                Latest materialization: <span :class="materialization.color">{{ materialization.label }}</span>
                            </div>
                            <div class="truncate text-xs text-muted">{{ materializationMeta }}</div>
                        </div>
                    </div>

                    <!-- History heatmap (max last 7 runs) -->
                    <div v-if="history.length"
                         class="rounded-md bg-elevated p-4">
                        <div class="mb-2.5 text-xs font-medium text-muted">Materialization history</div>
                        <div class="flex gap-1.5">
                            <div v-for="execution in history"
                                 :key="execution.run_id"
                                 class="size-7 rounded-md"
                                 :style="{ backgroundColor: heatColor(execution.status) }"
                                 :title="historyTooltip(execution)" />
                        </div>
                    </div>
                </div>
            </GraphPanelSection>

            <GraphPanelSection title="Destinations"
                               :count="destinations.length">
                <div v-if="destinations.length"
                     class="flex flex-col gap-2">
                    <div v-for="dest in destinations"
                         :key="dest.id"
                         class="flex items-center gap-4 rounded-md bg-elevated p-4">
                        <UIcon :name="dest.icon"
                               class="size-10 shrink-0" />
                        <div class="min-w-0 flex-1">
                            <div class="truncate text-sm font-medium">{{ dest.label }}</div>
                            <div class="text-xs text-muted">{{ dest.typeName }}</div>
                        </div>
                    </div>
                </div>
                <p v-else
                   class="text-sm text-dimmed italic">
                    No destinations configured
                </p>
            </GraphPanelSection>

            <GraphPanelSection v-if="isPartitioned"
                               title="Partitions"
                               :count="partitionData.length">
                <div v-if="partitionLoading"
                     class="flex items-center justify-center py-6">
                    <UIcon name="i-lucide-loader-circle"
                           class="size-5 animate-spin text-muted" />
                </div>
                <UAlert v-else-if="partitionNotMaterialized"
                        icon="i-lucide-info"
                        color="warning"
                        class="text-sm"
                        title="Asset has not been materialized yet." />
                <UAlert v-else-if="partitionError"
                        icon="i-lucide-triangle-alert"
                        color="error"
                        class="text-sm"
                        :title="partitionError" />
                <ChartPartitionRowCounts v-else-if="partitionData.length > 0"
                                         :data="partitionData" />
                <p v-else
                   class="text-sm text-dimmed italic">
                    No partition data available.
                </p>
            </GraphPanelSection>

            <GraphPanelSection v-if="dependencyRows.length"
                               title="Upstream dependencies"
                               :count="dependencyRows.length">
                <div class="flex flex-col gap-1.5">
                    <div v-for="dep in dependencyRows"
                         :key="dep.param"
                         class="flex items-center gap-2.5 rounded-md bg-elevated px-3 py-2">
                        <UIcon :name="dep.icon"
                               class="size-4 shrink-0 text-muted" />
                        <div class="min-w-0 flex-1">
                            <div class="truncate text-sm">{{ dep.name }}</div>
                            <div class="truncate text-xs text-muted">{{ dep.sourceName }}</div>
                        </div>
                        <!-- Outline: a soft neutral badge is the tile's own tone. -->
                        <UBadge color="neutral"
                                variant="outline"
                                size="sm"
                                class="shrink-0 font-mono">
                            {{ dep.param }}
                        </UBadge>
                    </div>
                </div>
            </GraphPanelSection>

            <GraphPanelSection v-if="schemaFields.length"
                               title="Schema"
                               :count="schemaFields.length">
                <UTable :data="schemaFields"
                        :columns="schemaColumns"
                        class="rounded-md bg-elevated"
                        :ui="{
                            th: 'py-2 px-3 text-xs first:ps-4',
                            td: 'py-2 px-3 text-xs whitespace-nowrap first:ps-4',
                            tbody: 'divide-accented',
                        }">
                    <template #name-cell="{ row }">
                        <span class="font-mono text-default">{{ row.original.name }}</span>
                    </template>
                    <template #type-cell="{ row }">
                        <UBadge color="neutral"
                                variant="outline"
                                size="sm">
                            {{ row.original.type }}
                        </UBadge>
                    </template>
                </UTable>
            </GraphPanelSection>
        </div>
    </div>
</template>
