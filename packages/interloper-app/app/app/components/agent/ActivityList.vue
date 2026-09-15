<script setup lang="ts">
/**
 * The tool calls and specialist handovers of one assistant turn, as a list of
 * collapsible rows: a running step shimmers, a finished one opens onto the
 * payloads it exchanged. The list itself folds into a summary once the work it
 * describes is behind the reader.
 */
import type { AgentActivity } from '~/types/agent'

const props = defineProps<{ activities: AgentActivity[], live?: boolean }>()

/** What each tool is doing, in the reader's terms rather than the function's. */
const TOOL_LABELS: Record<string, string> = {
    list_definitions: 'Browsing the catalog',
    get_definition: 'Reading a definition',
    get_asset_schema: 'Reading an asset schema',
    search_fields: 'Searching fields',
    compare_schemas: 'Comparing schemas',
    consult_catalog: 'Consulting the catalog specialist',
    list_components: 'Listing your components',
    create_connections: 'Creating connections',
    check_connection: 'Checking the connection',
    resolve_source_field_options: 'Fetching options from the source',
    create_source: 'Creating the source',
    create_sources: 'Creating sources',
    update_component: 'Updating the component',
    bind_relation: 'Linking components',
    unbind_relation: 'Unlinking components',
    create_job: 'Creating the job',
    get_upstream: 'Tracing upstream assets',
    get_downstream: 'Tracing downstream assets',
    get_full_lineage: 'Tracing lineage',
    impact_analysis: 'Analysing impact',
    cross_source_dependencies: 'Finding cross-source dependencies',
    list_jobs: 'Listing jobs',
    get_job_health: 'Checking job health',
    toggle_job: 'Switching the job',
    list_recent_runs: 'Reading recent runs',
    get_run_detail: 'Reading the run',
    list_failures: 'Collecting recent failures',
    trigger_run: 'Triggering the run',
    list_backfills: 'Listing backfills',
    trigger_backfill: 'Starting the backfill',
    toggle_asset: 'Switching the asset',
    run_history_summary: 'Summarising run history',
    partition_coverage: 'Checking partition coverage',
    freshness_check: 'Checking data freshness',
}

/** The specialists a handover can name. */
const AGENT_LABELS: Record<string, string> = {
    CatalogAgent: 'the catalog specialist',
    CollectionAgent: 'the collection specialist',
    LineageAgent: 'the lineage specialist',
    SchedulingAgent: 'the scheduling specialist',
    AnalyticsAgent: 'the analytics specialist',
    InterloperAgent: 'the main assistant',
}

const ICONS: Record<AgentActivity['kind'], string> = {
    tool: 'i-lucide-wrench',
    transfer: 'i-lucide-corner-down-right',
}

/** Cap the payload preview: a catalog listing runs to hundreds of kilobytes. */
const PREVIEW_LIMIT = 4000

const failed = computed(() => props.activities.filter(a => a.state === 'error').length)
const summary = computed(() => `${props.activities.length} step${props.activities.length === 1 ? '' : 's'}`)

/**
 * Open while the turn is still adding to the list, folded once it has moved on,
 * and pinned either way the moment the reader takes the decision over.
 *
 * Liveness is the turn's, not the list's: collapsing the instant every step
 * happened to have settled would fold the list between one tool call and the
 * next, which is precisely when someone is watching it.
 */
const open = ref(!!props.live)
const pinned = ref(false)

watch(() => props.live, (live) => {
    if (!pinned.value) open.value = !!live
})

function setOpen(value: boolean) {
    pinned.value = true
    open.value = value
}

function label(activity: AgentActivity) {
    if (activity.kind === 'transfer') return `Asking ${AGENT_LABELS[activity.name] ?? activity.name}`
    return TOOL_LABELS[activity.name] ?? _readable(activity.name)
}

function icon(activity: AgentActivity) {
    return activity.state === 'error' ? 'i-lucide-triangle-alert' : ICONS[activity.kind]
}

function preview(payload: Record<string, any>) {
    const json = JSON.stringify(payload, null, 2)
    return json.length > PREVIEW_LIMIT ? `${json.slice(0, PREVIEW_LIMIT)}\n…` : json
}

/** Fall back to the bare function name, made readable, for a tool with no label. */
function _readable(name: string) {
    const words = name.replace(/_/g, ' ')
    return words.charAt(0).toUpperCase() + words.slice(1)
}
</script>

<template>
    <UChatTool :text="summary"
               :suffix="failed ? `${failed} failed` : undefined"
               :icon="failed ? 'i-lucide-triangle-alert' : 'i-lucide-list-checks'"
               :open="open"
               :ui="failed ? { leadingIcon: 'text-error' } : undefined"
               @update:open="setOpen">
        <div class="flex flex-col items-start gap-0.5 py-1">
            <UChatTool v-for="activity in props.activities"
                       :key="activity.id"
                       :text="label(activity)"
                       :icon="icon(activity)"
                       :loading="activity.state === 'running'"
                       :streaming="activity.state === 'running'"
                       :ui="activity.state === 'error' ? { leadingIcon: 'text-error' } : undefined">
                <div class="flex flex-col gap-2 py-1">
                    <div v-if="activity.args">
                        <div class="eyebrow text-dimmed mb-1">Input</div>
                        <pre class="overflow-x-auto rounded-md bg-elevated/50 p-2 text-[11px]/4 font-mono text-toned">{{ preview(activity.args) }}</pre>
                    </div>
                    <div v-if="activity.response">
                        <div class="eyebrow text-dimmed mb-1">Output</div>
                        <pre class="overflow-x-auto rounded-md bg-elevated/50 p-2 text-[11px]/4 font-mono text-toned">{{ preview(activity.response) }}</pre>
                    </div>
                    <p v-if="!activity.args && !activity.response"
                       class="text-[12px] text-dimmed">
                        No details.
                    </p>
                </div>
            </UChatTool>
        </div>
    </UChatTool>
</template>
