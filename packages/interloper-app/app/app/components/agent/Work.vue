<script setup lang="ts">
/**
 * One turn's work trail: what the model thought and what it did, in order,
 * behind a row that collapses the lot.
 *
 * Each entry is the Nuxt UI chat component built for it: a thought is a
 * `UChatReasoning`, which opens itself while the model is still reasoning and
 * settles into "Thought for N seconds"; a step is a `UChatTool`, which shimmers
 * its label while it runs and opens onto the payloads it exchanged. They sit in
 * the body of one more `UChatTool`, closed until asked for.
 */
import type { AgentActivity, AgentStep } from '~/types/agent'

const props = defineProps<{ steps: AgentStep[], streaming?: boolean }>()

/** What each tool is doing, then what it did: a trail is read in both tenses. */
const TOOL_LABELS: Record<string, [running: string, done: string]> = {
    list_definitions: ['Browsing the catalog', 'Browsed the catalog'],
    get_definition: ['Reading a definition', 'Read a definition'],
    get_asset_schema: ['Reading an asset schema', 'Read an asset schema'],
    search_fields: ['Searching fields', 'Searched fields'],
    compare_schemas: ['Comparing schemas', 'Compared schemas'],
    consult_catalog: ['Consulting the catalog specialist', 'Consulted the catalog specialist'],
    list_components: ['Listing your components', 'Listed your components'],
    create_connections: ['Creating connections', 'Created connections'],
    check_connection: ['Checking the connection', 'Checked the connection'],
    resolve_source_field_options: ['Fetching options from the source', 'Fetched options from the source'],
    create_source: ['Creating the source', 'Created the source'],
    create_sources: ['Creating sources', 'Created sources'],
    update_component: ['Updating the component', 'Updated the component'],
    bind_relation: ['Linking components', 'Linked components'],
    unbind_relation: ['Unlinking components', 'Unlinked components'],
    create_job: ['Creating the job', 'Created the job'],
    get_upstream: ['Tracing upstream assets', 'Traced upstream assets'],
    get_downstream: ['Tracing downstream assets', 'Traced downstream assets'],
    get_full_lineage: ['Tracing lineage', 'Traced lineage'],
    impact_analysis: ['Analysing impact', 'Analysed impact'],
    cross_source_dependencies: ['Finding cross-source dependencies', 'Found cross-source dependencies'],
    list_jobs: ['Listing jobs', 'Listed jobs'],
    get_job_health: ['Checking job health', 'Checked job health'],
    toggle_job: ['Switching the job', 'Switched the job'],
    list_recent_runs: ['Reading recent runs', 'Read recent runs'],
    get_run_detail: ['Reading the run', 'Read the run'],
    list_failures: ['Collecting recent failures', 'Collected recent failures'],
    trigger_run: ['Triggering the run', 'Triggered the run'],
    list_backfills: ['Listing backfills', 'Listed backfills'],
    trigger_backfill: ['Starting the backfill', 'Started the backfill'],
    toggle_asset: ['Switching the asset', 'Switched the asset'],
    run_history_summary: ['Summarising run history', 'Summarised run history'],
    partition_coverage: ['Checking partition coverage', 'Checked partition coverage'],
    freshness_check: ['Checking data freshness', 'Checked data freshness'],
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

/** Only the trail's last entry can still be in flight, and only while the turn is. */
const live = computed(() => props.streaming ? props.steps[props.steps.length - 1] : undefined)

const activities = computed(() => props.steps.filter((step): step is AgentActivity => step.kind !== 'thought'))
const failed = computed(() => activities.value.filter(activity => activity.state === 'error').length)
const seconds = computed(() => props.steps.reduce((total, step) => total + (step.kind === 'thought' ? step.seconds ?? 0 : 0), 0))
const count = computed(() => `${activities.value.length} step${activities.value.length === 1 ? '' : 's'}`)

/**
 * What the closed trail says: the step it is on while the turn runs, and what
 * the turn amounted to once it is done.
 *
 * The *last* step rather than the one still running, because a tool settles in
 * milliseconds and the model then thinks for seconds. Keying on `running` put
 * the name up for an instant and took it straight back down.
 */
const summaryText = computed(() => {
    if (props.streaming) {
        const last = props.steps[props.steps.length - 1]
        return last && last.kind !== 'thought' ? label(last) : 'Thinking...'
    }
    // Bare "Thought" where the timings never came through, which is the same
    // thing UChatReasoning says of a block it could not time.
    return seconds.value ? `Thought for ${_duration(seconds.value)}` : 'Thought'
})

/** The count rides alongside, unless something failed, which is worth the space instead. */
const summarySuffix = computed(() => {
    if (props.streaming) return undefined
    if (failed.value) return `${failed.value} failed`
    return activities.value.length ? count.value : undefined
})

const summaryIcon = computed(() => failed.value ? 'i-lucide-triangle-alert' : 'i-lucide-list-checks')

function label(activity: AgentActivity) {
    const running = activity.state === 'running'
    if (activity.kind === 'transfer') {
        const specialist = AGENT_LABELS[activity.name] ?? activity.name
        return running ? `Asking ${specialist}` : `Asked ${specialist}`
    }
    return TOOL_LABELS[activity.name]?.[running ? 0 : 1] ?? _readable(activity.name)
}

function icon(activity: AgentActivity) {
    return activity.state === 'error' ? 'i-lucide-triangle-alert' : ICONS[activity.kind]
}

function preview(payload: Record<string, any>) {
    const json = JSON.stringify(payload, null, 2)
    return json.length > PREVIEW_LIMIT ? `${json.slice(0, PREVIEW_LIMIT)}\n…` : json
}

function _duration(seconds: number) {
    if (seconds < 60) return `${seconds} second${seconds === 1 ? '' : 's'}`
    const minutes = Math.floor(seconds / 60)
    return `${minutes} minute${minutes === 1 ? '' : 's'}`
}

/** Fall back to the bare function name, made readable, for a tool with no label. */
function _readable(name: string) {
    const words = name.replace(/_/g, ' ')
    return words.charAt(0).toUpperCase() + words.slice(1)
}
</script>

<template>
    <UChatTool :text="summaryText"
               :suffix="summarySuffix"
               :icon="summaryIcon"
               :loading="props.streaming"
               :streaming="props.streaming"
               chevron="leading"
               :ui="failed ? { leadingIcon: 'text-error' } : undefined">
        <div class="flex flex-col items-start gap-1 w-full">
            <template v-for="step in props.steps"
                      :key="step.id">
                <UChatReasoning v-if="step.kind === 'thought'"
                                :text="step.text"
                                :streaming="step === live"
                                :duration="step.seconds"
                                chevron="leading"
                                class="w-full">
                    <MDC :value="step.text"
                         :cache-key="step.id"
                         class="*:first:mt-0 *:last:mb-0" />
                </UChatReasoning>

                <UChatTool v-else
                           :text="label(step)"
                           :icon="icon(step)"
                           :loading="step.state === 'running'"
                           :streaming="step.state === 'running'"
                           chevron="leading"
                           class="w-full"
                           :ui="step.state === 'error' ? { leadingIcon: 'text-error' } : undefined">
                    <div class="flex flex-col gap-2">
                        <div v-if="step.args">
                            <div class="eyebrow text-dimmed mb-1">Input</div>
                            <pre class="overflow-x-auto rounded-md bg-elevated/50 p-2 text-[11px]/4 font-mono text-toned">{{ preview(step.args) }}</pre>
                        </div>
                        <div v-if="step.response">
                            <div class="eyebrow text-dimmed mb-1">Output</div>
                            <pre class="overflow-x-auto rounded-md bg-elevated/50 p-2 text-[11px]/4 font-mono text-toned">{{ preview(step.response) }}</pre>
                        </div>
                        <p v-if="!step.args && !step.response"
                           class="text-dimmed">
                            No details.
                        </p>
                    </div>
                </UChatTool>
            </template>
        </div>
    </UChatTool>
</template>
