<script setup lang="ts">
/**
 * One turn's work trail: what the model thought and what it did, in order,
 * behind a single collapsible.
 *
 * Folded by default. The trigger is the turn's progress indicator, naming the
 * step the trail is on while it runs and settling into how long the whole thing
 * took, so the account is legible without opening anything.
 */
import type { AgentActivity, AgentStep } from '~/types/agent'

const props = defineProps<{ steps: AgentStep[], cacheKey: string, seconds?: number, streaming?: boolean }>()

const appConfig = useAppConfig()

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

/**
 * The trigger's leading slot swaps its icon for the chevron on hover and while
 * open, the way the Nuxt UI chat components do their own.
 */
const RESTING = 'absolute inset-0 size-4 transition-opacity duration-200 ease-out group-hover:opacity-0 group-data-[state=open]:opacity-0'
const CHEVRON_SWAP = 'absolute inset-0 size-4 opacity-0 transition-[rotate,opacity] duration-200 ease-out group-hover:opacity-100 group-data-[state=open]:opacity-100 group-data-[state=open]:rotate-180 motion-reduce:transition-none'
const CHEVRON_ALONE = 'size-4 shrink-0 transition-transform duration-200 ease-out group-data-[state=open]:rotate-180 motion-reduce:transition-none'

const activities = computed(() => props.steps.filter((step): step is AgentActivity => step.kind !== 'thought'))
const failed = computed(() => activities.value.filter(activity => activity.state === 'error').length)
const thought = computed(() => props.steps.some(step => step.kind === 'thought'))

/**
 * What the trigger says while the turn runs: the step the trail is on.
 *
 * The *last* step rather than the one still running, because a tool settles in
 * milliseconds and the model then thinks for seconds. Keying on `running` put
 * the name up for an instant and took it away again.
 */
const current = computed(() => {
    const last = props.steps[props.steps.length - 1]
    return last && last.kind !== 'thought' ? label(last) : 'Thinking...'
})

/** What it says once the turn is done: how long it thought, or what it did if it reported no thinking. */
const settled = computed(() => {
    if (!thought.value) return `${activities.value.length} step${activities.value.length === 1 ? '' : 's'}`
    if (props.seconds === undefined) return 'Thought'
    return `Thought for ${_duration(props.seconds)}`
})

function label(activity: AgentActivity) {
    if (activity.kind === 'transfer') return `Asking ${AGENT_LABELS[activity.name] ?? activity.name}`
    return TOOL_LABELS[activity.name] ?? _readable(activity.name)
}

function icon(activity: AgentActivity) {
    return activity.state === 'error' ? 'i-lucide-triangle-alert' : ICONS[activity.kind]
}

function preview(payload: Record<string, any>) {
    const json = JSON.stringify(payload, null, 2)
    return json.length > PREVIEW_LIMIT ? `${json.slice(0, PREVIEW_LIMIT)}\n\u2026` : json
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
    <UCollapsible>
        <button type="button"
                class="group flex w-full items-center gap-1.5 min-w-0 rounded-sm text-sm text-muted hover:text-default transition-colors">
            <span class="relative size-4 shrink-0">
                <AgentIndicator v-if="props.streaming"
                                :class="RESTING" />
                <UIcon v-else-if="failed"
                       name="i-lucide-triangle-alert"
                       :class="[RESTING, 'text-error']" />
                <UIcon :name="appConfig.ui.icons.chevronDown"
                       :class="props.streaming || failed ? CHEVRON_SWAP : CHEVRON_ALONE" />
            </span>

            <UChatShimmer v-if="props.streaming"
                          :text="current"
                          class="truncate" />
            <span v-else
                  class="truncate">{{ settled }}</span>
        </button>

        <template #content>
            <!-- Bounded while the trail is still being written, in full once the
                 reader has opened a finished one. -->
            <div class="flex flex-col items-start gap-2 pt-2 text-sm text-dimmed"
                 :class="props.streaming && 'max-h-[200px] overflow-y-auto'">
                <template v-for="step in props.steps"
                          :key="step.id">
                    <MDC v-if="step.kind === 'thought'"
                         :value="step.text"
                         :cache-key="`${props.cacheKey}-${step.id}`"
                         class="*:first:mt-0 *:last:mb-0" />

                    <UChatTool v-else
                               :text="label(step)"
                               :icon="icon(step)"
                               :loading="step.state === 'running'"
                               :streaming="step.state === 'running'"
                               chevron="leading"
                               :ui="step.state === 'error' ? { leadingIcon: 'text-error' } : undefined">
                        <div class="flex flex-col gap-2 py-1">
                            <div v-if="step.args">
                                <div class="eyebrow text-dimmed mb-1">Input</div>
                                <pre class="overflow-x-auto rounded-md bg-elevated/50 p-2 text-[11px]/4 font-mono text-toned">{{ preview(step.args) }}</pre>
                            </div>
                            <div v-if="step.response">
                                <div class="eyebrow text-dimmed mb-1">Output</div>
                                <pre class="overflow-x-auto rounded-md bg-elevated/50 p-2 text-[11px]/4 font-mono text-toned">{{ preview(step.response) }}</pre>
                            </div>
                            <p v-if="!step.args && !step.response"
                               class="text-[12px] text-dimmed">
                                No details.
                            </p>
                        </div>
                    </UChatTool>
                </template>
            </div>
        </template>
    </UCollapsible>
</template>
