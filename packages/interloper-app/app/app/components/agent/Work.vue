<script setup lang="ts">
/**
 * One turn's work trail: what the model thought and what it did, in order,
 * behind a single collapsible.
 *
 * Thinking and acting are one account, not two, so they share a disclosure: the
 * header is the Nuxt UI chat template's reasoning trigger, shimmering while the
 * turn is still working and settling into "Thought for N seconds". A turn whose
 * model reports no thoughts falls back to a plain count of its steps.
 */
// Resolved by name at runtime, so they have to be imported rather than left to
// the template compiler's auto-import.
import { UChatReasoning, UChatTool } from '#components'
import type { AgentActivity, AgentStep } from '~/types/agent'

const props = defineProps<{ steps: AgentStep[], cacheKey: string, seconds?: number, streaming?: boolean }>()

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

const activities = computed(() => props.steps.filter(step => step.kind !== 'thought') as AgentActivity[])
const failed = computed(() => activities.value.filter(a => a.state === 'error').length)

/**
 * The thoughts, joined.
 *
 * `UChatReasoning` decides whether there is anything to open from its `text`,
 * and an empty one means this model reported none, which is what sends the
 * trail to the plain-count header instead.
 */
const reasoning = computed(() => props.steps.filter(step => step.kind === 'thought').map(step => step.text).join('\n\n'))

/** The disclosure the trail hangs off: the reasoning trigger where there is reasoning, a step count otherwise. */
const header = computed(() => {
    if (reasoning.value) {
        return {
            is: UChatReasoning,
            props: { text: reasoning.value, streaming: props.streaming, duration: props.seconds },
        }
    }
    const count = activities.value.length
    return {
        is: UChatTool,
        props: {
            text: `${count} step${count === 1 ? '' : 's'}`,
            suffix: failed.value ? `${failed.value} failed` : undefined,
        },
    }
})

/**
 * Both header components theme their body for preformatted prose, which this
 * one is not: it holds rendered markdown and step rows. The height cap earns
 * its keep while the turn is still writing, bounding a trail that has no end
 * in sight, but not once the reader has opened a finished one deliberately.
 */
const ui = computed(() => ({
    body: props.streaming ? 'whitespace-normal' : 'max-h-none whitespace-normal',
    ...(failed.value ? { leadingIcon: 'text-error' } : {}),
}))

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

/** Fall back to the bare function name, made readable, for a tool with no label. */
function _readable(name: string) {
    const words = name.replace(/_/g, ' ')
    return words.charAt(0).toUpperCase() + words.slice(1)
}
</script>

<template>
    <component :is="header.is"
               v-bind="header.props"
               :icon="failed ? 'i-lucide-triangle-alert' : undefined"
               chevron="leading"
               :ui="ui">
        <div class="flex flex-col items-start gap-2">
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
    </component>
</template>
