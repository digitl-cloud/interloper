<script setup lang="ts">
/**
 * One tool call of a turn, as a `UChatTool`: what it is doing while it runs,
 * what it did once done, Approve/Deny while the agent waits for the user, and
 * the payloads it exchanged behind the chevron.
 */
import type { DynamicToolUIPart, ToolUIPart } from 'ai'
import { getToolName } from 'ai'
import { isToolApprovalPending, isToolStreaming } from '@nuxt/ui/utils/ai'

const props = defineProps<{ part: ToolUIPart | DynamicToolUIPart }>()

const emit = defineEmits<{
    approve: [id: string, approved: boolean]
}>()

/** What each tool is doing, then what it did: a trail is read in both tenses. */
const LABELS: Record<string, [running: string, done: string]> = {
    list_definitions: ['Browsing the catalog', 'Browsed the catalog'],
    get_definition: ['Reading a definition', 'Read a definition'],
    get_asset_schema: ['Reading an asset schema', 'Read an asset schema'],
    search_fields: ['Searching fields', 'Searched fields'],
    compare_schemas: ['Comparing schemas', 'Compared schemas'],
    list_components: ['Listing your components', 'Listed your components'],
    update_component: ['Updating the component', 'Updated the component'],
    bind_relation: ['Linking components', 'Linked components'],
    unbind_relation: ['Unlinking components', 'Unlinked components'],
    check_connection: ['Checking the connection', 'Checked the connection'],
    create_connections: ['Creating connections', 'Created connections'],
    resolve_source_field_options: ['Fetching options from the source', 'Fetched options from the source'],
    create_source: ['Creating the source', 'Created the source'],
    create_sources: ['Creating sources', 'Created sources'],
    create_job: ['Creating the job', 'Created the job'],
    get_upstream: ['Tracing upstream assets', 'Traced upstream assets'],
    get_downstream: ['Tracing downstream assets', 'Traced downstream assets'],
    get_full_lineage: ['Tracing lineage', 'Traced lineage'],
    impact_analysis: ['Analysing impact', 'Analysed impact'],
    cross_source_dependencies: ['Finding cross-source dependencies', 'Found cross-source dependencies'],
    list_jobs: ['Listing jobs', 'Listed jobs'],
    get_job_health: ['Checking job health', 'Checked job health'],
    toggle_job: ['Switching the job', 'Switched the job'],
    toggle_asset: ['Switching the asset', 'Switched the asset'],
    list_recent_runs: ['Reading recent runs', 'Read recent runs'],
    get_run_detail: ['Reading the run', 'Read the run'],
    list_run_events: ['Reading the run\'s events', 'Read the run\'s events'],
    get_event: ['Reading an event', 'Read an event'],
    list_failures: ['Collecting recent failures', 'Collected recent failures'],
    error_breakdown: ['Breaking down the errors', 'Broke down the errors'],
    trigger_run: ['Triggering the run', 'Triggered the run'],
    retry_run: ['Retrying the run', 'Retried the run'],
    list_backfills: ['Listing backfills', 'Listed backfills'],
    backfill_timeline: ['Reading the backfill\'s timeline', 'Read the backfill\'s timeline'],
    trigger_backfill: ['Starting the backfill', 'Started the backfill'],
    cancel_backfill: ['Canceling the backfill', 'Canceled the backfill'],
    run_history_summary: ['Summarising run history', 'Summarised run history'],
    run_stats: ['Computing run statistics', 'Computed run statistics'],
    partition_coverage: ['Checking partition coverage', 'Checked partition coverage'],
    asset_coverage: ['Checking asset coverage', 'Checked asset coverage'],
    freshness_check: ['Checking data freshness', 'Checked data freshness'],
}

/** Cap the payload preview: a catalog listing runs to hundreds of kilobytes. */
const PREVIEW_LIMIT = 4000

const name = computed(() => getToolName(props.part))
const running = computed(() => isToolStreaming(props.part))
const pending = computed(() => isToolApprovalPending(props.part))
const failed = computed(() => props.part.state === 'output-error' || _failed(props.part.output))
const denied = computed(() => props.part.state === 'output-denied')

const text = computed(() => {
    if (pending.value) return `Approve: ${readable(name.value)}?`
    if (denied.value) return `${readable(name.value)} — denied`
    const labels = LABELS[name.value]
    return labels ? labels[running.value ? 0 : 1] : readable(name.value)
})

const icon = computed(() => failed.value ? 'i-lucide-triangle-alert' : pending.value ? 'i-lucide-shield-question' : 'i-lucide-wrench')

const actions = computed(() => {
    if (!pending.value || props.part.state !== 'approval-requested') return undefined
    const id = props.part.approval.id
    return [
        { label: 'Approve', onClick: () => emit('approve', id, true) },
        { label: 'Deny', color: 'neutral' as const, variant: 'ghost' as const, onClick: () => emit('approve', id, false) },
    ]
})

function preview(payload: unknown) {
    const json = JSON.stringify(payload, null, 2)
    return json.length > PREVIEW_LIMIT ? `${json.slice(0, PREVIEW_LIMIT)}\n…` : json
}

/** Fall back to the bare function name, made readable, for a tool with no label. */
function readable(toolName: string) {
    const words = toolName.replace(/_/g, ' ')
    return words.charAt(0).toUpperCase() + words.slice(1)
}

function _failed(output: unknown) {
    return typeof output === 'object' && output !== null && (output as { status?: string }).status === 'error'
}
</script>

<template>
    <UChatTool :text="text"
               :icon="icon"
               :loading="running"
               :streaming="running"
               :actions="actions"
               :variant="pending ? 'card' : 'inline'"
               chevron="leading"
               class="w-full"
               :ui="failed ? { leadingIcon: 'text-error' } : undefined">
        <div class="flex flex-col gap-2">
            <div v-if="part.input !== undefined">
                <div class="eyebrow text-dimmed mb-1">Input</div>
                <pre class="overflow-x-auto rounded-md bg-elevated/50 p-2 text-[11px]/4 font-mono text-toned">{{ preview(part.input) }}</pre>
            </div>
            <div v-if="part.output !== undefined">
                <div class="eyebrow text-dimmed mb-1">Output</div>
                <pre class="overflow-x-auto rounded-md bg-elevated/50 p-2 text-[11px]/4 font-mono text-toned">{{ preview(part.output) }}</pre>
            </div>
            <p v-if="part.state === 'output-error'"
               class="text-error">
                {{ part.errorText }}
            </p>
        </div>
    </UChatTool>
</template>
