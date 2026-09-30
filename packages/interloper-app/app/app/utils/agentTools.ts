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

/** The label of a tool call in the given tense; a tool with no entry gets its name made readable. */
export function toolLabel(name: string, running: boolean): string {
    const labels = LABELS[name]
    if (labels) return labels[running ? 0 : 1]
    const words = name.replace(/_/g, ' ')
    return words.charAt(0).toUpperCase() + words.slice(1)
}
