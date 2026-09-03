# Web UI

The platform ships a web application over the components the framework defines: browse what is
installed, configure sources and their credentials, attach destinations, schedule jobs, react to
outcomes with hooks, and follow every run down to its events. Nothing on these screens is written
per connector; each page is rendered from component definitions, as
[Generated from definitions](definitions.md) explains. This page walks through the app with the
demo sources that ship with the framework.

## Timeline

![Timeline](../assets/ui/timeline-light.png#only-light)
![Timeline](../assets/ui/timeline-dark.png#only-dark)

The landing page: one row per job and per source, one bar per run over a selectable window, green
for success and red for failure. Hovering a bar shows the run; clicking opens it.

## Graph

![Graph](../assets/ui/graph-light.png#only-light)
![Graph](../assets/ui/graph-dark.png#only-dark)

The lineage of every asset in the organisation, built from the dependency relations the
framework infers (`a` feeds `b`, `c`, `d`, which feed `e`). Nodes carry the asset's tags and its
health; the toolbar groups by asset, by source instance or by source type, and filters by status.
Selecting a node opens a panel with the asset's schema, partitioning, destinations and last runs.

## Collection

![Collection](../assets/ui/collection-light.png#only-light)
![Collection](../assets/ui/collection-dark.png#only-dark)

Every source instance and its assets, grouped by source type. Columns show the last run, the
connection, the number of upstream dependencies, the destinations and the jobs that target each
asset. The catalog of installed but not yet configured types is reachable from the "New source"
button.

## Sources

![Sources](../assets/ui/sources-light.png#only-light)
![Sources](../assets/ui/sources-dark.png#only-dark)

The configured source instances. Two instances of the same type ("Demo Data" and "Demo Flaky")
coexist because each has its own dataset; with a discriminator field they would also have
distinct table names.

### Creating a source

![Source wizard, type step](../assets/ui/source-wizard-types-light.png#only-light)
![Source wizard, type step](../assets/ui/source-wizard-types-dark.png#only-dark)

The wizard opens on the type step: one tile per source definition in the catalog, grouped by tag,
with the icon and name the class declares. Selecting a tile advances.

![Source wizard, assets step](../assets/ui/source-wizard-assets-light.png#only-light)
![Source wizard, assets step](../assets/ui/source-wizard-assets-dark.png#only-dark)

The assets step lists the definition's assets with their docstrings and tags, filterable by tag.
The selection becomes the source's children; assets left out stay available as dependencies.

![Source wizard, connection step](../assets/ui/source-wizard-connection-light.png#only-light)
![Source wizard, connection step](../assets/ui/source-wizard-connection-dark.png#only-dark)

One step per resource slot the source declares. It lists the existing connections of the required
type or creates one inline; the connection form is the same as on the Connections page.

![Source wizard, details step](../assets/ui/source-wizard-details-light.png#only-light)
![Source wizard, details step](../assets/ui/source-wizard-details-dark.png#only-dark)

The details step recaps the earlier choices and renders the form generated from the source's
configuration fields: the framework's own (`materialization_strategy`, `dataset`) and the
class's (`hello`, `random_failure_probability` on the demo source), with the labels,
descriptions and tooltips their field helpers declare.

## Connections

![Connections](../assets/ui/connections-light.png#only-light)
![Connections](../assets/ui/connections-dark.png#only-dark)

Credentials, stored encrypted and shared by every source that needs them. The page lists the
configured connections and, below, the catalog of connection types installed. The sidebar entry
exists because the catalog contains the `connection` resource kind; a package defining another
kind gets its own entry.

![Connection form](../assets/ui/connection-form-light.png#only-light)
![Connection form](../assets/ui/connection-form-dark.png#only-dark)

A connection declaring `OAuthConfig` gets a sign-in tab when the provider's app credentials are
configured on the server, and a manual tab showing every field. The `auto_renew` toggle appears
because the class is renewable; a "Test connection" action appears when it implements `check()`.

## Destinations

![Destinations](../assets/ui/destinations-light.png#only-light)
![Destinations](../assets/ui/destinations-dark.png#only-dark)

Where data lands. A destination's form comes from its configuration fields, and its connection
slot, when it has one, is a picker over the matching connections.

## Jobs

![Jobs](../assets/ui/jobs-light.png#only-light)
![Jobs](../assets/ui/jobs-dark.png#only-dark)

Cron jobs over sources or assets. The schedule column renders the cron expression and its
timezone; "Next run at" and "Last run at" are the job's state model, shown as columns because
the definition publishes a `state_schema`. The job form uses the cron and timezone widgets and
the partition-window fields (`lookback`, `offset`).

## Hooks

![Hooks](../assets/ui/hooks-light.png#only-light)
![Hooks](../assets/ui/hooks-dark.png#only-dark)

Reactions to run outcomes: a webhook posting failures to an operations endpoint, and a trigger
hook starting the monthly rollup when the daily job completes. The watched components and, for
trigger hooks, the targets are relation pickers derived from the hook class's vocabulary.

## Executions

![Executions](../assets/ui/executions-light.png#only-light)
![Executions](../assets/ui/executions-dark.png#only-dark)

Every run with its target, partition key, status and timings. Runs come from schedules, manual
triggers, hooks and backfills.

![Backfills](../assets/ui/backfills-light.png#only-light)
![Backfills](../assets/ui/backfills-dark.png#only-dark)

A backfill is one run per partition over a range, dispatched newest first with a concurrency
cap; the range is shown in the target's partition-key shape.

### Run detail

![Run detail](../assets/ui/run-light.png#only-light)
![Run detail](../assets/ui/run-dark.png#only-dark)

One run: the per-asset execution timeline (or the run's graph), and the full event stream the
framework emitted, filterable to lifecycle events, errors or the log lines assets wrote through
`context.logger`. Failed runs offer "Retry failed" and "Retry all".

## Search and switching

The search box (`⌘K`) opens a command palette over every page, source, job and hook, with
aliases (typing "DAG" finds the graph). The footer switches the organisation and the user, and
toggles light and dark mode.
