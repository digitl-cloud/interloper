# Downstream assets: findings and design

Date: 2026-09-04, revised 2026-09-05. Status: Part 1 (findings) stands; Part 2 (design) and Part 3 (phasing) are superseded by `2026-09-07-relation-model-design.md`.

Scope: how an asset consumes other assets in interloper, reviewed end to end (definition, wiring
and DAG, execution, platform and UI), and the design that makes a **campaign matcher** buildable.
The matcher consumes the `campaigns` entity asset of every ad source an organisation has
configured and produces a dated lookup table of canonical campaign names. Its defining trait is
that the number and identity of its upstreams is not fixed at class definition time: it depends
on which sources exist, and users add or remove providers later.

Part 1 records what the review found. Part 2 is the design. Part 3 is the phasing.

---

## Part 1. Findings

Method: read the code, probe with `uv run --frozen python` in a scratchpad, and explore the
platform layers. Severity: **B** blocks the matcher, **D** degrades DX or UX, **C** cosmetic.
File references are to the worktree at commit 0266d74a.

### 1.1 Definition

| Id | Sev | Finding |
|----|-----|---------|
| D1 | B | One upstream per parameter, fixed at class definition. `Asset.dependencies` is `dict[str, str]` and `requires` / `optional_requires` are class-level `dict[str, str]` (asset/base.py:172-194). A parameter cannot hold several upstream ids; every provider needs its own parameter; a new provider is a code change; two instances of one source type cannot feed one parameter. |
| D2 | B | No way to declare "all assets matching a predicate". Slot keys are exact identities, bare or qualified (`AssetIdentity.resolve`, asset/base.py:74-93). `RelationSlot` carries only `key` and `required` (component/base.py:56-63). |
| D3 | D | Parameter-name inference is intra-source only (source/base.py:365-395). A standalone asset with a `campaigns` parameter has no slot at all. |
| D4 | D | The contract reaches clients only as `relations.dependency.slots`. `AssetDefinition` has no `requires` field, yet the toolkit and the agent read `asset_def.get("requires", {})` off the dumped catalog (interloper-toolkit/catalog.py:173-174, interloper-agent/tools/collection.py:546-558), so their reported requirements are always empty. |
| D5 | D | Partition compatibility is one rule: `_check_partition_dependencies` (dag/base.py:186-204) forbids an unpartitioned asset downstream of a partitioned one and nothing else. |
| D6 | D | A fan-in asset's schema has no relation to its upstreams' schemas. Provider `campaigns` schemas disagree already (Facebook and Snapchat `id` / `name`, TikTok `campaign_id` / `campaign_name`). |
| D7 | C | `optional_requires` conflates "may be unbound" with "any read failure yields None" (asset/base.py:708-714). |

### 1.2 Wiring and DAG

| Id | Sev | Finding |
|----|-----|---------|
| W1 | B | Cross-source `requires` is checked but never wired. `Source._resolve_deps` skips foreign keys (source/base.py:551-553); the DAG validates only wired entries (dag/base.py:126-162); the store's `_wire_intra_deps` skips them too (interloper-db/store/components.py:787-827). Probe: `il.DAG(Shop(), Finance())` runs both in one generation and fails with `TypeError: Finance.revenue() missing 1 required positional argument: 'orders'`. |
| W2 | B | An unbound required slot is not an error at build time. `_build_kwargs` skips parameters absent from `dependencies` (asset/base.py:695-696); the failure is a `TypeError` inside `data()`. |
| W3 | D | Manual wiring by id works: it orders correctly, a `Job` can span sources, specs round-trip the edge, a `select=[]` source supplies read-only parents. This is the documented workaround. |
| W4 | D | The DAG checks slot identity, not cardinality or shape. |
| W5 | D | Three representations of the same fact (`requires` on the class, `dependencies` on the instance, `component_relations` rows) with no single reader. Hydration seeds from `config` before overlaying edges (interloper-db/store/hydration.py:141-160), so `config.dependencies` is a back door producing working dependencies with no edge, no lineage and no delete guard. |
| W6 | C | The spec `assets:` map is a whitelist (source/base.py:187-223); listing one asset drops the others. |

### 1.3 Execution

| Id | Sev | Finding |
|----|-----|---------|
| E1 | B | One partition scope per run, applied to every executing node (runner/base.py:254-304). A monthly asset cannot share a DAG with daily upstreams. With the upstream read-only, database destinations read the month's date range by accident while file and memory destinations fail (they key by partition id). |
| E2 | D | Window reads return one result per partition (destination/partitioned.py:94-98), so a windowed downstream receives a list of tables, newest first. The scheduler never runs windows; the CLI and Python users do. |
| E3 | D | `default_destination_key` is never read: `_destination_read` takes `destinations[0]` (asset/base.py:811-814). |
| E4 | D | Granularity mismatch fails late with a per-asset message; `allow_window` is not enforced on the read side. |
| E5 | D | Optional reads swallow every exception (asset/base.py:713). |
| E6 | D | `fail_fast=True` is the platform default (runner/async_runner.py:44-46) and `cancel_pending` cancels branches unrelated to the failure (runner/base.py:224, runner/state.py:253-267). |
| E7 | D | Upstreams outside the run are joined blind by `RunExecutor._resolve_upstream` (interloper-scheduler/executor.py:164-188): no freshness check, and job-level destinations do not cascade to joined nodes. |
| E8 | D | Joined and skipped legs emit no event and `RunResult` has no `skipped_ids` (runner/state.py:129-138, runner/results.py:134-147). |
| E9 | D | Retry re-runs cancelled dependents correctly (executor.py:121-125, 190-219); backfills cover dependents inside the targeted component only. |
| E10 | C | Reads are `SELECT *` with no projection; only CSV and GCS reconcile on read. |

### 1.4 Platform and UI

| Id | Sev | Finding |
|----|-----|---------|
| P1 | B | One edge per slot at three layers: partial unique index `uq_component_relations_slot` (interloper-db/models/components.py:158-166), `_upsert_relation` repointing on rebind (interloper-db/store/relations.py:384-422), `dependencies: dict[str, str]`. |
| P2 | B | Slot targets are validated against exact identities (`_check_slot_target`, relations.py:267-320); a class with no declared slots accepts any slot pointing at any asset (relations.py:256-257). |
| P3 | B | No UI path creates a standalone asset, and the DB allows `parent_id` on assets only. A matcher must be owned by a source to be reachable. |
| P4 | B | Only qualified slots get a picker, per source type (`AssetSelect.vue:59-61`); graph drag-connect matches exact qualified keys (`composables/graph.ts:225-237`); optional slots have no drag handle, no panel row and no warning (`AssetNode.vue:95-96`, `AssetPanel.vue:232-244`, `warnings.ts:102`). |
| P5 | D | The asset panel shows declared slots with the upstream source *type*, never the persisted bindings. The collection column renders a bare count. Edit mode shows bindings as empty (`Wizard.vue:33`) and swallows relation-write errors (`Wizard.vue:89`). |
| P6 | D | A vanished or disabled upstream is silent: the edge disappears from the graph model (`graphModel.ts:24-26`). |
| P7 | D | Neither the agent nor the toolkit can wire a dependency; `update_component` defers rebinding to the app. |
| P8 | D | Jobs may target several sources and assets, but the UI limits asset targets to standalone assets, and `select` is source-row state. |
| P9 | C | The collection panel resolves asset definitions by bare key, first match wins (`pages/collection.vue:31-37`). |
| P10 | C | `campaign_matcher` in interloper-assets is a nine-line stub with a copied docstring and the wrong class name, unexported. No shipped asset declares a dependency. |

### 1.5 Configuring a three-provider matcher today

Code: an asset class with one parameter and one qualified `optional_requires` entry per provider,
owned by a source and enabled in the catalog. Platform: three source creations, three relation
calls (`POST /components/{asset_id}/relations` with `type=dependency`, `slot=<param>`), one job
on the matcher's source. Impossible: a fourth provider without a release (D1), two accounts of
one provider (D1, P1), any predicate declaration (D2), a monthly matcher over daily snapshots in
one DAG (E1), any of it from the UI when the matcher is standalone (P3) or its slots optional (P4).

---

## Part 2. Design

### 2.1 Decisions

Taken with Guillaume on 2026-09-04 and 2026-09-05:

1. **Direction B**: multi-valued upstream slots with a wildcard key, explicit edges kept as the
   source of truth. A (fixed optional slots per provider) and C (predicate resolved at run time
   with no edges) rejected: A does not scale to instances or new providers; C makes lineage
   derived and breaks the delete and drift guards.
2. The matcher is an **Analytics-tagged source owning one daily asset**. No new component kind.
3. `data()` receives **`list[il.Upstream]`** for a many-valued slot (upstream identity plus data).
   Single slots keep receiving raw data.
4. **Matching logic stays empty in v1**: the asset reads its legs and emits placeholder rows.
5. Ride-along fixes: an **unbound non-optional slot fails at DAG build**, and
   **`default_destination_key` is honoured on read**. Everything else in 1.3 is a follow-up.
6. **Granularity must be equal** across any upstream edge between time-partitioned assets,
   read-only upstreams included. Roll-ups over finer legs are a designed follow-up.
7. Slot key grammar v1: **source wildcard only** (`*.campaigns`). Tags and `source.*` deferred.
8. **Auto-binding deferred**: binding stays explicit; the UI surfaces matching-but-unbound assets
   and offers a one-click bind-all.
9. Scheduling v1: **the matcher's own cron job**, legs joined read-only by the executor.
   **Trigger-hook chaining is explored as an experiment** (2.9).
10. **One vocabulary, one class.** `requires` and `optional_requires` become `depends_on`, holding
    strings and `il.Dependency` values (`optional`, `many`); `Dependency` is the renamed
    `RelationSlot`, so the class that declares a slot is the class the catalog publishes, for
    resource slots too. Breaking for docs, tests and the `il.RelationSlot` export only.
11. **No per-slot missing-data policy.** A bound leg with no data for the partition is handed to
    `data()` as `Upstream(asset, data=None)` with a warning event; the asset decides. `optional`
    stays a wiring rule checked at DAG build. Confirmed 2026-09-06 for single slots too: a
    non-optional slot means the wiring is mandatory, not the data; a mandatory single slot whose
    upstream holds no data for the partition receives `None`, since data is expected to be
    occasionally missing.
12. **The relation is `upstream`, the wiring is `upstreams`.** The asset-to-asset relation type
    is renamed from `dependency` to `upstream`, a role name like `target` and `watch`, matching
    the vocabulary the app, the MCP tools and the executor already use. The instance field
    `dependencies` becomes `upstreams`. Every relation is a dependency of sorts; this one points
    upstream.
13. **`upstreams` is always a list.** `dict[str, list[str]]`, for single and many slots alike; a
    bare string is accepted on input. No per-slot shaping, no flattening helper, no hydration
    heuristic.
14. **One read path.** Every slot is read through `_read_upstreams`; a single slot is the first
    leg. A leg with no data for the partition is `None`, for single and many slots alike; any
    other read error fails the asset. The swallow-everything branch for optional slots is gone.
15. **One contract check.** `Asset.validate_upstreams(nodes)` checks signature completeness,
    cardinality and identity; the DAG calls it once per live node.
16. **Bare keys resolve within the source instance.** The DAG's resolution pass restricts
    candidates for a bare key to siblings of the same source instance, so two instances of one
    source type never make a sibling reference ambiguous.
17. **Signatures fail loudly.** A `data()` parameter that is not the context, a resource, or a
    declared upstream, and has no default, is an error at DAG build, not a `TypeError` at run time.
18. **Resources stay declared by `resource_types`.** Folding them into `depends_on` on `Component`
    is compatible with every name chosen here and is recorded as a follow-up (2.8), not done now.

### 2.2 Definition layer (interloper-core)

`Dependency` replaces `RelationSlot` in `interloper/component/base.py` (exported as `il.Dependency`);
a new module `interloper/asset/upstream.py` holds the leg object `il.Upstream`:

```py
class Dependency(BaseModel):
    """One declared dependency of a component on another: the slot on a slotted relation type."""
    key: str = ""             # "campaigns" (sibling), "facebook_ads.campaigns", "*.campaigns"
    optional: bool = False    # may stay unbound (an empty list for a many slot)
    many: bool = False        # binds several upstreams, read into list[Upstream]

@dataclass(frozen=True)
class Upstream:
    """One leg of a many-valued slot as handed to data()."""
    asset: Asset
    data: Any        # None when the upstream holds no data for the partition
```

Declaration:

```py
@il.asset(
    depends_on={"campaigns": il.Dependency(key="*.campaigns", many=True)},
    schema=CampaignMatches,
    partitioning=il.TimePartitionConfig(column="date"),
    tags=["Entity"],
)
def campaign_matches(self, context: il.ExecutionContext, campaigns: list[il.Upstream]) -> list[dict]: ...
```

The vocabulary, side by side with the two relations an asset already has:

| | Resources | Destinations | Upstreams |
|---|---|---|---|
| Declared on the class | `resource_types` (by Python type) | `destination_types` (by type) | `depends_on` (by asset key) |
| Relation type | `resource` | `destination` | `upstream` |
| Wired on the instance | `resources` | `destinations` | `upstreams: dict[str, list[str]]` |
| Slot descriptor in the definition | `Dependency` | none, unslotted | `Dependency` |
| What `data()` receives | the resource instance | not passed | rows, or `list[Upstream]` for a many slot |

- `depends_on: ClassVar[dict[str, str | Dependency]]` replaces `requires` and `optional_requires`.
  A plain string keeps today's meaning (a single, non-optional upstream on that key);
  `Dependency(key=..., optional=True)` replaces `optional_requires`. Sibling inference stays: a
  `data()` parameter named after a sibling asset writes the qualified key into `depends_on`, a
  `None` default makes it optional. Inference is sugar over the one contract; nothing downstream
  knows an entry was inferred.
- The relation type is `upstream`: `RelationDefinition(kinds=["asset"], field="upstreams",
  slotted=True, inline=False, on_unbind="block")`. `Asset.upstreams: dict[str, list[str]]` is the
  wiring, parameter name to upstream ids, always a list; a `mode="before"` validator wraps a bare
  string so hand wiring and existing specs keep working. `to_spec` emits lists.
- `RelationSlot` is renamed `Dependency`; `required: bool = True` becomes `optional: bool = False`
  and `many: bool = False` is added. The catalog JSON changes accordingly (`optional`, `many`).
- `Asset.declared_upstreams()` is the **single reader** of the contract: it turns `depends_on`
  into `dict[str, Dependency]`. `relation_definitions()`, `validate_upstreams()`,
  `_build_kwargs()`, `Source._resolve_upstreams()`, the store and the DAG all read it.
- `Asset.sibling_upstreams(source_key, sibling_keys)` is the one sibling-wiring rule (which
  declared keys resolve to a sibling of the declaring source), used by `Source._resolve_upstreams`
  at construction and by the store's `_wire_intra_upstreams` at creation.
- Key grammar: a bare key is a sibling of the declaring source; `source.asset` is that source
  type, any instance; `*.asset` is that asset key from any source type, standalone assets
  included. `AssetIdentity.satisfies(declared_key, own_source_key=...)` implements the match.
  `AssetIdentity.resolve` is unchanged and yields `("*", asset_key)` for a wildcard.
- Data shape: a single slot receives the upstream's data, or `None` when the slot is optional and
  either unbound or without data for the partition; a many slot receives `list[il.Upstream]`,
  empty when nothing is bound, with `data=None` on a leg without data for the partition.
  `Upstream.asset` gives `source.key`, `source.discriminator`, `id` and `qualified_key`.

### 2.3 Wiring and DAG (interloper-core)

`DAG.__init__` gains a resolution step between flattening and edge building, and one contract
check per live node:

1. **Declared-key resolution.** For every materializable asset and every slot not yet wired,
   candidates are the other assets in the DAG whose identity satisfies the slot key; for a bare
   key the candidates are further restricted to assets of the same source instance. A many slot
   binds every candidate. A single slot binds exactly one; zero candidates leave the slot
   unwired for the contract check; two or more raise `DAGError` naming the candidates and asking
   for explicit wiring. Wiring writes `upstreams` on the asset instance, exactly as
   `Source._resolve_upstreams` does for siblings, so specs and the CLI benefit without a new concept.
2. **Contract.** `Asset.validate_upstreams(nodes)`, called by the DAG for every materializable
   node, checks in order: every `data()` parameter without a default is the context, a resource
   or a declared upstream (`AssetError`, closing the silent inference gap); every non-optional
   slot has at least one wired id present in the DAG (`DependencyNotFoundError`, W2); every wired
   id present satisfies the slot key (`DependencyContractError`). Non-materializable nodes are
   exempt.
3. **Partitioning.** `_check_partition_dependencies` keeps the existing rule and adds: two
   time-partitioned ends of an edge must share `granularity`, or `DAGError`.

`Source._resolve_upstreams` reads `sibling_upstreams()` and wires siblings only; wildcard and
foreign keys are left for the DAG. The platform path is unaffected: hydrated ids are already
present, so resolution finds nothing to do and the contract check is the only new gate.

Specs carry lists under the renamed field: `upstreams: {campaigns: [id1, id2]}`, or
`upstreams: {orders: [shop-orders]}` for a single slot (`orders: shop-orders` is still accepted).

### 2.4 Execution (interloper-core, interloper-scheduler)

- `_build_kwargs` reads every slot through `_read_upstreams`, which reads each wired leg with
  `_destination_read` and collects `Upstream(asset, data)`. A read that fails because the
  upstream has **no data for the partition** (`DataNotFoundError` as the cause) yields the leg
  with `data=None` and a `LOG` warning event naming it; any other read error fails the asset. A
  many slot receives the legs; a single slot receives the first leg's data, or `None` for an
  optional slot with no leg.
- `_destination_read` and `partition_row_counts` pick the destination whose key equals the
  upstream's `default_destination_key`, falling back to the first (E3).
- `RunExecutor._resolve_upstream` walks `upstreams.values()` lists so every leg is joined.
- Partition scope is unchanged: one partition per run, legs read at the same partition. With
  equal granularity enforced, the matcher and its legs are all daily.

### 2.5 Platform (interloper-db, interloper-api, interloper-toolkit, interloper-agent)

- **Migration 017** (ships with the core phase, because the store writes and hydrates the new
  relation type): `UPDATE component_relations SET type = 'upstream' WHERE type = 'dependency'`,
  and `uq_component_relations_slot` rebuilt to cover `resource` only. Single-valuedness of
  non-many upstream slots is enforced by the store, where repoint semantics already live.
- **Store.** `RelationStore.add` repoints only when the slot is not many; a many slot accumulates
  edges. `_check_slot_target` accepts any parent (or none) for a wildcard key and keeps the
  sibling and named-source rules otherwise. `remove` blocks a non-optional `on_unbind="block"` slot
  only when the removal leaves it with no binding. `_sync_relations` already accepts several
  bindings per slot once the index allows it. `_wire_intra_upstreams` uses `Asset.sibling_upstreams`.
- **Hydration.** `_build_init` groups the rows of a slotted relation per slot and emits a list per
  slot for the `upstream` relation (`inline=False`); resource slots keep one nested spec each.
- **API.** No route changes. `POST /components/{id}/relations` adds a leg, `DELETE .../{type}/{dst_id}`
  removes one, `PUT` replaces the full set. Clients pass `type=upstream`.
- **Toolkit and agent.** `get_asset_schema` reports `depends_on` (parameter to `Dependency` fields)
  read from `relations.upstream.slots` (D4), replacing the always-empty `requires` output. Lineage
  tools read `type="upstream"`. The agent gains `bind_upstream` (asset id, upstream asset id,
  slot) and its unresolved-requirements report reads the slots.

### 2.6 UI (interloper-app)

- **Types.** The `Dependency` interface (`key`, `optional`, `many`, renamed from `RelationSlot`);
  `upstreamMap(record)` returning `slot -> upstream ids`; a shared
  `slotAccepts(slotKey, upstreamQk, ownSourceKey)` helper mirroring `AssetIdentity.satisfies`.
  Every `'dependency'` relation literal becomes `'upstream'`.
- **Wizard assets step.** Every upstream slot with a non-sibling key gets a picker (qualified or
  wildcard, optional or not). Candidates are all assets in the organisation whose key satisfies
  the slot, labelled with the source instance name. Single slots keep the select menu; many slots
  get a multi-select that starts with every match selected. Edit mode seeds from the persisted
  relations. Relation writes after save surface errors instead of swallowing them.
- **Asset panel.** The upstream section lists **declared slots with their persisted bindings**,
  using the source instance label from `useAssetDisplayName`. A many slot shows "n bound" and a
  "m matching, not bound" line with a bind-all action. Unbound non-optional slots are marked.
- **Graph.** Drag-connect matches slots with `slotAccepts`, so bare and wildcard keys connect. A
  many slot accepts further legs. The target handle renders whenever the asset declares any slot.
  Runnability is per non-optional slot, not a count.
- **Warnings.** Satisfaction is checked per slot from the relation's `slot` field. A many slot with
  matching-but-unbound assets yields an attention warning (the nudge that replaces auto-binding).
- **Collection.** The upstreams cell gets a tooltip listing "Source instance, Asset" per leg.
  The panel resolves definitions by qualified key.
- **Jobs.** No change; a job targets the matcher's source.

### 2.7 The matcher (interloper-assets)

`interloper_assets/campaign_matcher/source.py` replaces the stub:

```py
@il.source(tags=["Analytics"], icon="carbon:connection-signal")
class CampaignMatcher(il.Source):
    """Groups campaigns from every configured ad source under canonical names."""

    @il.asset(
        schema=schemas.CampaignMatches,
        partitioning=il.TimePartitionConfig(column="date"),
        depends_on={"campaigns": il.Dependency(key="*.campaigns", many=True)},
        tags=["Entity"],
    )
    def campaign_matches(self, context: il.ExecutionContext, campaigns: list[il.Upstream]) -> list[dict[str, Any]]:
        """One row per campaign per day with its canonical name. Placeholder matching."""
```

Schema `CampaignMatches`: `date`, `source_key`, `source_id`, `campaign_id`, `campaign_name`,
`canonical_name`, `match_method`. v1 emits one placeholder row per leg that has data
(`match_method = "placeholder"`) so the fan-in is exercised end to end while the matching logic is
designed separately. Daily partitioning, REPLACE per partition; each day is a version of the
lookup table and "current" is the latest date downstream.

Answers to the remaining brief questions: rules and manual overrides are not modelled in v1 (they
belong to the matching design); a provider that has no data for the partition arrives as a leg
with `data=None` (the framework emits a warning event naming it) and the matcher skips it, so the
row set for that day simply lacks it; the matcher runs from its own cron job offset after the
provider jobs.

### 2.8 Out of scope, recorded as follow-ups

Resources as `depends_on` entries on `Component` (one declaration for everything a component
consumes; compatible with every name chosen here), auto-binding of new matching assets, tag
predicates, coarser-granularity roll-ups, `OPERATION_SKIPPED` events and `skipped_ids`, per-job
`select`, `fail_fast` cancelling unrelated branches, read projection, the config back door (W5),
matching rules and overrides.

### 2.9 Experiment: trigger-hook chaining

Question: can a `TriggerHook` watching the provider jobs and targeting the matcher job replace the
cron offset? Known from the code: the hook fires once per finished run and propagates the
partition key, so with three provider jobs the matcher runs three times per day, each time reading
whatever legs exist; the last run has every leg. On the dev instance: create two ad sources with
placeholder credentials, the matcher, a job per provider and a job for the matcher, a
`TriggerHook` with `watches=[provider jobs]`, `targets=[matcher job]`, `events=["run_completed"]`;
trigger the provider jobs for one partition; record how many matcher runs appear, their partition
keys and their missing-leg warnings. Outcome goes into an appendix of this document and decides
whether a quorum or debounce on hooks is worth designing.

---

## Part 3. Phasing

Each phase is one branch and one PR, mergeable alone, Conventional Commits, rebase-only, commit
messages ending with "By Digitl". Detailed task lists live in `docs/superpowers/plans/`.

1. **Core** (`feat/many-upstreams-core`, a `feat!:` PR): the `upstream` relation and `upstreams`
   field (with migration 017 and the mechanical rename across store, toolkit, agent, scheduler and
   app), `depends_on` and `il.Dependency` (renamed `RelationSlot`) replacing `requires` /
   `optional_requires`, `il.Upstream`, `declared_upstreams()` and `sibling_upstreams()`, DAG
   resolution and the single contract check, equal-granularity check, the single read path with
   `None` for missing legs, `default_destination_key` on read, docs and the plugin skills.
   Existing single-slot sources keep working; hand-written specs rename `dependencies` to
   `upstreams`.
2. **Platform** (`feat/many-upstreams-platform`): store add/remove/target rules for many slots and
   the wildcard, hydration lists, executor flattening, toolkit `depends_on`, agent `bind_upstream`.
3. **Matcher skeleton** (`feat/campaign-matcher-skeleton`): `CampaignMatcher` source with the
   placeholder asset, schema, tests and registration. Placed before the UI phase because the UI
   work needs a real many-slot asset in the catalog to verify against.
4. **UI** (`feat/many-upstreams-app`): wizard picker, panel bindings, graph rules, warnings nudge,
   collection tooltip. Verified headless on a dev instance with the matcher.

The trigger-hook experiment runs after phase 3 on the dev instance and only writes to this
document.
