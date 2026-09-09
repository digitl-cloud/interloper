# Relation model: one primitive for every link between components

Date: 2026-09-07. Status: approved design, implementation not started.

Supersedes Part 2 (design) and Part 3 (phasing) of `2026-09-04-downstream-assets-design.md` and the
four phase plans derived from it. Part 1 of that document (findings) stands. PR #321
(`feat/many-upstreams-core`, the first-phase implementation of the earlier design) is unmerged and is
reworked on top of this model before anything lands on `main`.

Scope: how components relate to one another in interloper, at the component level first and then
what each kind adds, with assets as the one kind that injects its relations into a `data()`
function. The north star is unchanged: a **campaign matcher** asset fanning in every `campaigns`
asset an organisation has configured, with the set of upstreams decided by wiring, not by the class.

What the model has to achieve, in Guillaume's words: define relationships between components;
constrain them at the definition level (kinds, keys, fields); wire them at the instance level; and
treat assets specially, because some of their relations must be passed to `data()`.

---

## 1. Decisions

| Topic | Decision |
|---|---|
| Primitive | One `Relation` class declares every link, on every kind. It replaces `relation_types`, `RelationDefinition`, `resource_types`, `ResourceRef`, `depends_on`, `Dependency` and `RelationSlot`. |
| Selection | Always by key: an exact key, `source.asset`, `*.asset`, or a list of keys. A class in a declaration is shorthand for its key. |
| Bare key | A bare asset key selects a sibling of the same source instance. |
| Wiring | Always an instance on the component. Ids exist only in specs and in rows. |
| Fallback | Fallbacks are resolved when a relation is read, never bound: an explicit `default=` first, otherwise the declared class when it is a `Resource` (settings may come from the environment, so it is constructed at read time and a validation error surfaces then, as today) or when all its fields are defaulted. An unbound non-optional relation with no fallback is a build error. |
| Optional | Means the wiring may be absent. It says nothing about data: a bound upstream whose partition holds nothing yields `data=None`, for every relation. |
| Removal | `on_delete` is declared per relation (`block` or `detach`). Unbinding is derived: refused when it would empty a non-optional relation, detach otherwise. `on_unbind` is dropped. |
| Declaration forms | Annotation with a Component class (`connection: Conn`), a typed `il.Relation` attribute (`destinations: list[Destination] = il.Relation("destination", many=True, optional=True)`, the annotation is for the type checker), a bare `il.Relation` attribute, or `@il.asset(relations={...})`. Explicit beats annotation; a subclass replaces its parent's declaration of the same name. `Relation` is its own descriptor (class access gives the `Relation`, instance access the bound value); a `TYPE_CHECKING`-only `__new__` returning `Any` makes the typed form check under ty. |
| Injection | `data()` receives the bound instance for resource relations and `il.Upstream` (single) or `list[il.Upstream]` (many) for asset relations. |
| Serialisation | A manifest is one component, nested, and is exactly `to_spec()` output. A target that has a parent is emitted under its parent and referenced as `{ref: id}` everywhere else. A target without a parent is inline at the first relation that reaches it and referenced afterwards. No flag, no kind knowledge. |
| Containment | Stays `parent_id` (assets under sources). `parent` moves onto `Component` so the serialisation rule is generic. To revisit once this lands (containment as an owned relation). |
| Rows | `component_relations(src_id, name, dst_id, ...)`, primary key `(src_id, name, dst_id)`. `type` and `slot` collapse into `name`. |
| DAG | Edges from bound asset relations. A bound upstream not among the DAG's workloads is included as a read-only node. |
| Migration | Revision 017 is rewritten in place (it never shipped); no 018. |
| Compatibility | None. All consumers ship together; the manifests repo updates in lockstep. |

---

## 2. The primitive

`interloper/component/relation.py`

```py
class ComponentIdentity(NamedTuple):
    source_key: str | None
    key: str

    @classmethod
    def of(cls, component: Component) -> ComponentIdentity: ...
    def satisfies(self, declared_key: str, *, own_source_key: str | None) -> bool: ...


class Relation(BaseModel):
    kind: str | list[str]
    key: str | list[str] = ""
    many: bool = False
    optional: bool = False
    default: Callable[[], Component] | None = None
    on_delete: Literal["block", "detach"] = "block"
    name: str = ""                                   # set by Component.collect()

    def accepts(self, kind: str, identity: ComponentIdentity, *, owner: ComponentIdentity) -> bool: ...
    @property
    def self_filling(self) -> bool: ...              # default set, or a single-valued class target with no required fields
    def fallback(self) -> Component | None: ...        # a fresh instance from default() or target(), or None


    def __get__(self, instance, owner) -> Component | list[Component] | Relation | None: ...   # Relation is its own descriptor
    def __set__(self, instance, value) -> None: ...                                        # atomic rebind
```

`Relation(FacebookAdsConnection)` is shorthand for `Relation(kind="connection", key="facebook_ads_connection")`.
`Relation("asset", "*.campaigns", many=True)` selects every asset with key `campaigns` in any source.

`ComponentIdentity` is the single place a declared key is compared to a concrete component. Its
`satisfies` handles the four key forms. For non-asset kinds `source_key` is `None` and only the exact
form applies. `accepts` takes identities rather than instances so the store can apply the same rule
to rows (section 9).

`Relation` replaces `ResourceRef` as the descriptor too. On the class it returns itself; on an instance it
returns the bound instance, the bound list, or `None` / `[]` when unbound.

---

## 3. Component operations

`interloper/component/base.py`

```py
class Component(Serializable):
    kind: ClassVar[str]
    relations: ClassVar[dict[str, Relation]]
    id: str
    parent: Component | None
    _bound: dict[str, Component | list[Component]]

    def __init__(self, /, **data): ...              # relation names accepted as kwargs

    @classmethod
    def collect(cls) -> None: ...                    # at class creation: annotations, Relation attributes, decorator
    def bind(self, name: str, *targets: Component) -> None: ...
    def unbind(self, name: str, *targets: Component) -> None: ...
    def bound(self, name: str) -> Component | list[Component] | None: ...
    def bound_ids(self) -> dict[str, list[str]]: ...
    def trickle(self, child: Component) -> None: ...
    def resolve(self, name: str) -> Component | list[Component] | None: ...   # bound(name), else relation.fallback()
    def validate_relations(self, nodes: Mapping[str, Component] | None = None) -> None: ...

    @property
    def identity(self) -> ComponentIdentity: ...
    @property
    def qualified_key(self) -> str: ...

    @classmethod
    def definition(cls) -> ComponentDefinition: ...
    def to_spec(self) -> Spec: ...
    @classmethod
    def from_spec(cls, spec: Spec, catalog=None, *, resolve: Callable[[str], Component] | None = None) -> Self: ...
```

- **collect** gathers relations from annotations whose type is a Component class, from `Relation`
  class attributes (typed or bare), and from the decorator's `relations=`. It installs the stamped
  `Relation` as the attribute and removes the name from pydantic's fields. Explicit wins over annotation; a subclass declaration replaces
  the parent's. No error on a double declaration, since it is always deliberate.
- **bind / unbind** are the only writers of `_bound`. `bind` checks `relation.accepts` for each
  target; `many` accumulates, single replaces. `unbind` refuses to empty a non-optional relation.
- **trickle(child)** fills a child's relation only when it is still unbound, has the same name and
  `accepts` the parent's target. An explicit child binding is never overridden. It re-runs on every
  `bind` of the parent, so destinations bound after the assets exist still propagate.
- **resolve(name)** is what injection reads: the bound value, else a fresh `relation.fallback()`.
  Fallbacks are never bound, so an explicit binding or a later `trickle` always wins over them, and
  `to_spec()` never carries an auto-instantiated component.
- **validate_relations(nodes)** fails on an unbound non-optional relation without a fallback, on more than one target
  for a single relation, and on a bound target whose identity does not satisfy the declared key.
  With `nodes` it also requires every bound asset target to be a live node.
- **identity / qualified_key**: `source.asset` for a contained asset, the bare key otherwise.
- **definition()** exports each relation as `kind`, `key`, `many`, `optional`, `on_delete` for the
  catalog.
- **to_spec / from_spec** follow the serialisation rule of section 8. `resolve` turns an id that is
  not in the document into an instance (the store passes `store.load`).

Retired from `Component`: `resource_types`, `relation_types`, `resources`, `trickle_resources`,
`relation_definitions()`, `RelationDefinition`, `Dependency`, `AssetIdentity`.

---

## 4. Kinds

Each anchor class keeps its kind, its entry in `KINDS`, and everything unrelated to relations. The
umbrella `resource` relation every kind declares today, together with `resource_types`, disappears:
each resource is its own named relation with one kind, declared by annotation on the subclass.

| Kind | Anchor declares | Subclasses typically add |
|---|---|---|
| Resource, Connection, Config | nothing | nothing (leaves of the graph) |
| Destination | nothing | `connection: GoogleCloudConnection` |
| Source | `destinations = Relation("destination", many=True, optional=True)` | `connection: ...`, `config: ...` |
| Asset | `destinations` as Source, filled by trickle | inferred from `data()` or written in `relations=` (section 5) |
| Job | `targets = Relation(["source", "asset"], many=True, optional=True, on_delete="detach")`, `destinations` | nothing (CronJob) |
| Hook | `watches = Relation(["source", "asset", "job"], many=True, optional=True, on_delete="detach")` | `connection: SlackConnection` |

`destinations` is optional at the relation level because a source or job may exist without one, for
example mid-wizard. Runnability is a different question: the runner refuses to materialise an asset
with no destination.

Source containment is unchanged: `asset_types` collects the asset classes, `__init__` builds the
instances and sets `parent`, and every `bind` on the source trickles into them.
`default_destination_key` stays a plain field that selects among the bound destinations.

`Connection` stays an `Operation` for `check()`. `Operation` keeps `materializable`, `partitioning`
and `source`, and gains `upstream_relations()` (the relations whose kind is `asset`), which the DAG
uses. It loses `upstreams`, `declared_upstreams` and `validate_upstreams`.

---

## 5. Assets

### 5.1 Inference, one pass, local to the class

`Asset.collect()` extends the generic collection with the `data()` signature. For every parameter
other than `self`, `context`, `source` and `**kwargs` that is not already declared:

| Annotation | Relation |
|---|---|
| a Component class `C` | `Relation(kind=C.kind, key=C.key)` |
| `il.Upstream` | `Relation("asset", key=<parameter name>)`, a bare key, so the sibling of the same source instance |
| `list[il.Upstream]` | the same with `many=True` |
| anything else | a definition error at class creation: nothing could ever fill it |

A `None` default makes the inferred relation optional. Cross-source upstreams and wildcards are
written in `relations=` or as a class attribute.

This retires `Source._infer_upstreams`: the source no longer inspects its assets' signatures, because
the parameter's type says what it is. The two inference passes that exist today (resources on the
asset, upstreams on the source) become one, on `Asset`.

### 5.2 Sibling wiring

`Source.sibling_bindings()` is a pure classmethod returning `{asset_key: {relation_name: sibling_key}}`
for every asset relation whose key resolves inside this source (bare, or qualified with the source's
own key), using `ComponentIdentity.satisfies`. `Source.__init__` applies it to the instances it
built; the store applies it to the rows it creates (section 9). One rule, two callers. Anything
that does not resolve to a sibling stays unbound until an explicit bind or the DAG resolves it.

### 5.3 Injection and the read path

`_build_kwargs` walks the signature: `context` and `source` are injected as today, an asset relation
gets `Upstream` or `list[Upstream]` from `_read_upstreams`, every other relation gets `resolve(name)`:
the bound instance, else the relation's fallback (an explicit `default`, else the declared class
instantiated without arguments, as `_resolve_resource` does today), else `None` when optional.

`_read_upstreams` is unchanged from PR #321: a leg whose destination has nothing for the scope
(`DataNotFoundError` cause) is handed over as `Upstream(asset, data=None)` with a warning event; a
bound id absent from the DAG is skipped with a warning; any other read error fails the asset. An
existing but empty scope is whatever the destination returns for an empty read, not `None`.

```py
class Upstream:            # interloper/asset/upstream.py, unchanged
    asset: Asset
    data: Any | None
```

### 5.4 Validation

Nothing asset-specific. `validate_relations(nodes)` covers unbound, cardinality and identity; the DAG
passes its operation map so bound asset targets must be live nodes.

### 5.5 Declaration forms

```py
@il.source
class FacebookAds(il.Source):
    connection: FacebookAdsConnection            # Relation(kind="connection", key="facebook_ads_connection")
    account_id: str = il.InputField(discriminator=True)

    @il.asset(schema=schemas.Campaigns, partitioning=il.TimePartitionConfig(column="date"), tags=["Entity"])
    def campaigns(
        self,
        context: il.ExecutionContext,
    ) -> list[dict]:
        return [{**row, "date": context.partition_date} for row in _get_campaigns(self.connection, self.account_id)]

    @il.asset(schema=schemas.CampaignsStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def campaigns_stats(
        self,
        context: il.ExecutionContext,
        campaigns: il.Upstream,                  # inferred: Relation("asset", "campaigns"), the sibling
    ) -> list[dict]:
        ids = [row["id"] for row in campaigns.data or []]
        return _get_stats(self.connection, self.account_id, ids, context.partition_date)

    @il.asset(schema=schemas.AdsStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def ads_stats(
        self,
        context: il.ExecutionContext,
        ads: il.Upstream | None = None,          # inferred as optional
    ) -> list[dict]: ...
```

The same source written explicitly, with the connection as a declared relation of each asset
(visible in its definition, overridable per asset, filled from the source by `trickle`):

```py
    @il.asset(
        schema=schemas.CampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
        relations={
            "connection": il.Relation(FacebookAdsConnection),
            "campaigns": il.Relation("asset", "facebook_ads.campaigns"),
        },
    )
    def campaigns_stats(
        self,
        context: il.ExecutionContext,
        connection: FacebookAdsConnection,
        campaigns: il.Upstream,
    ) -> list[dict]: ...
```

A cross-source consumer, the matcher, and a class-based asset with a self-filling config:

```py
@il.source
class Finance(il.Source):
    currency: str = il.InputField(default="EUR")

    @il.asset(
        partitioning=il.TimePartitionConfig(column="date"),
        relations={
            "orders": il.Relation("asset", "shop.orders"),
            "fx": il.Relation("asset", "rates.daily_fx", optional=True),
        },
    )
    def revenue(
        self,
        context: il.ExecutionContext,
        orders: il.Upstream,
        fx: il.Upstream | None,
    ) -> list[dict]: ...


@il.source(tags=["Analytics"])
class CampaignMatcher(il.Source):
    @il.asset(
        schema=schemas.CampaignMatches,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
        relations={"campaigns": il.Relation("asset", "*.campaigns", many=True)},
    )
    def campaign_matches(
        self,
        context: il.ExecutionContext,
        campaigns: list[il.Upstream],
    ) -> list[dict]:
        return [
            {"date": context.partition_date, "source_key": leg.asset.source.key, **match(row)}
            for leg in campaigns
            if leg.data is not None
            for row in leg.data
        ]


class MatcherConfig(il.Config):
    min_similarity: float = il.InputField(default=0.8)


class CampaignMatches(il.Asset):
    config = il.Relation(MatcherConfig)          # every field defaulted: instantiated at read time when unbound
    campaigns = il.Relation("asset", "*.campaigns", many=True)
    schema = schemas.CampaignMatches
    partitioning = il.TimePartitionConfig(column="date")

    def data(
        self,
        context: il.ExecutionContext,
        config: MatcherConfig,
        campaigns: list[il.Upstream],
    ) -> list[dict]: ...
```

---

## 6. DAG

`interloper/dag/base.py`

```py
class DAG:
    def _build_graph(self, items) -> None: ...            # edges from upstream_relations() x bound()
    def _include_read_only_upstreams(self) -> None: ...   # bound targets outside the workloads join as materializable=False
    def _resolve_declared(self) -> None: ...              # unbound asset relations bound from the DAG's nodes
    def _check_relations(self) -> None: ...               # validate_relations(operation_map) per live node
    def _check_circular_dependencies(self) -> None: ...
    def _check_partition_dependencies(self) -> None: ...
    def to_spec(self) -> DAGSpec: ...
    def mini_dag(self, operation_id: str) -> DAG: ...


class DAGSpec(BaseModel):
    items: list[Spec]                                     # one document per root workload
    def reconstruct(self, catalog=None) -> DAG: ...
```

- **Edges** come from every bound target of every asset relation. No declared-upstream lookup, no
  optional bookkeeping: the target is an instance and its relation knows whether it is optional.
- **Read-only upstreams.** A bound asset target that is not among the DAG's workloads is included
  as a non-materialisable node, since it is an instance with its own destinations and can be read
  without being run. A job that targets only the matcher runs the matcher and reads the campaign
  tables. Partition checks still apply along those edges. `mini_dag` reduces to "this operation,
  with its parents read-only", the same mechanism.
- **Declared resolution** keeps its role: for each live asset and each unbound asset relation with a
  key, the candidates are the DAG's other assets whose identity satisfies the key, a bare key
  restricted to the asset's own source instance. Many binds all, single binds one, several
  candidates for a single is a `DAGError`, none leaves it unbound for validation. It writes through
  `asset.bind()`, so explicit wiring is never overwritten. In practice this handles cross-source keys
  and wildcards, since siblings were bound at construction.
- **Validation**: `_check_relations` calls `validate_relations(operation_map)` on every live node.
  Cycle and partition checks are unchanged (equal granularity along every edge).
- **Serialisation**: `to_spec()` emits one document per root workload, multi-document YAML. A parent
  pulled in as a read-only upstream appears as its own document with the asset marked
  `materializable: false`. `from_spec_file` builds the DAG over the documents' roots.

Wiring in Python:

```py
gcp = GoogleCloudConnection(service_account_key=...)
bq = BigQueryDestination(connection=gcp, project="dwh")
fb = FacebookAds(account_id="act_1", connection=FacebookAdsConnection(...), destinations=[bq])
tt = TiktokAds(advertiser_id="9", connection=TiktokAdsConnection(...), destinations=[bq])
matcher = CampaignMatcher(destinations=[bq])

dag = il.DAG(fb, tt, matcher)            # binds matcher.campaign_matches.campaigns to [fb.campaigns, tt.campaigns]
fb.campaigns_stats.campaigns             # fb.campaigns, bound by the source at construction
matcher.campaign_matches.bound_ids()     # {"destinations": [bq.id], "campaigns": [fb.campaigns.id, tt.campaigns.id]}

job = CronJob(cron="0 6 * * *", targets=[matcher], destinations=[bq])
alert = SlackHook(connection=slack, channel="#data", watches=[job], events=["run_failed"])
```

---

## 7. Serialisation

A manifest is one component. Its top level identifies the class by `key` (catalog) or `path`
(import path), and its relations sit inside `init` under their names, nested. A YAML file may hold
several documents separated by `---`, each a component. There is no normalised or flat form: the
document is `to_spec()` output.

```py
class Spec(BaseModel):
    key: str | None = None
    path: str | None = None
    id: str | None = None                # required only when something references it
    init: dict[str, Any]                 # fields; a relation name holds a Spec, a {"ref": id}, or a list of either
```

**The one rule.** A component that has a `parent` is emitted under its parent and every relation
that points at it emits `{ref: id}`. A component without a parent is emitted inline at the first
relation that reaches it, and `{ref: id}` at any later one. Assets are referenced from upstreams
because they live under a source, not because they are assets. A destination shared by two sources
is inline once and referenced once. The rule lives in the generic `to_spec()` traversal keyed on
`parent`, and in the hydrator keyed on `parent_id`. No kind knows about serialisation and no flag
exists.

**Resolution.** A `{ref: id}` resolves within the document first, then through `resolve=` when a
store or catalog is given, and fails otherwise. In a hand-written manifest the referenced component
must be in the tree. In the platform, the store resolves it.

```yaml
path: interloper.job.cron.CronJob
init:
  cron: "0 7 * * *"
  destinations:
    - key: bigquery_destination
      id: bq
      init: {project: dwh, connection: {key: google_cloud_connection, init: {service_account_key: ${GCP_KEY}}}}
  targets:
    - key: facebook_ads
      init:
        account_id: act_1
        connection: {key: facebook_ads_connection, init: {access_token: ${FB_TOKEN}, app_id: "1"}}
        assets: {campaigns: {id: fb-campaigns, materializable: false}}
    - key: campaign_matcher
      init:
        destinations: [{ref: bq}]                # second occurrence of a parentless component: a reference
        assets:
          campaign_matches:
            campaigns: [{ref: fb-campaigns}]     # the asset has a parent: always a reference
```

Without the explicit `campaigns:` line the DAG binds every `*.campaigns` node it holds.

---

## 8. Persistence

### 8.1 Rows

```
component_relations(src_id, name, dst_id, org_id, src_kind, dst_kind)
  PRIMARY KEY (src_id, name, dst_id)
  FK (src_id, org_id, src_kind) and (dst_id, org_id, dst_kind) as today, ON DELETE CASCADE
  INDEX (org_id, name), INDEX (dst_id, name)
```

`type` and `slot` collapse into `name`, never empty. The partial unique index on resource slots goes:
whether a relation is single-valued is a class rule the schema cannot see, so `RelationStore`
enforces it under `SELECT ... FOR UPDATE` on the source row. `components.parent_id` and its check
constraint are untouched.

### 8.2 RelationStore

```py
class RelationStore:
    def list_all(self, org_id, *, name=None, src_kind=None, dst_kind=None) -> list[ComponentRelation]: ...
    def add(self, component_id, *, name, dst_id) -> ComponentRelation: ...   # accepts; many inserts, single repoints; row lock
    def remove(self, component_id, *, name, dst_id) -> None: ...             # refused if it would empty a non-optional relation
    def _sync_relations(self, session, src, bindings: dict[str, list[UUID]]) -> None: ...
```

`add` looks up `relations[name]` on the catalog class, builds the target's `ComponentIdentity` from
its row and parent row, and calls `Relation.accepts`. The slotted and unslotted branches, the
vocabulary check on `type`, `_check_slot_target` and `_wire_intra_upstreams` disappear; sibling
rows come from `Source.sibling_bindings()` in `_ensure_children`.

### 8.3 Hydrator

`_build_init` groups rows by `name`. Each target is its nested spec when its row has no `parent_id`
and `{ref: dst_id}` when it has one, the same structural rule as `to_spec()`. Single or list follows
`many`. Children still travel as the `assets` override map and `_load_owned_asset` still loads
through the parent source. `from_spec` receives `resolve=store.load`, so a reference to an asset
outside the document loads its source on demand. Within a document, references resolve to the
inline instance first, so a job that targets both the Facebook source and the matcher shares one
`campaigns` instance.

### 8.4 Delete

`_blocking_referrers` reads `on_delete` from the referrer's relation by name. Unchanged otherwise.

### 8.5 Migration 017, rewritten

Production is at 016; the 017 in PR #321 never shipped and is replaced:

- add `name`; fill it from `slot` where `type IN ('resource', 'dependency', 'upstream')` and from
  the plural field name for `destination` (`destinations`), `target` (`targets`), `watch` (`watches`);
- drop `type` and `slot`; rebuild the primary key as `(src_id, name, dst_id)`; drop
  `uq_component_relations_slot`; recreate the two secondary indexes on `name`.

Downgrade is the reverse and lossless: every old `type` is recoverable from `name` plus `dst_kind`.

---

## 9. Platform surfaces

**Catalog JSON.** `ComponentDefinition.relations` is `{name: {kind, key, many, optional, on_delete}}`.
Gone from the wire: `slotted`, `slots`, `field`, `inline`, `on_unbind`, the nested `Dependency`.

**API** (`interloper-api/routes/components.py`). `RelationCreateRequest(name, dst_id)`,
`RelationRef(dst_id, dst_kind)`, `RelationResponse(src_id, name, dst_id, dst_kind)`. A component's
embedded `relations` is keyed by name. Delete route `/{id}/relations/{name}/{dst_id}`. The org-wide
list gains `name`, `src_kind` and `dst_kind` filters. Fetch-field providers keep `"<name>.<method>"`.

**App** (`interloper-app`). `resourceSlots(defn)` becomes the relations whose kind is `connection`,
`config` or `resource`; `upstreamSlots(defn)` the relations whose kind is `asset`. Types drop `slot`
and `type` for `name`. The components store reads `defn.relations[name].optional` directly; the graph
composable pairs assets by relation name; dedup keys drop the slot segment. Wizard, `AssetSelect` and
the destinations page change only in which helper they call.

**Toolkit** (`interloper-toolkit/lineage.py`). Lineage tools filter rows on `src_kind` and `dst_kind`
both `asset` and report `name` as the parameter. One generic `bind_relation(component_id, name, dst_id)`
tool replaces the planned `bind_upstream`.

**Agent** (`interloper-agent/tools/collection.py`). `_source_relations` finds the relation whose kind
is `connection`, binds by name, and passes `{connection: [...], destinations: [...]}` and
`{targets: [...]}` to the store.

**Scheduler** (`interloper-scheduler/hooks.py`). The hook matcher filters on `name == "watches"`.

**Manifests repo.** YAML `dependencies:` keys become relation names; lockstep with the release.

---

## 10. Retired names

`relation_types`, `RelationDefinition`, `resource_types`, `ResourceRef`, `resources`,
`trickle_resources`, `relation_definitions()`, `depends_on`, `Dependency`, `RelationSlot`,
`AssetIdentity`, `upstreams`, `declared_upstreams`, `sibling_upstreams`, `validate_upstreams`,
`_infer_resource_types`, `_infer_upstreams`, `_check_slot_target`, `_wire_intra_upstreams`,
`slotted`, `slots`, `field`, `inline`, `on_unbind`, `embedded`, `type` and `slot` on rows and on the
wire.

---

## 11. Phasing

Each phase is one PR that leaves `main` releasable. Plans are written per phase after this spec is
approved, replacing `2026-09-04-downstream-assets-phase-{1,2,3,4}-*.md`.

1. **Core** (`interloper-core`, `interloper-assets` connectors). Rework PR #321 on this model:
   `Relation`, `ComponentIdentity`, `Bound`, `Component` operations, per-kind anchors, asset
   inference and injection, DAG, serialisation rule, `il.Upstream`, decorator `relations=`, docs.
   Every connector in `interloper-assets` moves from `resource_types` to annotations. Existing
   `interloper-core` tests are the acceptance suite.
2. **Platform** (`interloper-db`, `interloper-api`, `interloper-toolkit`, `interloper-agent`,
   `interloper-scheduler`). Rows and migration 017, `RelationStore`, hydrator, API models, toolkit
   filters and `bind_relation`, agent `_source_relations`, hook matcher. Verified on a throwaway
   database, never the shared dev DB.
3. **Matcher** (`interloper-assets`). `CampaignMatcher` with fake matching logic, its schema, tags,
   a run through the platform reading two connectors' `campaigns`.
4. **App** (`interloper-app`). Types and helpers, wizard, `AssetSelect` for many-valued relations,
   graph pairing by name, destinations page.

---

## 12. Follow-ups, recorded

- Containment as an owned relation instead of `parent_id` (cost and benefit to be weighed after the
  model lands; today the only child kind is asset).
- A literal `kind:` marker at the top of a manifest, if wanted for readability; `key` and `path`
  already fix the class.
- `roots: [...]` inside one document, only if one document per root proves noisy in child-pod specs.
- Empty-partition semantics differ per destination (BigQuery returns an empty result, others may
  raise); a design follow-up from the earlier spec that still stands.
- Auto-binding a new source's `campaigns` into existing matchers in the platform (a store-side rule
  or a UI nudge), deferred as before.
