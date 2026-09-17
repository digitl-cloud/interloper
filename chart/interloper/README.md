# Interloper Helm chart

Deploys Interloper (scheduler + API + frontend) onto Kubernetes.

## Components

| Component | Purpose |
|-----------|---------|
| **scheduler** | Singleton: runs cron, the queue worker, and the reaper in one process. Dispatches runs via the configured launcher. Required. |
| **api** | FastAPI backend serving `/api/*`. |
| **frontend** | nginx serving the pre-built Nuxt SPA. |
| **mcp** | Read-only MCP server (streamable HTTP, PAT bearer auth) for external AI agents. Optional, off by default (`mcp.enabled`). |

The first three are deployed as separate Deployments by default.  Each can be
disabled via `<component>.enabled: false`.

## Images

Each component is its own image, named `interloper-<component>`, published to
GitHub Container Registry, which is the chart's default `image.registry`
(`ghcr.io/digitl-cloud`). Every image comes in two variants: the **loaded**
one on the bare tag, and the extras-free **slim** one on `-slim`.

```
ghcr.io/digitl-cloud/interloper-scheduler:<version>   # every launcher + sources + destinations
ghcr.io/digitl-cloud/interloper-api:<version>         # + the ADK agent
ghcr.io/digitl-cloud/interloper-frontend:<version>
ghcr.io/digitl-cloud/interloper-core:<version>        # the framework; kubernetes runner per-asset Job target
ghcr.io/digitl-cloud/interloper-mcp:<version>         # read-only MCP server
```

The chart deploys the loaded images, so `launcher.type` and `agent.enabled`
are pure runtime settings: no tag mapping, nothing to rebuild when they
change. The `-slim` images carry core plus the component's own packages and
nothing optional; they are a base to build on rather than a smaller
deployment, so a chart pointed at one needs its own image adding back
whatever the catalog names. Set `<component>.image.repository` (or `.tag`) to
use one, and override `image.registry` (plus `image.pullSecrets` for a
private registry) to pull from elsewhere.

## Quick start (dev)

```bash
helm dependency update chart/interloper
helm install interloper chart/interloper \
  --namespace interloper --create-namespace \
  --set postgresql.enabled=true \
  --set secrets.encryptionKey="$(openssl rand -base64 32)"
```

This bundles Postgres via the Bitnami subchart — convenient for local
testing, but **not production-ready**.  Use an external managed Postgres
for anything serious (see below).

## Production install

```bash
helm install interloper chart/interloper \
  --namespace interloper --create-namespace \
  -f values.prod.yaml
```

With a `values.prod.yaml` like:

```yaml
image:
  registry: registry.example.com
  tag: "0.2.0"
  pullSecrets:
    - name: registry-creds

postgresql:
  enabled: false

externalPostgres:
  host: postgres.prod.example.internal
  port: 5432
  user: interloper
  database: interloper

secrets:
  postgresPassword: "{{ .from.vault }}"
  encryptionKey: "{{ .from.vault }}"

ingress:
  enabled: true
  className: nginx
  host: interloper.example.com
  tls:
    enabled: true
    secretName: interloper-tls

launcher:
  type: kubernetes
  # image + namespace + service_account_name are auto-filled from the release

runner:
  type: async

catalog:
  - interloper_assets.demo.source.DemoSource
  - interloper_google_cloud.BigQueryDestination
```

## Configuration

### App settings — interloper.yaml

App runtime settings are root-level values, one block per feature:
`launcher`, `runner`, `catalog`, `smtp`, `auth`, and `agent`.  The chart
renders them into a ConfigMap mounted at
`/etc/interloper/interloper.yaml`, auto-filling Kubernetes launcher and
runner defaults (`image`, `namespace`, `service_account_name`) from the
release context, so you rarely need to set them manually.

Secret-bearing fields never go through the ConfigMap — they are injected
as env vars from the chart's Secret (`secrets.*`), pairing with their
feature block: `auth.google_client_id` + `secrets.googleClientSecret`,
`smtp.user` + `secrets.smtpPassword`, `externalPostgres` +
`secrets.postgresPassword`.

Anything else interloper.yaml accepts (server, cron, worker, reaper, mcp
tuning) goes under `extraConfig`, which is rendered verbatim into the
generated file.  Chart-managed and secret-bearing sections are rejected
there — a YAML-provided field would override the injected env vars.

### `secrets.*`

Either inline values (dev) or reference a pre-existing Secret:

```yaml
secrets:
  existingSecret: my-interloper-secret
```

Expected keys: `INTERLOPER_POSTGRES_PASSWORD`, `INTERLOPER_ENCRYPTION_KEY`
(recommended), `INTERLOPER_SMTP_PASSWORD` (optional).

### Ingress vs Gateway API

Either `ingress.enabled: true` or `httpRoute.enabled: true`.  The
HTTPRoute uses `gateway.networking.k8s.io/v1` and requires the Gateway
API CRDs installed in your cluster.

### Streaming endpoints and proxy timeouts

The API serves long-lived SSE streams (agent chat), so any proxy in
front of it with a *total response* timeout must allow the longest
expected turn.  The chart stays cloud-agnostic — configure the timeout
where your ingress implementation expects it:

- **ingress-nginx** — via `ingress.annotations`:
  `nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"`.
- **GKE Gateway API** (`httpRoute`) — deploy a `GCPBackendPolicy`
  alongside the release, with `spec.default.timeoutSec` and a
  `targetRef` at the chart's API Service
  (`<release>-interloper-api`).  GKE's default is 30 s, which cuts
  streams mid-response.
- **GKE Ingress** — deploy a `BackendConfig` with `spec.timeoutSec`
  and bind it via `api.service.annotations`:
  `cloud.google.com/backend-config: '{"default": "<name>"}'`.

### Zero-downtime rollouts

Cloud L7 load balancers route to pod IPs directly (GKE NEGs, AWS ALB
target groups) and deprogram a removed endpoint *asynchronously*, with
no signal back into the cluster.  A pod that exits as soon as it is
deleted therefore keeps receiving traffic at an address nothing is
listening on.  The api/frontend/mcp defaults cover the part that costs
nothing to get right:

| Value | Default | Purpose |
|-------|---------|---------|
| `strategy.rollingUpdate.maxUnavailable` | `0` | Set explicitly: the Kubernetes `25%` default only *floors* to 0 below five replicas. |
| `podDisruptionBudget` | `maxUnavailable: 1` | Node upgrades and drains evict pods without consulting the rollout strategy. |

Two things are left to the deployment, because both depend on where the
chart runs.

**The drain.** How long a pod must outlive its own deletion is a property
of the ingress implementation, so `lifecycle` ships empty and
`terminationGracePeriodSeconds` keeps the Kubernetes default:

- **ingress-nginx**, and in-cluster proxies generally — endpoint changes
  converge in about a second, so a `preStop` of `sleep 10` is already
  generous.
- **GKE Gateway API / Ingress** — follow [Addressing 500 series errors
  with NEGs during workload scaling][gke-neg-500], which prescribes a
  60 s BackendService drain timeout (via `GCPBackendPolicy` for Gateway,
  `BackendConfig` for Ingress), `terminationGracePeriodSeconds: 210`,
  and a `preStop` of `sleep 120` on every container.  Note that the NEG
  readiness gate does not cover this: it gates a pod becoming *ready*,
  never its termination.
- **AWS ALB** — align with the target group's
  `deregistration_delay.timeout_seconds` (default `300`).

**Replica count**, which defaults to `1` so a small install stays small.
A drained rollout is already clean at one replica: the replacement is
surged and ready before the outgoing pod is deleted, and the drain covers
the deprogramming window.  What one replica has nothing to fall back on is
*involuntary* loss — node failure, preemption, OOM kill — where no drain
runs and the backend empties outright.  Behind a cloud load balancer, run
at least two.

```yaml
api:
  replicaCount: 2
  terminationGracePeriodSeconds: 210
  lifecycle:
    preStop:
      exec:
        command: ["/bin/sh", "-c", "sleep 120"]
```

If rollouts still drop requests, the load balancer's own logs separate
the two causes: a failure to *connect* to a backend means the drain is
too short, while a failure to *select* one means the backend was emptied,
which points at `replicaCount` or `maxUnavailable` instead.

[gke-neg-500]: https://docs.cloud.google.com/kubernetes-engine/docs/troubleshooting/load-balancing#500-series-errors

### RBAC (Kubernetes launcher)

`rbac.create: true` (the default) creates a ServiceAccount + Role +
RoleBinding that let the scheduler manage Jobs and read pod logs in
the release namespace.  The launcher config auto-wires the
ServiceAccount name.  Set `rbac.create: false` if you manage RBAC
externally — provide the ServiceAccount name via `serviceAccount.name`.

## Upgrades

```bash
helm upgrade interloper chart/interloper -f values.prod.yaml
```

DB schema migrations run automatically on scheduler startup.

## Uninstall

```bash
helm uninstall interloper --namespace interloper
```

If Postgres was bundled, its PVC is retained by default — delete it
manually to free storage.
