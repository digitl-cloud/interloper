# Interloper Helm chart

Deploys Interloper (scheduler + API + frontend) onto Kubernetes.

## Components

| Component | Purpose |
|-----------|---------|
| **scheduler** | Runs cron, the queue worker (dispatches runs via the configured launcher), and the reaper.  Required. |
| **api** | FastAPI backend serving `/api/*`. |
| **app** | Nuxt SSR server (Node.js). |

All three are deployed as separate Deployments by default.  Each can be
disabled via `<component>.enabled: false`.

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
  registry: registry.example.com/interloper
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

config:
  launcher:
    type: kubernetes
    # image + namespace + service_account_name are auto-filled from the release
  runner:
    type: multi_thread
  catalog:
    - interloper_assets.demo.source.DemoSource
    - interloper_google_cloud.BigQueryDestination
```

## Configuration

### `config.*` — interloper.yaml

Everything under `config` is rendered into a ConfigMap mounted at
`/etc/interloper/interloper.yaml`.  The chart auto-fills Kubernetes
launcher defaults (`image`, `namespace`, `service_account_name`) from
the release context, so you rarely need to set them manually.

### `secrets.*`

Either inline values (dev) or reference a pre-existing Secret:

```yaml
secrets:
  existingSecret: my-interloper-secret
```

Expected keys: `INTERLOPER_POSTGRES_PASSWORD`, `SECRETS_ENCRYPTION_KEY`
(recommended), `INTERLOPER_SMTP_PASSWORD` (optional).

### Ingress vs Gateway API

Either `ingress.enabled: true` or `httpRoute.enabled: true`.  The
HTTPRoute uses `gateway.networking.k8s.io/v1` and requires the Gateway
API CRDs installed in your cluster.

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
