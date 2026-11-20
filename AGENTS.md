# interloper

Python monorepo (uv workspace under `packages/`) plus a bundled Nuxt SPA. Provides a data-asset framework (`interloper-core`) and the runners, IO backends, API, scheduler, agent, and web UI built on top of it.

## Layout

```
packages/
  platform/                    the framework and the roles built on it
    interloper-core/           the framework: assets, sources, DAG, runners, IO, partitioning, the `run` command
    interloper-db/             database persistence layer, platform errors, `interloper db`
    interloper-scheduler/      cron + queue worker + reaper + launchers; `interloper scheduler`
    interloper-api/            FastAPI HTTP backend, `interloper api`; reads catalog metadata only, never executes assets
    interloper-app/            Nuxt SPA (app/) + Python package serving it; `interloper app` (all roles in one process); sdk/ is @interloper/ui, what extension UIs build with
    interloper-toolkit/        tool functions shared by the agent and the MCP server
    interloper-agent/          the assistant (pydantic-ai)
    interloper-mcp/            MCP server over the catalog, lineage and run history
  extensions/                  reached only through their manifests; each installs on its own
    interloper-assets/         pre-built source definitions (bing, facebook, google, …)
    interloper-pandas/         pandas DataFrame representation and normalizer
    interloper-google-cloud/   Google Cloud connection; BigQuery, GCS and Sheets destinations
    interloper-aws/            AWS connection and S3 destination
    interloper-azure/          Azure connection and Fabric Warehouse destination
    interloper-duckdb/         DuckDB / MotherDuck destination
    interloper-clickhouse/     ClickHouse destination
    interloper-databricks/     Databricks Delta destination
    interloper-snowflake/      Snowflake destination
    interloper-sql/            SQLAlchemy destination
    interloper-slack/          Slack connection and notification hook
    interloper-docker/         Docker runner + launcher
    interloper-k8s/            Kubernetes runner + launcher
examples/                      runnable usage examples; extension/ is an extension with routes and a UI, never published
chart/                         Helm chart
docker/                        uv-sync.sh helper and nginx template
dockerfile                     multi-target build (api / frontend / core / scheduler / mcp; all but frontend also as -slim)
docs/                          documentation site (zensical), published to GitHub Pages at docs.interloper.dev
dev/docs_macros.py             macros the reference pages render settings and CLI tables with
```

A package lives in `platform/` when a role process imports it by name, and in `extensions/` when the platform reaches it only through a registry reading its `extension.yaml` (see [docs/extending/extensions.md](docs/extending/extensions.md)). The workspace glob is `packages/*/*`, so a new package goes in one of the two groups.

The frontend lives at `packages/platform/interloper-app/app/` and has its own toolchain (pnpm + Nuxt) and its own [AGENTS.md](packages/platform/interloper-app/app/AGENTS.md). `make build-app` builds the SPA and copies it into `packages/platform/interloper-app/src/interloper_app/static/`. One pnpm workspace, rooted at the repository (`pnpm-workspace.yaml`, one `pnpm-lock.yaml`), holds the app, the SDK and the extensions' UIs (`packages/extensions/*/ui`, `examples/*/ui`); `pnpm install` anywhere in it installs them all. Building an extension's UI is in [docs/extending/extensions.md](docs/extending/extensions.md#building-it-with-the-sdk).

## Commands

Python (uv workspace, run from repo root):

- Lint: `uv run ruff check`
- Type check: `uv run ty check`
- Test: `uv run pytest` (marker: `functional`, excluded by default); the whole suite runs in parallel with `-n auto --maxprocesses 4`, as `make check-python`, the pre-push hook and CI do

Frontend (run from `packages/platform/interloper-app/app/`):

- Lint: `pnpm run lint`
- Type check: `pnpm exec nuxt typecheck`

Combined (from repo root):

- `make check` — both Python and frontend checks
- `make check-python` / `make check-typescript` — individual halves
- `make build-app` — build the SPA and stage it inside the Python package
- `make build-extensions`: build the SDK and every extension UI into its Python package's `static/`
- `make check-sdk-package` / `make check-extension`: the SDK as npm installs it, and an extension created, built and checked with `interloper extension` in a fresh project against the packed SDK (both need the network; CI runs both)
- `make setup` — `pre-commit install` + `uv sync --all-packages --all-extras`

Extensions in their own repositories (`uv init --lib`, then `uv add --dev interloper-app`):

- `uv run interloper extension init`: the bare extension (manifest, entry point, `interloper-core`, a gitignored `interloper.yaml`)
- `uv run interloper extension add <source|connection|settings|command|routes|ui> [name]`: one part, its files and its manifest entry (`ui` needs Node)
- `uv run interloper extension build`: the UI with npm when there is one, then the sdist and wheel in `dist/`
- `uv run interloper extension check`: import every installed manifest's entries and name what fails or what nothing reads

## Local dev instance

Stand up a running, seeded instance for trying out / verifying features. Both paths migrate the DB to head and seed the same minimal dataset via [dev/seed.py](dev/seed.py): a super-admin profile, one organisation (`Dev Org`), and the `demo` source with its `a → b,c,d → e` asset DAG. The seed is idempotent — re-running never duplicates.

The dev super-admin is set via `INTERLOPER_DEV_USER_EMAIL` (default `admin@dev.local`), read from the `make` command line, a gitignored repo-root `.env`, or the shell env (compose reads `.env` natively). The seed promotes the profile holding that address, or provisions one that nothing signs in to yet. The first login whose address such a profile holds claims it, so your first login with that address, through any provider, lands super-admin in `Dev Org` with no duplicate and no mail server. Adding a second way to sign in to a profile later asks you to prove it's yours, by signing in the way the profile already does, or by following a link emailed to its address (which needs SMTP).

- **Host** (local Postgres + CLI; fast inner loop). Needs a Postgres reachable at the [interloper.yaml](interloper.yaml) `postgres` creds (`localhost:5432`, `postgres/postgres/interloper`).
  - `make dev` — full bootstrap: reset + seed + run (one command).
  - `make dev-reset` — drop/recreate + migrate + seed (no server).
  - `make dev-up` — run api + cron + worker + reaper plus the Nuxt dev server (hot reload) at `http://localhost:3000` (the API moves to a free port Nuxt proxies to) against the existing DB — non-destructive, keeps your data/session. Installs the app deps (`pnpm install`) on first use if missing.
- **docker-compose** (Postgres + api + scheduler + frontend in containers; closest to prod). Only Docker required.
  - `make compose-up` — build + start everything; the app is on `http://localhost:3000` (nginx serves the SPA, proxies `/api`), with the API also reachable directly on `:3001`.
  - `make compose-down` — stop and drop the volume. `make compose-seed` — run only the one-shot seed.

**Automated/agent sessions must run on a port other than 3000** so they don't collide with the developer's own `:3000` instance: `INTERLOPER_SERVER_PORT=3100 make dev-up` (any free non-3000 port — the API still auto-picks its own free port that Nuxt proxies to). OAuth's redirect URI is pinned to `:3000`, so a non-3000 instance can't run the login flow itself, but it doesn't need to: cookies are scoped by host (not port) and every instance shares the same DB + encryption key, so an instance on `:3100` reuses whatever session a `:3000` login already established. (If no session exists yet, log in once on `:3000` first.)

Logging in needs a sign-in provider, and none is configured by default (the login page then offers no way to sign in). Copy [.env.example](.env.example) to a gitignored repo-root `.env` and fill in `INTERLOPER_AUTH_GOOGLE_CLIENT_ID` / `INTERLOPER_AUTH_GOOGLE_CLIENT_SECRET` (from a Google Cloud OAuth web client whose authorised redirect URI is `http://localhost:3000/api/auth/google/callback`), and/or `INTERLOPER_AUTH_MICROSOFT_CLIENT_ID` / `INTERLOPER_AUTH_MICROSOFT_CLIENT_SECRET` (from a multi-tenant Microsoft Entra app with the redirect URI `http://localhost:3000/api/auth/microsoft/callback` and the optional ID token claims `email` and `xms_edov`); the Makefile forwards these (and the dev-user vars) into both the host and compose paths. `INTERLOPER_AUTH_COOKIE_SECURE` defaults to `false` so the session cookie sticks over local http.

Both run against [dev/interloper.yaml](dev/interloper.yaml), which carries **only** the catalog and the agent (enabled, with the model dev chats with; the agent is opt-in and names no model by default): the `INTERLOPER_*` vars in the Makefile's `DEV_ENV` / [dev/docker-compose.yml](dev/docker-compose.yml) select the in-process launcher and the right DB, and the environment beats the config file field by field anyway. The host targets `cd dev` so that file is the active `./interloper.yaml` rather than the repo-root [interloper.yaml](interloper.yaml); compose mounts it. The compose stack lives in `dev/` too (build context is the repo root, where the dockerfile is). The dev encryption key/DB password are throwaways — never reuse them.

## Docker images

Built from a single multi-target [dockerfile](dockerfile). The image catalog is defined in the [Makefile](Makefile): one image per role (`interloper-<role>`), each shipping two **variants**.

- `ROLES` (`api`, `frontend`, `core`, `scheduler`, `mcp`) → image `interloper-<role>:<version>`, the **loaded** variant: the role's packages, every component class a catalog can name (sources, destinations, the Slack hook) and what the role runs on (both launchers on the scheduler, the agent on the api, otel). The vendor SDKs go only to the roles that execute assets (`scheduler`, `core`), since the asset modules guard those imports.
- `SLIM_ROLES` (everything but `frontend`) also builds `interloper-<role>:<version>-slim`, the **slim** variant: `interloper-core` plus the role's own packages and nothing optional. It is a base to extend, not a smaller deployment. The recipe is in [docs/ui/running.md](docs/ui/running.md).
- Each target stem is also the dockerfile target: `make docker-build-<role>` or `make docker-build-<role>-slim`. A slim stage declares none of the build `ARG`s, which is what keeps everything optional out of it.
- Build everything: `make docker-build` (host arch) or `make docker-build-linux` (linux/amd64 for the registry).
- Push: `make docker-push`, or `make docker-build-push` to build linux + push in one step.

The dockerfile's `ARG` defaults are the single source of truth for what "loaded" means: neither the Makefile nor [publish.yaml](.github/workflows/publish.yaml) passes a build arg. Set `COMPONENT_PACKAGES`, `ASSETS_EXTRAS`, `API_EXTRAS` or `COMMON_EXTRAS` at `make` time to override one locally. `COMPONENT_PACKAGES` names packages; the scheduler stage names its two launcher packages itself.

## Conventions

- Conventional Commits (`feat:`, `fix:`, `chore:`, `refactor:`, …). Breaking changes use `!` — see the recent `refactor!:` commits.
- Branch names use the same type prefix with a slash: `feat/xxx`, `fix/xxx`, `chore/xxx`, …
- PR titles follow Conventional Commits (`feat: …`, `fix: …`); every commit that lands on `main` feeds `python-semantic-release`.
- Python ≥3.11, ruff line length 120, type-checked with `ty`.
- Test files mirror the package layout one-to-one: a test for `src/interloper/<pkg>/<module>.py` lives in `tests/<pkg>/test_<module>.py`. Don't add standalone `test_<feature>.py` files — fold tests for an existing module into that module's test file (e.g. tests for `asset/base.py` go in `tests/asset/test_base.py`, not a new `test_<feature>.py`).
- Pre-commit runs ruff + ty on every commit and pytest on every push ([.pre-commit-config.yaml](.pre-commit-config.yaml)); `pre-commit install` (part of `make setup`) installs both hooks.
- All workspace packages share `version = "0.2.0"`, bumped by `python-semantic-release` from commit history.
- Component modules import pandas, ibis and vendor SDKs inside the functions that use them, through the package (`import pandas as pd`, the same at every call site), and name their types under `TYPE_CHECKING`: the api and mcp import every class to describe it. Why, and the Python 3.14 deadlock it avoids, in [docs/extending/components.md](docs/extending/components.md#definitions-in-the-catalog); `test_app.py` in interloper-api fails when one leaks back.
- Comment sparingly. A comment earns its place only when it's genuinely useful and targeted: explain a non-obvious *why* for the exact code it sits on. Don't restate what the code already says, and keep comments **scoped** — don't explain other parts of the architecture that this code doesn't own (e.g. don't describe the cloud-sql-proxy / Cloud SQL IAM setup in a chart env helper). That context belongs where that component lives, and drifts stale here.

### Asset naming

Assets in `interloper-assets` are named for **what the asset is**, decided by its actual row grain (not the upstream/vendor report name). Four categories:

- **Entity** — one row per object (a dimension snapshot, e.g. an ad or campaign and its attributes). Name is a **bare plural noun**, `tags=["Entity"]`, snapshot-partitioned on a stamped `date` column (`partitioning=il.SnapshotPartitionConfig(column="date")`, rows stamped with `context.partition_date`, schema carries `date: dt.date | None`). An entity reads the vendor's current state, so its partition is the day the run observed in, never the run's window: a backfill or retry of a past day writes today. Examples: `ads`, `campaigns`, `advertisers`, `custom_audiences`.
- **Report** — metrics aggregated over a date/dimension grain. Name is **`<base>_stats`**, with any `_by_<dim>` breakdown *after* `stats`; `tags=["Report"]`, time-partitioned, or snapshot-partitioned when it reads lifetime totals ([assets.md](.claude/rules/assets.md) rule 2). Examples: `ads_stats`, `ads_stats_by_country`, `page_stats`, `performance_stats`, `followers_stats` (snapshot).
- **Event / fact** — one row per event/record (per-row identifiers like `order_id`/`event_date`, no aggregation). Name is a **bare plural noun**, `tags=["Report"]`, time-partitioned. Examples: `orders`, `transactions`, `conversions`, `clicks`, `actions`.
- **Star schema**: views over the categories above, tagged `Fact` (`fact_<process>`, e.g. `fact_ad_performance`) or `Dimension` (`dim_<noun>`, e.g. `dim_campaigns`), unpartitioned. The views every platform publishes, the names their columns share and how dimensions are built are the contract in [.claude/rules/star-schema.md](.claude/rules/star-schema.md).

How an asset is implemented — mirroring the vendor response, when a partition column may be
stamped, how metrics are typed — is in [.claude/rules/assets.md](.claude/rules/assets.md).

Schema classes follow the asset name: one class per file, the file named after the asset and the class its `PascalCase` (asset `ads_stats` → `class AdsStats` in `schemas/ads_stats.py`). The `demo` source is a test fixture and is exempt from these rules.

### Package layout

A top-level folder of `interloper_assets` is a **container**, never a source: it holds what its sources share (`connection.py`, `api.py`, `constants.py`, and `normalizer.py` for a normalizer several of them use) and one **source folder** per source. A **vendor folder** holds what several containers of one vendor share and no source: `google/` (the service account grant), `linkedin/` (the REST API engine), `meta/` (the Graph API engine). A source folder is `source.py`, plus `normalizer.py` when the source defines a normalizer class and `schemas/` when it declares any schema; nothing else is ever named `source.py`. The platform's own fetching source is `connector/`; a derived source is named by its key with the container's prefix stripped (`facebook_ads/star_schema/` for `facebook_ads_star_schema`, `google_analytics_4/export/` for `google_analytics_4_export`); cross-platform sources share the `campaign/` container (`campaign/matcher/`, `campaign/performance_analysis/`). Tests mirror the tree one to one (`tests/facebook_ads/star_schema/test_source.py`, `tests/facebook_ads/connector/test_normalizer.py`). Catalog keys come from class names, so a move never changes a key.

### Git flow

`main` is kept strictly linear — no merge commits. Feature branches rebase onto `main`; merges into `main` are rebase-merges.

1. Branch from `main`: `git checkout -b feat/xxx`.
2. Each commit on the branch is itself a valid Conventional Commit — it may land on `main` as-is.
3. **Squash as you go.** Keep the branch to its minimal set of logical commits — don't accumulate WIP/fixup commits. Amend the existing commit (`git commit --amend`) or squash into it (`git rebase -i`) as progress is made, so the branch is always in a clean, mergeable state.
4. Keep up to date with `main` by rebase, never merge:
   ```
   git fetch origin
   git rebase origin/main
   ```
5. After a rebase or squash, push with `--force-with-lease` — never plain `--force`.
6. Merge the PR with **rebase-and-merge** (or squash, when the branch is one logical change). Never create a merge commit.
7. Resolve conflicts during rebase rather than discarding work or abandoning the branch.

### Worktrees

Worktrees live under `.claude/worktrees/`. Keep the worktree **directory name matching its branch** (i.e. the feature), so `git worktree list` reads at a glance — never leave a worktree on a random generated name.

1. **Decide the conventional branch name first** (`feat/xxx`, `fix/xxx`, …) from the task, then create the worktree with that exact name. With Claude Code's worktree tooling, pass it as the worktree `name` so the directory is `.claude/worktrees/<type>/<slug>` from the start; otherwise `git worktree add .claude/worktrees/<type>/<slug> -b <type>/<slug>`.
2. The built-in tool creates the branch as `claude/<name>`. Rename it to drop the prefix so it follows the convention: `git branch -m claude/<type>/<slug> <type>/<slug>`. The directory then matches the branch.
3. **Never rename or `git worktree move` the worktree you're currently working in** — moving the active directory breaks the session's working tree. Set the name at creation time instead.
4. To realign a stale worktree from an earlier session, do it while that worktree is *not* in use: `git worktree move .claude/worktrees/<old> .claude/worktrees/<type>/<slug>` (rename the branch separately with `git branch -m`).
