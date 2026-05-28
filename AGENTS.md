# interloper

Python monorepo (uv workspace under `packages/`) plus a bundled Nuxt SPA. Provides a data-asset framework (`interloper-core`) and the runners, IO backends, API, scheduler, agent, and web UI built on top of it.

## Layout

```
packages/
  interloper-core/           framework: assets, sources, DAG, runners, IO, partitioning
  interloper-assets/         pre-built source definitions (bing, facebook, google, …)
  interloper-pandas/         pandas DataFrame normalizer/adapter
  interloper-db/             database persistence layer
  interloper-google-cloud/   BigQuery destination
  interloper-docker/         Docker runner + backfiller
  interloper-k8s/            Kubernetes runner + backfiller
  interloper-scheduler/      cron + queue worker + reaper (singleton process)
  interloper-api/            FastAPI HTTP backend — reads catalog metadata only, never executes assets
  interloper-agent/          AI agent (Google ADK)
  interloper-app/            Nuxt SPA + Python package that serves it as static assets
examples/                    runnable usage examples
chart/                       Helm chart
docker/                      uv-sync.sh helper and nginx template
dockerfile                   multi-target build (core / scheduler / worker / api / frontend)
```

The frontend lives at `packages/interloper-app/app/` and has its own toolchain (pnpm + Nuxt) and its own [AGENTS.md](packages/interloper-app/app/AGENTS.md). `make build-app` builds the SPA and copies it into `packages/interloper-app/src/interloper_app/static/`.

## Commands

Python (uv workspace, run from repo root):

- Lint: `uv run ruff check`
- Type check: `uv run pyright`
- Test: `uv run pytest` (markers: `integration`, `functional` — `functional` is excluded by default)

Frontend (run from `packages/interloper-app/app/`):

- Lint: `pnpm run lint`
- Type check: `pnpm exec nuxt typecheck`

Combined (from repo root):

- `make check` — both Python and frontend checks
- `make check-python` / `make check-typescript` — individual halves
- `make build-app` — build the SPA and stage it inside the Python package
- `make setup` — `pre-commit install` + `uv sync --all-packages --all-extras`

## Docker images

Built from a single multi-target [dockerfile](dockerfile). The image catalog is defined in the [Makefile](Makefile):

- `ROLES` (`api`, `frontend`, `worker`) → image `interloper-<role>:<version>`.
- `ROLES_LAUNCHER_AWARE` (`scheduler`) builds one image per launcher: `interloper-scheduler`, `interloper-scheduler-k8s`, `interloper-scheduler-docker`. The launcher lives in the **image name**, not the tag; the Helm chart picks the suffix from `config.launcher.type`.
- Build one target: `make docker-build-<role>` or `make docker-build-<role>-<launcher>`.
- Build everything: `make docker-build` (host arch) or `make docker-build-linux` (linux/amd64 for the registry).
- Push: `make docker-push`, or `make docker-build-push` to build linux + push in one step.

Override extras at `make` time: `CORE_EXTRAS`, `ASSETS_EXTRAS`, `SCHEDULER_EXTRAS`.

## Conventions

- Conventional Commits (`feat:`, `fix:`, `chore:`, `refactor:`, …). Breaking changes use `!` — see the recent `refactor!:` commits.
- Branch names use the same type prefix with a slash: `feat/xxx`, `fix/xxx`, `chore/xxx`, …
- PR titles follow Conventional Commits (`feat: …`, `fix: …`); every commit that lands on `main` feeds `python-semantic-release`.
- Python ≥3.10, ruff line length 120, pyright `basic` mode.
- Pre-commit runs ruff + pyright + pytest on every commit ([.pre-commit-config.yaml](.pre-commit-config.yaml)).
- All workspace packages share `version = "0.2.0"`, bumped by `python-semantic-release` from commit history.

### Git flow

`main` is kept strictly linear — no merge commits. Feature branches rebase onto `main`; merges into `main` are rebase-merges.

1. Branch from `main`: `git checkout -b feat/xxx`.
2. Each commit on the branch is itself a valid Conventional Commit — it may land on `main` as-is.
3. Keep up to date with `main` by rebase, never merge:
   ```
   git fetch origin
   git rebase origin/main
   ```
4. After a rebase (or `rebase -i` cleanup), push with `--force-with-lease` — never plain `--force`.
5. Merge the PR with **rebase-and-merge** (or squash, when the branch is one logical change). Never create a merge commit.
6. Resolve conflicts during rebase rather than discarding work or abandoning the branch.
