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

## Local dev instance

Stand up a running, seeded instance for trying out / verifying features. Both paths migrate the DB to head and seed the same minimal dataset via [dev/seed.py](dev/seed.py): a super-admin profile, one organisation (`Dev Org`), and the `demo` source with its `a → b,c,d → e` asset DAG. The seed is idempotent — re-running never duplicates.

The dev super-admin is set via `INTERLOPER_DEV_USER_EMAIL` (default `admin@dev.local`) and `INTERLOPER_DEV_USER_GOOGLE_ID` — read from the `make` command line, a gitignored repo-root `.env`, or the shell env (compose reads `.env` natively). Login resolves a profile by Google `google_id`, **not** email, so set `INTERLOPER_DEV_USER_GOOGLE_ID` to your Google subject id and the seed writes the exact profile your login lands on: super-admin in `Dev Org` out of the box, no duplicate. To find your id, log in once and run `make dev-seed` with only the email set — it matches your profile by email and prints the `google_id` to drop into `.env`. With neither matching profile nor id, it creates a synthetic placeholder so the instance is still usable.

- **Host** (local Postgres + CLI; fast inner loop). Needs a Postgres reachable at the [interloper.yaml](interloper.yaml) `postgres` creds (`localhost:5432`, `postgres/postgres/interloper`).
  - `make dev` — full bootstrap: reset + seed + run (one command).
  - `make dev-reset` — drop/recreate + migrate + seed (no server).
  - `make dev-up` — run api + cron + worker + reaper plus the Nuxt dev server (hot reload) at `http://localhost:3000` (the API moves to a free port Nuxt proxies to) against the existing DB — non-destructive, keeps your data/session. Installs the app deps (`pnpm install`) on first use if missing.
- **docker-compose** (Postgres + api + scheduler + frontend in containers; closest to prod). Only Docker required.
  - `make compose-up` — build + start everything; the app is on `http://localhost:3000` (nginx serves the SPA, proxies `/api`), with the API also reachable directly on `:3001`.
  - `make compose-down` — stop and drop the volume. `make compose-seed` — run only the one-shot seed.

Logging in needs Google OAuth, which is off by default ("Google OAuth not configured"). Copy [.env.example](.env.example) to a gitignored repo-root `.env` and fill in `INTERLOPER_AUTH_GOOGLE_CLIENT_ID` / `INTERLOPER_AUTH_GOOGLE_CLIENT_SECRET` (from a Google Cloud OAuth web client whose authorised redirect URI is `http://localhost:3000/api/auth/google/callback`); the Makefile forwards these (and the dev-user vars) into both the host and compose paths. `INTERLOPER_AUTH_COOKIE_SECURE` defaults to `false` so the session cookie sticks over local http.

Both run against [dev/interloper.yaml](dev/interloper.yaml), which carries **only** the catalog. The repo-root [interloper.yaml](interloper.yaml) pins postgres + launcher to prod/k8s, and pydantic-settings lets a YAML block win over env for that whole submodel — so the dev config omits those blocks, letting the `INTERLOPER_*` vars in the Makefile's `DEV_ENV` / [dev/docker-compose.yml](dev/docker-compose.yml) select the in-process launcher and the right DB. The host targets `cd dev` so that file is the active `./interloper.yaml`; compose mounts it. The compose stack lives in `dev/` too (build context is the repo root, where the dockerfile is). The dev encryption key/DB password are throwaways — never reuse them.

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
- Test files mirror the package layout one-to-one: a test for `src/interloper/<pkg>/<module>.py` lives in `tests/<pkg>/test_<module>.py`. Don't add standalone `test_<feature>.py` files — fold tests for an existing module into that module's test file (e.g. tests for `asset/base.py` go in `tests/asset/test_base.py`, not a new `test_<feature>.py`).
- Pre-commit runs ruff + pyright + pytest on every commit ([.pre-commit-config.yaml](.pre-commit-config.yaml)).
- All workspace packages share `version = "0.2.0"`, bumped by `python-semantic-release` from commit history.

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
