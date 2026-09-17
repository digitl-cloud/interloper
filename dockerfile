# ================================================================
# Interloper — Multi-target Dockerfile
# ================================================================
#
# Targets (build with: docker build --target <target> .):
#
# Every runtime role ships in two variants:
#
#   <role>          loaded: the role's packages, plus every component class a
#                   catalog can name, plus what the role runs on
#   <role>-slim     pure: core plus the role's own packages, nothing optional
#
#   core            the framework + assets (per-asset Job target for runner.type=kubernetes)
#   core-slim       the framework and nothing else
#   scheduler       core + db + scheduler + assets (cron + queue worker + reaper)
#   scheduler-slim  core + db + scheduler (in-process launcher only)
#   api             core + db + api + agent (assets installed; SDK extras skipped)
#   api-slim        core + db + api
#   mcp             core + db + toolkit + mcp (read-only MCP server, PAT auth)
#   mcp-slim        core + db + toolkit + mcp, no assets
#   frontend        pre-built Nuxt SPA served by nginx (single variant)
#
# Two rules decide what a loaded image carries. Any role that builds a catalog
# must be able to import every class a catalog can name, which is why the api
# and mcp ship sources, destinations and hooks they never execute: they
# describe them, and a class that fails to import is skipped with a warning
# rather than an error. The vendor SDKs are the exception, since their imports
# are guarded: only the roles that actually execute assets (scheduler, core)
# carry them, which is what keeps the api and mcp images a fraction of the size.
#
# The slim variants are a base to extend, not a smaller deployment: no vendor
# SDKs, no destinations, no launcher beyond in-process, no telemetry SDK, and
# no ready-made sources. A deployment layers exactly the packages it needs on
# top of one. See "Extending an image" in the docs.
#
# The static documentation site has its own standalone build: docs.dockerfile.
#
# Tagging convention: one image per role, the slim variant rides the tag.
#   docker build --target scheduler      -t interloper-scheduler:0.2.0 .
#   docker build --target scheduler-slim -t interloper-scheduler:0.2.0-slim .
#
# Build args carry what "loaded" means and are the single source of truth for
# it: the Makefile and the publish workflow pass none, so these defaults are
# what ships. The slim stages declare no extras ARG at all, so none of this
# can reach them (an ARG is only in scope where it is redeclared).
#
#   CORE_EXTRAS       comma-separated interloper-core extras
#                     (default: google-cloud,slack). Each extra maps to
#                     --package interloper-{name}.
#   ASSETS_EXTRAS     comma-separated interloper-assets extras (default: bing,facebook,google)
#                     Each extra maps to --extra {name} on interloper-assets.
#                     Pass "" to disable.
#   SCHEDULER_EXTRAS  comma-separated interloper-scheduler extras (default: docker,k8s).
#                     Each extra pulls in the corresponding launcher/runner
#                     package; the loaded image carries both, and
#                     launcher.type picks between them at runtime.
#   API_EXTRAS        comma-separated interloper-api extras (default: agent).
#                     agent bundles interloper-agent so the /agent routes
#                     mount; agent.enabled still gates them at runtime.
#   COMMON_EXTRAS     comma-separated extras defined by several workspace
#                     packages (default: otel). Each maps to --extra {name},
#                     applied to whichever selected packages carry it:
#                     otel lands the SDK/exporters via core plus the
#                     FastAPI/SQLAlchemy instrumentors via api/db where
#                     those packages are in the image. Pass "" to disable.
#
# ================================================================

ARG CORE_EXTRAS=google-cloud,slack
ARG ASSETS_EXTRAS=bing,facebook,google
ARG SCHEDULER_EXTRAS=docker,k8s
ARG API_EXTRAS=agent
ARG COMMON_EXTRAS=otel


# ── Python base: workspace manifests for dependency caching ────
FROM ghcr.io/astral-sh/uv:python3.12-alpine AS base

WORKDIR /interloper
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY docker/uv-sync.sh docker/uv-sync.sh
COPY pyproject.toml uv.lock ./
COPY packages/interloper-core/pyproject.toml        packages/interloper-core/pyproject.toml
COPY packages/interloper-assets/pyproject.toml      packages/interloper-assets/pyproject.toml
COPY packages/interloper-db/pyproject.toml          packages/interloper-db/pyproject.toml
COPY packages/interloper-scheduler/pyproject.toml   packages/interloper-scheduler/pyproject.toml
COPY packages/interloper-api/pyproject.toml         packages/interloper-api/pyproject.toml
COPY packages/interloper-app/pyproject.toml         packages/interloper-app/pyproject.toml
COPY packages/interloper-docker/pyproject.toml      packages/interloper-docker/pyproject.toml
COPY packages/interloper-k8s/pyproject.toml         packages/interloper-k8s/pyproject.toml
COPY packages/interloper-google-cloud/pyproject.toml packages/interloper-google-cloud/pyproject.toml
COPY packages/interloper-agent/pyproject.toml       packages/interloper-agent/pyproject.toml
COPY packages/interloper-pandas/pyproject.toml      packages/interloper-pandas/pyproject.toml
COPY packages/interloper-mcp/pyproject.toml         packages/interloper-mcp/pyproject.toml
COPY packages/interloper-toolkit/pyproject.toml     packages/interloper-toolkit/pyproject.toml
COPY packages/interloper-slack/pyproject.toml       packages/interloper-slack/pyproject.toml


# ── Python runtime base ───────────────────────────────────────
FROM python:3.12-alpine AS runtime

RUN addgroup -S app && adduser -S app -G app
ENV PATH="/interloper/.venv/bin:$PATH"


# ================================================================
# BUILD STAGES
# ================================================================

# ── scheduler ─────────────────────────────────────────────────
# One image runs cron + queue worker + reaper in the same process. It carries
# both launchers; launcher.type picks between them at runtime.
FROM base AS build-scheduler
ARG CORE_EXTRAS
ARG ASSETS_EXTRAS
ARG SCHEDULER_EXTRAS
ARG COMMON_EXTRAS

RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh --frozen interloper-core interloper-assets interloper-db interloper-scheduler
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh interloper-core interloper-assets interloper-db interloper-scheduler


# ── scheduler-slim ────────────────────────────────────────────
# No extras ARG is declared here, so none is in scope and the venv gets the
# named packages alone: the in-process launcher, and no sources to run.
FROM base AS build-scheduler-slim

RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh --frozen interloper-core interloper-db interloper-scheduler
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh interloper-core interloper-db interloper-scheduler


# ── core (the framework itself; leaf per-asset Job target) ────
# Used as runner.config.image when runner.type=kubernetes. Executes a single
# mini-DAG via `interloper run --format inline`. No DB, no scheduler,
# no launcher: core + assets + destinations + pandas.
FROM base AS build-core
ARG CORE_EXTRAS
ARG ASSETS_EXTRAS
ARG COMMON_EXTRAS

RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh --frozen interloper-core interloper-assets
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh interloper-core interloper-assets


# ── core-slim ─────────────────────────────────────────────────
# The framework alone, the base every other slim image builds on.
FROM base AS build-core-slim

RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh --frozen interloper-core
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh interloper-core


# ── api ───────────────────────────────────────────────────────
# The api never executes asset code — it only reads catalog metadata
# (Catalog.from_paths runs definition() on each module, which is pure
# introspection). interloper-assets is still installed so its modules
# remain importable from the catalog, but the heavy SDK extras
# (bing/google/facebook) are skipped.
FROM base AS build-api
ARG CORE_EXTRAS
ARG API_EXTRAS
ARG COMMON_EXTRAS
ENV ASSETS_EXTRAS=""

RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh --frozen interloper-core interloper-assets interloper-db interloper-api
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh interloper-core interloper-assets interloper-db interloper-api


# ── api-slim ──────────────────────────────────────────────────
FROM base AS build-api-slim

RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh --frozen interloper-core interloper-db interloper-api
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh interloper-core interloper-db interloper-api


# ── mcp ───────────────────────────────────────────────────────
# Like the api, the MCP server only reads catalog metadata — assets stay
# importable but the heavy SDK extras are skipped. google-adk is never
# pulled: the shared tool logic lives in interloper-toolkit.
FROM base AS build-mcp
ARG CORE_EXTRAS
ARG COMMON_EXTRAS
ENV ASSETS_EXTRAS=""

RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh --frozen interloper-core interloper-assets interloper-db interloper-toolkit interloper-mcp
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh interloper-core interloper-assets interloper-db interloper-toolkit interloper-mcp


# ── mcp-slim ──────────────────────────────────────────────────
FROM base AS build-mcp-slim

RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh --frozen interloper-core interloper-db interloper-toolkit interloper-mcp
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh interloper-core interloper-db interloper-toolkit interloper-mcp


# ── frontend (Nuxt SPA, built static) ────────────────────────
# Pin this stage to the *build* platform (the runner's native arch) rather
# than the target. The SPA output is static, architecture-independent assets,
# so there is no reason to run the heavy Vite/Rollup/Tailwind build under
# QEMU for the linux/arm64 target — emulating that bundler is what made the
# multi-arch frontend build hang. Only the final nginx runtime stage below
# is built per-target-arch (a trivial COPY).
FROM --platform=$BUILDPLATFORM node:24-alpine AS build-spa

WORKDIR /app
RUN corepack enable
COPY packages/interloper-app/app/package.json packages/interloper-app/app/pnpm-lock.yaml packages/interloper-app/app/pnpm-workspace.yaml ./
COPY packages/interloper-app/app/patches/ patches/
RUN pnpm install --frozen-lockfile --ignore-scripts
COPY packages/interloper-app/app/ ./
# One level up from the app dir: nuxt.config reads the workspace version from it.
COPY packages/interloper-app/pyproject.toml /pyproject.toml
RUN pnpm exec nuxt prepare && NUXT_PRESET=static pnpm build


# ================================================================
# RUNTIME STAGES
# ================================================================

# ── scheduler (cron + worker + reaper; singleton) ─────────────
FROM runtime AS scheduler
COPY --from=build-scheduler --chown=app:app /interloper/.venv /interloper/.venv
USER app
CMD ["interloper", "app", "--no-api", "--cron", "--worker", "--reaper", "--no-create-tables"]

FROM runtime AS scheduler-slim
COPY --from=build-scheduler-slim --chown=app:app /interloper/.venv /interloper/.venv
USER app
CMD ["interloper", "app", "--no-api", "--cron", "--worker", "--reaper", "--no-create-tables"]

# ── core (the framework; also the per-asset Job target) ───────
FROM runtime AS core
COPY --from=build-core --chown=app:app /interloper/.venv /interloper/.venv
USER app
CMD ["interloper"]

FROM runtime AS core-slim
COPY --from=build-core-slim --chown=app:app /interloper/.venv /interloper/.venv
USER app
CMD ["interloper"]

# ── api (HTTP backend; horizontally scalable) ─────────────────
FROM runtime AS api
COPY --from=build-api --chown=app:app /interloper/.venv /interloper/.venv
USER app
EXPOSE 3000
CMD ["interloper", "app", "--api", "--no-cron", "--no-worker", "--no-reaper", "--no-create-tables"]

FROM runtime AS api-slim
COPY --from=build-api-slim --chown=app:app /interloper/.venv /interloper/.venv
USER app
EXPOSE 3000
CMD ["interloper", "app", "--api", "--no-cron", "--no-worker", "--no-reaper", "--no-create-tables"]

# ── mcp (streamable-HTTP MCP server; horizontally scalable) ───
FROM runtime AS mcp
COPY --from=build-mcp --chown=app:app /interloper/.venv /interloper/.venv
USER app
EXPOSE 3001
CMD ["interloper-mcp"]

FROM runtime AS mcp-slim
COPY --from=build-mcp-slim --chown=app:app /interloper/.venv /interloper/.venv
USER app
EXPOSE 3001
CMD ["interloper-mcp"]

# ── frontend (nginx serving the pre-built SPA) ────────────────
FROM nginx:1.27-alpine-slim AS frontend
COPY --from=build-spa /app/.output/public/ /usr/share/nginx/html/
# nginx-alpine expands /etc/nginx/templates/*.template into
# /etc/nginx/conf.d/ at startup with envsubst. API_UPSTREAM is required;
# the Helm chart sets it to the in-cluster API service.
COPY docker/frontend.nginx.conf.template /etc/nginx/templates/default.conf.template
ENV API_UPSTREAM=http://localhost:3000
EXPOSE 80
