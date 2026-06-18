# ================================================================
# Interloper — Multi-target Dockerfile
# ================================================================
#
# Targets (build with: docker build --target <target> .):
#
#   core       interloper-core only (lightest)
#   scheduler  core + db + scheduler + assets (cron + worker + reaper)
#   worker     core + assets only (per-asset Job target for runner.type=kubernetes)
#   api        core + db + api (assets installed; SDK extras skipped)
#   frontend   pre-built Nuxt SPA served by nginx
#
# Tagging convention:
#   docker build --target scheduler -t interloper-scheduler:0.2.0 .
#
# Build args:
#   CORE_EXTRAS       comma-separated interloper-core extras (default: google-cloud)
#                     Each extra maps to --package interloper-{name}.
#   ASSETS_EXTRAS     comma-separated interloper-assets extras (default: bing,facebook,google)
#                     Each extra maps to --extra {name} on interloper-assets.
#                     Pass "" to disable.
#   SCHEDULER_EXTRAS  comma-separated interloper-scheduler extras (default: docker)
#                     Supported: docker, k8s.  Each extra pulls in the
#                     corresponding launcher/runner package.
#
# ================================================================

ARG CORE_EXTRAS=google-cloud
ARG ASSETS_EXTRAS=bing,facebook,google
ARG SCHEDULER_EXTRAS=docker


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


# ── Python runtime base ───────────────────────────────────────
FROM python:3.12-alpine AS runtime

RUN addgroup -S app && adduser -S app -G app
ENV PATH="/interloper/.venv/bin:$PATH"


# ================================================================
# BUILD STAGES
# ================================================================

# ── core ──────────────────────────────────────────────────────
FROM base AS build-core

RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh --frozen interloper-core
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh interloper-core


# ── scheduler ─────────────────────────────────────────────────
# One image runs cron + queue worker + reaper in the same process.
# Launcher extras (docker | k8s | none) are picked at build time.
FROM base AS build-scheduler
ARG CORE_EXTRAS
ARG ASSETS_EXTRAS
ARG SCHEDULER_EXTRAS

RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh --frozen interloper-core interloper-assets interloper-db interloper-scheduler
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh interloper-core interloper-assets interloper-db interloper-scheduler


# ── worker (leaf per-asset Job target) ────────────────────────
# Used as runner.config.image when runner.type=kubernetes. Executes a single
# mini-DAG via `interloper run --format inline`. No DB, no scheduler,
# no launcher — just core + assets + destinations + pandas.
FROM base AS build-worker
ARG CORE_EXTRAS
ARG ASSETS_EXTRAS

RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh --frozen interloper-core interloper-assets
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh interloper-core interloper-assets


# ── api ───────────────────────────────────────────────────────
# The api never executes asset code — it only reads catalog metadata
# (Catalog.from_paths runs definition() on each module, which is pure
# introspection). interloper-assets is still installed so its modules
# remain importable from the catalog, but the heavy SDK extras
# (bing/google/facebook) are skipped.
FROM base AS build-api
ARG CORE_EXTRAS
ARG API_EXTRAS
ENV ASSETS_EXTRAS=""

RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh --frozen interloper-core interloper-assets interloper-db interloper-api
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    docker/uv-sync.sh interloper-core interloper-assets interloper-db interloper-api


# ── frontend (Nuxt SPA, built static) ────────────────────────
FROM node:22-alpine AS build-spa

WORKDIR /app
RUN corepack enable
COPY packages/interloper-app/app/package.json packages/interloper-app/app/pnpm-lock.yaml ./
COPY packages/interloper-app/app/patches/ patches/
RUN pnpm install --frozen-lockfile --ignore-scripts
COPY packages/interloper-app/app/ ./
RUN pnpm exec nuxt prepare && NUXT_PRESET=static pnpm build


# ================================================================
# RUNTIME STAGES
# ================================================================

# ── core ──────────────────────────────────────────────────────
FROM runtime AS core
COPY --from=build-core --chown=app:app /interloper/.venv /interloper/.venv
USER app
CMD ["interloper"]

# ── scheduler (cron + worker + reaper; singleton) ─────────────
FROM runtime AS scheduler
COPY --from=build-scheduler --chown=app:app /interloper/.venv /interloper/.venv
USER app
CMD ["interloper", "app", "--no-api", "--cron", "--worker", "--reaper", "--no-create-tables"]

# ── worker (per-asset Job target; runner.type=kubernetes only) ───────
FROM runtime AS worker
COPY --from=build-worker --chown=app:app /interloper/.venv /interloper/.venv
USER app
CMD ["interloper"]

# ── api (HTTP backend; horizontally scalable) ─────────────────
FROM runtime AS api
COPY --from=build-api --chown=app:app /interloper/.venv /interloper/.venv
USER app
EXPOSE 3000
CMD ["interloper", "app", "--api", "--no-cron", "--no-worker", "--no-reaper", "--no-create-tables"]

# ── frontend (nginx serving the pre-built SPA) ────────────────
FROM nginx:1.27-alpine-slim AS frontend
COPY --from=build-spa /app/.output/public/ /usr/share/nginx/html/
# nginx-alpine expands /etc/nginx/templates/*.template into
# /etc/nginx/conf.d/ at startup with envsubst. API_UPSTREAM is required;
# the Helm chart sets it to the in-cluster API service.
COPY docker/frontend.nginx.conf.template /etc/nginx/templates/default.conf.template
ENV API_UPSTREAM=http://localhost:3000
EXPOSE 80
